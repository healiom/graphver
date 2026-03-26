"""Cypher helper functions for graph migrations.

All mutating helpers use MERGE (not CREATE) for idempotency.
Optional snapshot_path captures pre-mutation state for rollback.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from neo4j import Session

logger = logging.getLogger(__name__)

BATCH_SIZE = 1_000


def bulk_create_nodes(
    session: Session,
    label: str,
    data: list[dict[str, Any]],
    merge_on: str = "name",
    batch_size: int = BATCH_SIZE,
) -> int:
    """Create or update nodes using UNWIND + MERGE. Returns count."""
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        result = session.run(
            f"UNWIND $batch AS row "
            f"MERGE (n:{label} {{{merge_on}: row.{merge_on}}}) "
            f"SET n += row "
            f"RETURN count(n) AS cnt",
            batch=batch,
        )
        total += result.single()["cnt"]
    logger.info(f"bulk_create_nodes({label}): {total} nodes")
    return total


def bulk_create_edges(
    session: Session,
    rel_type: str,
    data: list[dict[str, Any]],
    match_on: str = "name",
    batch_size: int = BATCH_SIZE,
) -> int:
    """Create edges using MATCH + MERGE.

    Each item in data must have:
      source_label, source_{match_on}, target_label, target_{match_on}
    Additional keys become edge properties.
    """
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        reserved = {
            "source_label",
            "target_label",
            f"source_{match_on}",
            f"target_{match_on}",
        }
        prop_keys = [k for k in batch[0] if k not in reserved] if batch else []
        set_clause = ""
        if prop_keys:
            assignments = ", ".join(f"r.{k} = row.{k}" for k in prop_keys)
            set_clause = f"SET {assignments} "

        source_labels = {d["source_label"] for d in batch}
        target_labels = {d["target_label"] for d in batch}

        for src_label in source_labels:
            for tgt_label in target_labels:
                sub_batch = [
                    d
                    for d in batch
                    if d["source_label"] == src_label
                    and d["target_label"] == tgt_label
                ]
                if not sub_batch:
                    continue
                result = session.run(
                    f"UNWIND $batch AS row "
                    f"MATCH (a:{src_label} {{{match_on}: row.source_{match_on}}}) "
                    f"MATCH (b:{tgt_label} {{{match_on}: row.target_{match_on}}}) "
                    f"MERGE (a)-[r:{rel_type}]->(b) "
                    f"{set_clause}"
                    f"RETURN count(r) AS cnt",
                    batch=sub_batch,
                )
                total += result.single()["cnt"]
    logger.info(f"bulk_create_edges({rel_type}): {total} edges")
    return total


def bulk_delete_nodes(
    session: Session,
    label: str,
    match_on: str,
    values: list[str],
    snapshot_path: Path | None = None,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Delete nodes by matching property values. DETACH DELETE removes edges too.

    If snapshot_path provided, captures full state before deleting.
    """
    if snapshot_path:
        from graphver.snapshot import snapshot_nodes

        snap = snapshot_nodes(session, label, match_on, values)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(json.dumps(snap, indent=2, default=str))
        logger.info(f"Snapshot saved: {snapshot_path} ({len(snap)} nodes)")

    total = 0
    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        result = session.run(
            f"UNWIND $batch AS val "
            f"MATCH (n:{label} {{{match_on}: val}}) "
            f"DETACH DELETE n "
            f"RETURN count(n) AS cnt",
            batch=batch,
        )
        total += result.single()["cnt"]
    logger.info(f"bulk_delete_nodes({label}): {total} nodes")
    return total


def bulk_delete_edges(
    session: Session,
    rel_type: str,
    data: list[dict[str, Any]],
    match_on: str = "name",
    batch_size: int = BATCH_SIZE,
) -> int:
    """Delete specific edges by matching source and target nodes."""
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        source_labels = {d["source_label"] for d in batch}
        target_labels = {d["target_label"] for d in batch}

        for src_label in source_labels:
            for tgt_label in target_labels:
                sub_batch = [
                    d
                    for d in batch
                    if d["source_label"] == src_label
                    and d["target_label"] == tgt_label
                ]
                if not sub_batch:
                    continue
                result = session.run(
                    f"UNWIND $batch AS row "
                    f"MATCH (a:{src_label} {{{match_on}: row.source_{match_on}}})"
                    f"-[r:{rel_type}]->"
                    f"(b:{tgt_label} {{{match_on}: row.target_{match_on}}}) "
                    f"DELETE r "
                    f"RETURN count(r) AS cnt",
                    batch=sub_batch,
                )
                total += result.single()["cnt"]
    logger.info(f"bulk_delete_edges({rel_type}): {total} edges")
    return total


def merge_nodes(
    session: Session,
    source_label: str,
    source_match: dict[str, str],
    target_label: str,
    target_match: dict[str, str],
    snapshot_path: Path | None = None,
) -> dict[str, int]:
    """Merge source node into target: transfer all edges, then delete source."""
    src_where = " AND ".join(f"src.{k} = ${k}_src" for k in source_match)
    tgt_where = " AND ".join(f"tgt.{k} = ${k}_tgt" for k in target_match)
    params: dict[str, Any] = {}
    for k, v in source_match.items():
        params[f"{k}_src"] = v
    for k, v in target_match.items():
        params[f"{k}_tgt"] = v

    if snapshot_path:
        from graphver.snapshot import snapshot_nodes

        match_key = list(source_match.keys())[0]
        snap = snapshot_nodes(
            session, source_label, match_key, [list(source_match.values())[0]]
        )
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(json.dumps(snap, indent=2, default=str))

    # Transfer outgoing edges
    session.run(
        f"MATCH (src:{source_label}) WHERE {src_where} "
        f"MATCH (tgt:{target_label}) WHERE {tgt_where} "
        f"MATCH (src)-[r]->(other) "
        f"WITH tgt, other, type(r) AS rtype, properties(r) AS rprops "
        f"CALL apoc.create.relationship(tgt, rtype, rprops, other) YIELD rel "
        f"RETURN count(rel)",
        **params,
    )

    # Transfer incoming edges
    session.run(
        f"MATCH (src:{source_label}) WHERE {src_where} "
        f"MATCH (tgt:{target_label}) WHERE {tgt_where} "
        f"MATCH (other)-[r]->(src) "
        f"WITH tgt, other, type(r) AS rtype, properties(r) AS rprops "
        f"CALL apoc.create.relationship(other, rtype, rprops, tgt) YIELD rel "
        f"RETURN count(rel)",
        **params,
    )

    result = session.run(
        f"MATCH (src:{source_label}) WHERE {src_where} "
        f"DETACH DELETE src "
        f"RETURN count(src) AS cnt",
        **params,
    )
    deleted = result.single()["cnt"]
    logger.info(
        f"merge_nodes: {source_label}({source_match}) -> "
        f"{target_label}({target_match}), deleted={deleted}"
    )
    return {"deleted": deleted}


def update_properties(
    session: Session,
    label: str,
    match_on: dict[str, str],
    set_props: dict[str, Any],
    snapshot_path: Path | None = None,
) -> int:
    """Update properties on matched nodes."""
    where = " AND ".join(f"n.{k} = ${k}" for k in match_on)
    params = {**match_on}

    if snapshot_path:
        prop_list = list(set_props.keys())
        result = session.run(
            f"MATCH (n:{label}) WHERE {where} "
            f"RETURN properties(n) AS props",
            **params,
        )
        old_values = [
            {k: r["props"].get(k) for k in prop_list} for r in result
        ]
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(
            json.dumps(
                {"match": match_on, "old_values": old_values}, indent=2, default=str
            )
        )

    set_clause = ", ".join(f"n.{k} = $set_{k}" for k in set_props)
    for k, v in set_props.items():
        params[f"set_{k}"] = v

    result = session.run(
        f"MATCH (n:{label}) WHERE {where} "
        f"SET {set_clause} "
        f"RETURN count(n) AS cnt",
        **params,
    )
    cnt = result.single()["cnt"]
    logger.info(f"update_properties({label}): {cnt} nodes updated")
    return cnt


def run_cypher(
    session: Session, cypher: str, **params: Any
) -> list[dict[str, Any]]:
    """Execute arbitrary Cypher. Returns list of record dicts."""
    result = session.run(cypher, **params)
    return [dict(r) for r in result]
