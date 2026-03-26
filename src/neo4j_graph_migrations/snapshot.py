"""Snapshot capture and restore for reversible graph mutations.

Before destructive operations (DELETE, MERGE), captures the full state
of affected nodes and their edges so downgrade() can restore them.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from neo4j import Session

logger = logging.getLogger(__name__)


def snapshot_nodes(
    session: Session,
    label: str,
    match_on: str,
    values: list[str],
) -> list[dict[str, Any]]:
    """Capture full state of nodes + all their edges for rollback."""
    snapshots = []
    for val in values:
        node_result = session.run(
            f"MATCH (n:{label} {{{match_on}: $val}}) "
            f"RETURN properties(n) AS props, labels(n) AS lbls",
            val=val,
        )
        node_record = node_result.single()
        if not node_record:
            continue

        edge_result = session.run(
            f"MATCH (n:{label} {{{match_on}: $val}})-[r]-(other) "
            f"RETURN type(r) AS rel_type, "
            f"  properties(r) AS rel_props, "
            f"  labels(other)[0] AS other_label, "
            f"  other.name AS other_name, "
            f"  CASE WHEN startNode(r) = n THEN 'out' ELSE 'in' END AS direction",
            val=val,
        )
        edges = [dict(r) for r in edge_result]

        snapshots.append(
            {
                "label": node_record["lbls"][0] if node_record["lbls"] else label,
                "properties": dict(node_record["props"]),
                "edges": edges,
            }
        )

    return snapshots


def restore_from_snapshot(
    session: Session,
    snapshot_path: Path,
) -> dict[str, int]:
    """Restore nodes and edges from a snapshot file."""
    data = json.loads(snapshot_path.read_text())
    nodes_restored = 0
    edges_restored = 0

    for snap in data:
        node_label = snap["label"]
        props = snap["properties"]

        merge_key = "name" if "name" in props else next(iter(props), None)
        if not merge_key:
            logger.warning(f"Snapshot node has no properties, skipping")
            continue
        merge_val = props[merge_key]

        session.run(
            f"MERGE (n:{node_label} {{{merge_key}: $merge_val}}) "
            f"SET n = $props",
            merge_val=merge_val,
            props=props,
        )
        nodes_restored += 1

        for edge in snap.get("edges", []):
            other_label = edge["other_label"]
            other_name = edge.get("other_name")
            rel_type = edge["rel_type"]
            rel_props = edge.get("rel_props", {})
            direction = edge["direction"]

            if not other_name:
                logger.warning(f"Cannot match edge target: {edge}")
                continue

            other_match = f"(other:{other_label} {{name: $other_id}})"

            if direction == "out":
                cypher = (
                    f"MATCH (n:{node_label} {{{merge_key}: $merge_val}}) "
                    f"MATCH {other_match} "
                    f"MERGE (n)-[r:{rel_type}]->(other) "
                    f"SET r += $rel_props "
                    f"RETURN count(r) AS cnt"
                )
            else:
                cypher = (
                    f"MATCH (n:{node_label} {{{merge_key}: $merge_val}}) "
                    f"MATCH {other_match} "
                    f"MERGE (other)-[r:{rel_type}]->(n) "
                    f"SET r += $rel_props "
                    f"RETURN count(r) AS cnt"
                )

            result = session.run(
                cypher,
                merge_val=merge_val,
                other_id=other_name,
                rel_props=rel_props,
            )
            edges_restored += result.single()["cnt"]

    logger.info(
        f"Restored from {snapshot_path.name}: "
        f"{nodes_restored} nodes, {edges_restored} edges"
    )
    return {"nodes_restored": nodes_restored, "edges_restored": edges_restored}
