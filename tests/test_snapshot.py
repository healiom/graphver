"""Tests for snapshot capture and restore."""

from __future__ import annotations

from pathlib import Path

from neo4j_graph_migrations import helpers
from neo4j_graph_migrations.snapshot import restore_from_snapshot, snapshot_nodes


class TestSnapshotRoundTrip:
    def test_snapshot_and_restore_nodes(self, neo4j_session, tmp_path):
        # Create nodes with edges
        helpers.bulk_create_nodes(neo4j_session, "TestAnimal", [
            {"name": "cat", "legs": 4},
            {"name": "dog", "legs": 4},
        ])
        helpers.bulk_create_nodes(neo4j_session, "TestFood", [{"name": "fish"}])
        helpers.bulk_create_edges(neo4j_session, "EATS", [
            {"source_label": "TestAnimal", "source_name": "cat",
             "target_label": "TestFood", "target_name": "fish"},
        ])

        # Snapshot cat
        snap = snapshot_nodes(neo4j_session, "TestAnimal", "name", ["cat"])
        assert len(snap) == 1
        assert snap[0]["properties"]["name"] == "cat"
        assert snap[0]["properties"]["legs"] == 4
        assert len(snap[0]["edges"]) == 1
        assert snap[0]["edges"][0]["rel_type"] == "EATS"

        # Delete cat
        helpers.bulk_delete_nodes(neo4j_session, "TestAnimal", "name", ["cat"])
        result = neo4j_session.run("MATCH (n:TestAnimal {name: 'cat'}) RETURN count(n) AS cnt").single()
        assert result["cnt"] == 0

        # Restore from snapshot
        snap_path = tmp_path / "snap.json"
        import json
        snap_path.write_text(json.dumps(snap, default=str))
        result = restore_from_snapshot(neo4j_session, snap_path)
        assert result["nodes_restored"] == 1
        assert result["edges_restored"] == 1

        # Verify restored
        r = neo4j_session.run("MATCH (n:TestAnimal {name: 'cat'}) RETURN n.legs AS legs").single()
        assert r["legs"] == 4
        r = neo4j_session.run("MATCH (:TestAnimal {name:'cat'})-[r:EATS]->(:TestFood {name:'fish'}) RETURN count(r) AS cnt").single()
        assert r["cnt"] == 1

    def test_snapshot_with_bulk_delete(self, neo4j_session, tmp_path):
        """Test the integrated snapshot_path parameter on bulk_delete_nodes."""
        helpers.bulk_create_nodes(neo4j_session, "TestWidget", [
            {"name": "w1"}, {"name": "w2"}, {"name": "w3"},
        ])

        snap_path = tmp_path / "widgets.json"
        deleted = helpers.bulk_delete_nodes(
            neo4j_session, "TestWidget", "name", ["w1", "w2"],
            snapshot_path=snap_path,
        )
        assert deleted == 2
        assert snap_path.exists()

        # Restore
        result = restore_from_snapshot(neo4j_session, snap_path)
        assert result["nodes_restored"] == 2

        # All 3 should exist now
        r = neo4j_session.run("MATCH (n:TestWidget) RETURN count(n) AS cnt").single()
        assert r["cnt"] == 3
