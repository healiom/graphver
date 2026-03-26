"""Tests for Cypher helper functions."""

from __future__ import annotations

from graphver import helpers


class TestBulkCreateNodes:
    def test_creates_nodes(self, neo4j_session):
        count = helpers.bulk_create_nodes(
            neo4j_session,
            "TestPerson",
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
        )
        assert count == 2
        result = neo4j_session.run("MATCH (n:TestPerson) RETURN n.name AS name ORDER BY name")
        names = [r["name"] for r in result]
        assert names == ["Alice", "Bob"]

    def test_merge_is_idempotent(self, neo4j_session):
        data = [{"name": "Carol"}]
        helpers.bulk_create_nodes(neo4j_session, "TestPerson", data)
        helpers.bulk_create_nodes(neo4j_session, "TestPerson", data)
        result = neo4j_session.run("MATCH (n:TestPerson {name: 'Carol'}) RETURN count(n) AS cnt").single()
        assert result["cnt"] == 1


class TestBulkCreateEdges:
    def test_creates_edges(self, neo4j_session):
        helpers.bulk_create_nodes(neo4j_session, "TestCity", [{"name": "NYC"}, {"name": "LA"}])
        count = helpers.bulk_create_edges(
            neo4j_session,
            "CONNECTS",
            [{"source_label": "TestCity", "source_name": "NYC",
              "target_label": "TestCity", "target_name": "LA", "distance": 2800}],
        )
        assert count == 1
        result = neo4j_session.run(
            "MATCH (:TestCity {name:'NYC'})-[r:CONNECTS]->(:TestCity {name:'LA'}) "
            "RETURN r.distance AS d"
        ).single()
        assert result["d"] == 2800


class TestBulkDeleteNodes:
    def test_deletes_with_edges(self, neo4j_session):
        helpers.bulk_create_nodes(neo4j_session, "TestItem", [{"name": "X"}, {"name": "Y"}])
        helpers.bulk_create_edges(neo4j_session, "LINKS", [
            {"source_label": "TestItem", "source_name": "X",
             "target_label": "TestItem", "target_name": "Y"},
        ])
        deleted = helpers.bulk_delete_nodes(neo4j_session, "TestItem", "name", ["X"])
        assert deleted == 1
        # Edge should be gone too (DETACH DELETE)
        result = neo4j_session.run("MATCH ()-[r:LINKS]->() RETURN count(r) AS cnt").single()
        assert result["cnt"] == 0


class TestRunCypher:
    def test_returns_records(self, neo4j_session):
        helpers.bulk_create_nodes(neo4j_session, "TestThing", [{"name": "Z"}])
        results = helpers.run_cypher(neo4j_session, "MATCH (n:TestThing) RETURN n.name AS name")
        assert results == [{"name": "Z"}]
