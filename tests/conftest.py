"""Test fixtures for neo4j-graph-migrations."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
from neo4j import GraphDatabase, Session


@pytest.fixture(scope="session")
def neo4j_driver():
    """Connect to a Neo4j instance for testing.

    Set NEO4J_TEST_URI env var to point to your test instance.
    Default: bolt://localhost:7687 with neo4j/test credentials.

    For CI, use testcontainers:
        docker run -d -p 7687:7687 -e NEO4J_AUTH=neo4j/test neo4j:5-community
    """
    import os

    uri = os.environ.get("NEO4J_TEST_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_TEST_USER", "neo4j")
    password = os.environ.get("NEO4J_TEST_PASSWORD", "test")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    yield driver
    driver.close()


@pytest.fixture
def neo4j_session(neo4j_driver) -> Session:
    """Provide a session that cleans up all test data after each test."""
    with neo4j_driver.session() as session:
        yield session
        # Clean up everything
        session.run("MATCH (n) DETACH DELETE n")


@pytest.fixture
def migration_dirs(tmp_path):
    """Create temporary versions/ and snapshots/ directories."""
    versions_dir = tmp_path / "versions"
    versions_dir.mkdir()
    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    return versions_dir, snapshots_dir


@pytest.fixture
def write_migration(migration_dirs):
    """Helper to write a migration file to the temp versions dir."""
    versions_dir, _ = migration_dirs

    def _write(filename: str, content: str) -> Path:
        path = versions_dir / filename
        path.write_text(content)
        return path

    return _write
