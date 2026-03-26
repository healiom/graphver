"""Tests for the migration engine — chain resolution, apply, rollback."""

from __future__ import annotations

import textwrap

import pytest

from graphver.engine import (
    MigrationConfig,
    apply_migrations,
    get_status,
    load_migrations,
    rollback_migrations,
)


def _migration_source(revision: str, down_revision: str | None, body_up: str = "pass", body_down: str = "pass") -> str:
    down = f'"{down_revision}"' if down_revision else "None"
    return textwrap.dedent(f'''\
        """Test migration {revision}."""
        revision = "{revision}"
        down_revision = {down}

        def upgrade(session, helpers):
            {body_up}

        def downgrade(session, helpers):
            {body_down}
    ''')


class TestChainResolution:
    def test_empty_dir(self, migration_dirs):
        versions_dir, _ = migration_dirs
        config = MigrationConfig(versions_dir=versions_dir)
        assert load_migrations(config) == []

    def test_single_root(self, write_migration, migration_dirs):
        versions_dir, _ = migration_dirs
        write_migration("0001_init.py", _migration_source("0001", None))
        config = MigrationConfig(versions_dir=versions_dir)
        chain = load_migrations(config)
        assert len(chain) == 1
        assert chain[0].revision == "0001"

    def test_linear_chain(self, write_migration, migration_dirs):
        versions_dir, _ = migration_dirs
        write_migration("0001_init.py", _migration_source("0001", None))
        write_migration("0002_add.py", _migration_source("0002", "0001"))
        write_migration("0003_more.py", _migration_source("0003", "0002"))
        config = MigrationConfig(versions_dir=versions_dir)
        chain = load_migrations(config)
        assert [m.revision for m in chain] == ["0001", "0002", "0003"]

    def test_multiple_roots_raises(self, write_migration, migration_dirs):
        versions_dir, _ = migration_dirs
        write_migration("0001_a.py", _migration_source("0001", None))
        write_migration("0002_b.py", _migration_source("0002", None))
        config = MigrationConfig(versions_dir=versions_dir)
        with pytest.raises(ValueError, match="Expected exactly 1 root"):
            load_migrations(config)

    def test_branch_raises(self, write_migration, migration_dirs):
        versions_dir, _ = migration_dirs
        write_migration("0001_root.py", _migration_source("0001", None))
        write_migration("0002_a.py", _migration_source("0002a", "0001"))
        write_migration("0002_b.py", _migration_source("0002b", "0001"))
        config = MigrationConfig(versions_dir=versions_dir)
        with pytest.raises(ValueError, match="Branch detected"):
            load_migrations(config)


class TestApplyRollback:
    def test_apply_creates_nodes(self, neo4j_driver, write_migration, migration_dirs):
        versions_dir, snapshots_dir = migration_dirs
        write_migration(
            "0001_init.py",
            _migration_source(
                "0001",
                None,
                body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "alice"}, {"name": "bob"}])',
                body_down='helpers.run_cypher(session, "MATCH (n:TestNode) DETACH DELETE n")',
            ),
        )
        config = MigrationConfig(versions_dir=versions_dir, snapshots_dir=snapshots_dir)

        # Apply
        applied = apply_migrations(neo4j_driver, config=config)
        assert applied == ["0001"]

        # Verify nodes exist
        with neo4j_driver.session() as s:
            result = s.run("MATCH (n:TestNode) RETURN count(n) AS cnt").single()
            assert result["cnt"] == 2

        # Verify status
        status = get_status(neo4j_driver, config=config)
        assert status["head"] == "0001"
        assert status["pending_count"] == 0

    def test_rollback_removes_nodes(self, neo4j_driver, write_migration, migration_dirs):
        versions_dir, snapshots_dir = migration_dirs
        write_migration(
            "0001_init.py",
            _migration_source(
                "0001",
                None,
                body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "carol"}])',
                body_down='helpers.run_cypher(session, "MATCH (n:TestNode) DETACH DELETE n")',
            ),
        )
        config = MigrationConfig(versions_dir=versions_dir, snapshots_dir=snapshots_dir)

        apply_migrations(neo4j_driver, config=config)

        # Rollback
        rolled = rollback_migrations(neo4j_driver, config=config)
        assert rolled == ["0001"]

        # Verify nodes gone
        with neo4j_driver.session() as s:
            result = s.run("MATCH (n:TestNode) RETURN count(n) AS cnt").single()
            assert result["cnt"] == 0

        # Verify status
        status = get_status(neo4j_driver, config=config)
        assert status["head"] is None

    def test_idempotent_apply(self, neo4j_driver, write_migration, migration_dirs):
        versions_dir, snapshots_dir = migration_dirs
        write_migration(
            "0001_init.py",
            _migration_source(
                "0001",
                None,
                body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "dave"}])',
                body_down='helpers.run_cypher(session, "MATCH (n:TestNode) DETACH DELETE n")',
            ),
        )
        config = MigrationConfig(versions_dir=versions_dir, snapshots_dir=snapshots_dir)

        # Apply twice
        apply_migrations(neo4j_driver, config=config)
        applied = apply_migrations(neo4j_driver, config=config)
        assert applied == []  # nothing to apply

        # Still 1 node (MERGE is idempotent)
        with neo4j_driver.session() as s:
            result = s.run("MATCH (n:TestNode) RETURN count(n) AS cnt").single()
            assert result["cnt"] == 1

    def test_apply_rollback_apply_cycle(self, neo4j_driver, write_migration, migration_dirs):
        versions_dir, snapshots_dir = migration_dirs
        write_migration(
            "0001_init.py",
            _migration_source(
                "0001",
                None,
                body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "eve"}])',
                body_down='helpers.run_cypher(session, "MATCH (n:TestNode) DETACH DELETE n")',
            ),
        )
        config = MigrationConfig(versions_dir=versions_dir, snapshots_dir=snapshots_dir)

        # Cycle: apply → rollback → apply
        apply_migrations(neo4j_driver, config=config)
        rollback_migrations(neo4j_driver, config=config)
        applied = apply_migrations(neo4j_driver, config=config)

        assert applied == ["0001"]
        with neo4j_driver.session() as s:
            result = s.run("MATCH (n:TestNode) RETURN count(n) AS cnt").single()
            assert result["cnt"] == 1

    def test_target_apply(self, neo4j_driver, write_migration, migration_dirs):
        versions_dir, snapshots_dir = migration_dirs
        write_migration("0001_a.py", _migration_source("0001", None,
            body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "a"}])'))
        write_migration("0002_b.py", _migration_source("0002", "0001",
            body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "b"}])'))
        write_migration("0003_c.py", _migration_source("0003", "0002",
            body_up='helpers.bulk_create_nodes(session, "TestNode", [{"name": "c"}])'))
        config = MigrationConfig(versions_dir=versions_dir, snapshots_dir=snapshots_dir)

        # Apply only up to 0002
        applied = apply_migrations(neo4j_driver, target="0002", config=config)
        assert applied == ["0001", "0002"]

        status = get_status(neo4j_driver, config=config)
        assert status["head"] == "0002"
        assert status["pending_count"] == 1
