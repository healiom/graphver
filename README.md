# neo4j-graph-migrations

**Alembic-style migration tool for Neo4j** — the only Python tool with rollback support.

Forward and backward migrations with snapshot-based reversibility for destructive operations. Track your graph schema and data changes with the same rigor you track relational database migrations.

## Why?

| Tool | Language | Rollback? | Python? |
|------|----------|-----------|---------|
| neo4j-migrations (Neo4j Labs) | Java | No | No |
| neo4j-python-migrations | Python | No | Yes |
| Morpheus | TypeScript | No | No |
| Liquigraph | Java | EOL | No |
| **neo4j-graph-migrations** | **Python** | **Yes** | **Yes** |

## Install

```bash
pip install neo4j-graph-migrations
```

## Quick Start

```bash
# Initialize migrations directory
mkdir -p migrations/versions

# Create your first migration
neo4j-migrate new add_user_nodes -d "Add User nodes with email constraint"

# Edit migrations/versions/2026_03_26_0001_add_user_nodes.py
# Then apply:
neo4j-migrate apply

# Check status
neo4j-migrate status

# Roll back if needed
neo4j-migrate rollback
```

## Writing Migrations

Each migration is a Python file with `upgrade()` and `downgrade()` functions:

```python
"""Add User nodes with email uniqueness constraint.

Revision ID: 0001
Revises: None
"""

revision: str = "0001"
down_revision: str | None = None
source: str = "manual"
author: str = "alice"

def upgrade(session, helpers):
    session.run(
        "CREATE CONSTRAINT user_email IF NOT EXISTS "
        "FOR (u:User) REQUIRE u.email IS UNIQUE"
    )
    helpers.bulk_create_nodes(session, "User", [
        {"email": "admin@example.com", "name": "Admin", "role": "admin"},
    ])

def downgrade(session, helpers):
    helpers.bulk_delete_nodes(session, "User", "email", ["admin@example.com"])
    session.run("DROP CONSTRAINT user_email IF EXISTS")
```

## Helpers

The `helpers` module is injected into every migration:

```python
# Create nodes (MERGE for idempotency)
helpers.bulk_create_nodes(session, "Person", [{"name": "Alice"}, {"name": "Bob"}])

# Create edges
helpers.bulk_create_edges(session, "KNOWS", [
    {"source_label": "Person", "source_name": "Alice",
     "target_label": "Person", "target_name": "Bob", "since": 2024},
])

# Delete with snapshot (for rollback)
helpers.bulk_delete_nodes(session, "Person", "name", ["Alice"],
    snapshot_path=Path("migrations/snapshots/0002.json"))

# Merge nodes (transfer edges, then delete source)
helpers.merge_nodes(session, "Person", {"name": "Bob"}, "Person", {"name": "Robert"})

# Update properties
helpers.update_properties(session, "Person", {"name": "Alice"}, {"role": "admin"})

# Arbitrary Cypher
helpers.run_cypher(session, "MATCH (n:Person) SET n.updated = datetime()")
```

## Snapshot Rollback

Destructive operations (DELETE, MERGE) can capture a snapshot before executing. The snapshot stores full node properties and all connected edges as JSON. On rollback, nodes and edges are recreated from the snapshot.

```python
def upgrade(session, helpers):
    helpers.bulk_delete_nodes(session, "OldNode", "name", ["deprecated"],
        snapshot_path=Path("migrations/snapshots/0003.json"))

def downgrade(session, helpers):
    from neo4j_graph_migrations.snapshot import restore_from_snapshot
    restore_from_snapshot(session, Path("migrations/snapshots/0003.json"))
```

## CLI Reference

```bash
neo4j-migrate status                    # Show head + pending migrations
neo4j-migrate apply [--target REV]      # Apply all pending (or up to target)
neo4j-migrate rollback [--steps N]      # Roll back N migrations (default: 1)
neo4j-migrate history                   # Full audit trail with timestamps
neo4j-migrate new SLUG [-d DESC]        # Generate new migration file
```

### Connection Options

```bash
neo4j-migrate --uri bolt://localhost:7687 --user neo4j --password secret status
neo4j-migrate --database my_graph apply
neo4j-migrate --versions-dir ./my_migrations/versions status
```

Or use environment variables:

```bash
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=secret
export NEO4J_MIGRATE_VERSIONS_DIR=./migrations/versions
export NEO4J_MIGRATE_SNAPSHOTS_DIR=./migrations/snapshots
```

## Version Tracking

Applied migrations are tracked as `_GraphMigration` nodes in Neo4j:

```cypher
MATCH (m:_GraphMigration) RETURN m ORDER BY m.applied_at
```

Each node stores: revision, applied_at, source, author, title.

## Multi-Database Support

Target a specific Neo4j database:

```bash
neo4j-migrate --database patients apply
neo4j-migrate --database knowledge_graph rollback
```

## Programmatic Usage

```python
from neo4j import GraphDatabase
from neo4j_graph_migrations.engine import (
    MigrationConfig,
    apply_migrations,
    rollback_migrations,
    get_status,
)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

config = MigrationConfig(
    versions_dir="./migrations/versions",
    snapshots_dir="./migrations/snapshots",
)

# Apply all pending
applied = apply_migrations(driver, config=config)

# Roll back last 2
rolled = rollback_migrations(driver, steps=2, config=config)

# Check status
status = get_status(driver, config=config)
print(f"Head: {status['head']}, Pending: {status['pending_count']}")
```

## License

Apache 2.0 — See [LICENSE](LICENSE).
