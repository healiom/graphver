"""neo4j-graph-migrations — Alembic-style versioning for Neo4j graphs.

The only Python migration tool for Neo4j with rollback support.
Forward and backward migrations with snapshot-based reversibility
for destructive operations.

Usage:
    pip install neo4j-graph-migrations
    neo4j-migrate status
    neo4j-migrate apply
    neo4j-migrate rollback
    neo4j-migrate new my_change
"""

__version__ = "0.1.0"
