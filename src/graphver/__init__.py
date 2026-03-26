"""graphver — Alembic-style versioning for Neo4j graphs.

The only Python migration tool for Neo4j with rollback support.
Forward and backward migrations with snapshot-based reversibility
for destructive operations.

Usage:
    pip install graphver
    graphver status
    graphver apply
    graphver rollback
    graphver new my_change
"""

__version__ = "0.1.0"
