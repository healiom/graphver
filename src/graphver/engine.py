"""Migration engine — chain resolution, module loading, apply/rollback.

Each migration is a Python module with revision/down_revision metadata
and upgrade()/downgrade() functions, mirroring alembic's approach.
"""

from __future__ import annotations

import importlib.util
import logging
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from neo4j import Driver, Session

from graphver import helpers as _helpers

logger = logging.getLogger(__name__)

# Default directories — can be overridden via MigrationConfig
_DEFAULT_VERSIONS_DIR = Path("migrations/versions")
_DEFAULT_SNAPSHOTS_DIR = Path("migrations/snapshots")


class MigrationConfig:
    """Configuration for where to find migration files and store snapshots."""

    def __init__(
        self,
        versions_dir: Path | str = _DEFAULT_VERSIONS_DIR,
        snapshots_dir: Path | str = _DEFAULT_SNAPSHOTS_DIR,
    ):
        self.versions_dir = Path(versions_dir)
        self.snapshots_dir = Path(snapshots_dir)


class MigrationModule:
    """Loaded migration with metadata and functions."""

    def __init__(self, path: Path):
        self.path = path
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if not spec or not spec.loader:
            raise ImportError(f"Cannot load migration: {path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        self.revision: str = getattr(mod, "revision")
        self.down_revision: str | None = getattr(mod, "down_revision")
        self.source: str = getattr(mod, "source", "unknown")
        self.author: str = getattr(mod, "author", "unknown")
        self.docstring: str = mod.__doc__ or ""
        self._upgrade = mod.upgrade
        self._downgrade = mod.downgrade

    def upgrade(self, session: Session) -> None:
        self._upgrade(session, _helpers)

    def downgrade(self, session: Session) -> None:
        self._downgrade(session, _helpers)

    @property
    def title(self) -> str:
        first_line = self.docstring.strip().split("\n")[0] if self.docstring else ""
        return first_line or self.path.stem

    def __repr__(self) -> str:
        return f"Migration({self.revision}, {self.title!r})"


def load_migrations(config: MigrationConfig | None = None) -> list[MigrationModule]:
    """Load all migration modules from versions/ and return in chain order."""
    versions_dir = (config or MigrationConfig()).versions_dir
    files = sorted(versions_dir.glob("*.py"))
    files = [f for f in files if f.name != "__init__.py"]

    if not files:
        return []

    modules = {}
    for f in files:
        try:
            m = MigrationModule(f)
            modules[m.revision] = m
        except Exception as e:
            logger.error(f"Failed to load {f.name}: {e}")
            raise

    # Find root (down_revision=None)
    roots = [m for m in modules.values() if m.down_revision is None]
    if len(roots) != 1:
        raise ValueError(
            f"Expected exactly 1 root migration (down_revision=None), "
            f"found {len(roots)}: {[r.revision for r in roots]}"
        )

    # Walk chain from root
    chain: list[MigrationModule] = []
    current: MigrationModule | None = roots[0]
    visited: set[str] = set()
    while current:
        if current.revision in visited:
            raise ValueError(f"Cycle detected at revision {current.revision}")
        visited.add(current.revision)
        chain.append(current)
        next_mods = [
            m for m in modules.values() if m.down_revision == current.revision
        ]
        if len(next_mods) > 1:
            raise ValueError(
                f"Branch detected: multiple migrations point to "
                f"{current.revision}: {[m.revision for m in next_mods]}"
            )
        current = next_mods[0] if next_mods else None

    if len(chain) != len(modules):
        orphans = set(modules.keys()) - visited
        raise ValueError(f"Orphan migrations (not in chain): {orphans}")

    return chain


def get_applied(session: Session) -> dict[str, dict[str, Any]]:
    """Get all applied migrations from _GraphMigration nodes."""
    result = session.run(
        "MATCH (m:_GraphMigration) "
        "RETURN m.revision AS revision, m.applied_at AS applied_at, "
        "  m.source AS source, m.author AS author "
        "ORDER BY m.applied_at"
    )
    return {
        r["revision"]: {
            "applied_at": r["applied_at"],
            "source": r["source"],
            "author": r["author"],
        }
        for r in result
    }


def record_applied(session: Session, migration: MigrationModule) -> None:
    """Record a migration as applied."""
    session.run(
        "MERGE (m:_GraphMigration {revision: $rev}) "
        "SET m.applied_at = datetime(), "
        "  m.source = $source, "
        "  m.author = $author, "
        "  m.title = $title",
        rev=migration.revision,
        source=migration.source,
        author=migration.author,
        title=migration.title,
    )


def remove_applied(session: Session, revision: str) -> None:
    """Remove a migration record."""
    session.run(
        "MATCH (m:_GraphMigration {revision: $rev}) DETACH DELETE m",
        rev=revision,
    )


def apply_migrations(
    driver: Driver,
    target: str | None = None,
    database: str | None = None,
    config: MigrationConfig | None = None,
) -> list[str]:
    """Apply all pending migrations (or up to target). Returns applied revisions."""
    chain = load_migrations(config)
    if not chain:
        logger.info("No migration files found.")
        return []

    with driver.session(database=database) as session:
        session.run(
            "CREATE CONSTRAINT graph_migration_rev IF NOT EXISTS "
            "FOR (m:_GraphMigration) REQUIRE m.revision IS UNIQUE"
        )

        applied = get_applied(session)
        pending = [m for m in chain if m.revision not in applied]

        if target:
            target_idx = next(
                (i for i, m in enumerate(pending) if m.revision == target), None
            )
            if target_idx is None:
                if target in applied:
                    logger.info(f"Target {target} is already applied.")
                    return []
                raise ValueError(
                    f"Target revision {target!r} not found in pending migrations"
                )
            pending = pending[: target_idx + 1]

        if not pending:
            logger.info("No pending migrations.")
            return []

        applied_revs = []
        for migration in pending:
            logger.info(f"Applying {migration.revision}: {migration.title}")
            try:
                migration.upgrade(session)
                record_applied(session, migration)
                applied_revs.append(migration.revision)
                logger.info(f"  Applied {migration.revision}")
            except Exception:
                logger.error(f"  FAILED {migration.revision}")
                raise

        return applied_revs


def rollback_migrations(
    driver: Driver,
    steps: int = 1,
    target: str | None = None,
    database: str | None = None,
    config: MigrationConfig | None = None,
) -> list[str]:
    """Roll back the most recent N migrations. Returns rolled-back revisions."""
    chain = load_migrations(config)

    with driver.session(database=database) as session:
        applied = get_applied(session)

        applied_chain = [m for m in chain if m.revision in applied]
        if not applied_chain:
            logger.info("No migrations to roll back.")
            return []

        if target:
            target_idx = next(
                (i for i, m in enumerate(applied_chain) if m.revision == target),
                None,
            )
            if target_idx is None:
                raise ValueError(
                    f"Target revision {target!r} not found in applied migrations"
                )
            to_rollback = list(reversed(applied_chain[target_idx + 1 :]))
        else:
            to_rollback = list(reversed(applied_chain[-steps:]))

        if not to_rollback:
            logger.info("Nothing to roll back.")
            return []

        rolled_back = []
        for migration in to_rollback:
            logger.info(f"Rolling back {migration.revision}: {migration.title}")
            try:
                migration.downgrade(session)
                remove_applied(session, migration.revision)
                rolled_back.append(migration.revision)
                logger.info(f"  Rolled back {migration.revision}")
            except Exception:
                logger.error(f"  FAILED rollback of {migration.revision}")
                raise

        return rolled_back


def get_status(
    driver: Driver,
    database: str | None = None,
    config: MigrationConfig | None = None,
) -> dict[str, Any]:
    """Get current migration status."""
    chain = load_migrations(config)

    with driver.session(database=database) as session:
        applied = get_applied(session)

    applied_chain = [m for m in chain if m.revision in applied]
    pending_chain = [m for m in chain if m.revision not in applied]
    head = applied_chain[-1].revision if applied_chain else None

    return {
        "head": head,
        "applied_count": len(applied_chain),
        "pending_count": len(pending_chain),
        "applied": [
            {
                "revision": m.revision,
                "title": m.title,
                "applied_at": str(applied[m.revision]["applied_at"]),
                "author": applied[m.revision].get("author"),
            }
            for m in applied_chain
        ],
        "pending": [
            {"revision": m.revision, "title": m.title, "author": m.author}
            for m in pending_chain
        ],
    }


def generate_migration_file(
    slug: str,
    description: str = "",
    source: str = "manual",
    author: str = "system",
    config: MigrationConfig | None = None,
) -> Path:
    """Generate a new migration file from template."""
    cfg = config or MigrationConfig()
    chain = load_migrations(cfg)
    down_rev = chain[-1].revision if chain else None

    now = datetime.now(timezone.utc)
    date_prefix = now.strftime("%Y_%m_%d")
    existing = list(cfg.versions_dir.glob(f"{date_prefix}_*.py"))
    seq = len(existing) + 1
    revision = f"{date_prefix}_{seq:04d}"

    filename = f"{revision}_{slug}.py"
    path = cfg.versions_dir / filename

    down_rev_str = f'"{down_rev}"' if down_rev else "None"
    desc = description or f"Migration: {slug}"

    content = textwrap.dedent(f'''\
        """{desc}

        Revision ID: {revision}
        Revises: {down_rev or "None"}
        Create Date: {now.strftime("%Y-%m-%d %H:%M:%S")}

        Source: {source}
        Author: {author}
        """

        from __future__ import annotations

        from neo4j import Session

        revision: str = "{revision}"
        down_revision: str | None = {down_rev_str}
        source: str = "{source}"
        author: str = "{author}"


        def upgrade(session: Session, helpers) -> None:  # type: ignore[type-arg]
            """Apply forward migration."""
            pass


        def downgrade(session: Session, helpers) -> None:  # type: ignore[type-arg]
            """Reverse migration."""
            pass
    ''')

    cfg.versions_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    logger.info(f"Created migration: {path}")
    return path
