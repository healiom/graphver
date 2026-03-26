"""CLI runner for Neo4j graph migrations.

Usage:
    graphver status
    graphver apply [--target REV]
    graphver rollback [--steps N]
    graphver history
    graphver new SLUG [--description TEXT]
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import click
from neo4j import GraphDatabase

from graphver.engine import (
    MigrationConfig,
    apply_migrations,
    generate_migration_file,
    get_status,
    rollback_migrations,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)


def _resolve_config(versions_dir: str | None, snapshots_dir: str | None) -> MigrationConfig:
    """Build config from CLI args or env vars."""
    return MigrationConfig(
        versions_dir=Path(
            versions_dir
            or os.environ.get("NEO4J_MIGRATE_VERSIONS_DIR", "migrations/versions")
        ),
        snapshots_dir=Path(
            snapshots_dir
            or os.environ.get("NEO4J_MIGRATE_SNAPSHOTS_DIR", "migrations/snapshots")
        ),
    )


@click.group()
@click.option("--uri", default=None, help="Neo4j bolt URI (default: $NEO4J_URI or bolt://localhost:7687)")
@click.option("--user", default=None, help="Neo4j username (default: $NEO4J_USERNAME or neo4j)")
@click.option("--password", default=None, help="Neo4j password (default: $NEO4J_PASSWORD)")
@click.option("--database", default=None, help="Neo4j database name")
@click.option("--versions-dir", default=None, help="Path to migration files (default: migrations/versions)")
@click.option("--snapshots-dir", default=None, help="Path to snapshot files (default: migrations/snapshots)")
@click.pass_context
def cli(
    ctx: click.Context,
    uri: str | None,
    user: str | None,
    password: str | None,
    database: str | None,
    versions_dir: str | None,
    snapshots_dir: str | None,
) -> None:
    """graphver — Alembic-style migrations for Neo4j graphs."""
    ctx.ensure_object(dict)

    resolved_uri = uri or os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    resolved_user = user or os.environ.get("NEO4J_USERNAME", "neo4j")
    resolved_pass = password or os.environ.get("NEO4J_PASSWORD", "neo4j")

    driver = GraphDatabase.driver(resolved_uri, auth=(resolved_user, resolved_pass))
    driver.verify_connectivity()

    ctx.obj["driver"] = driver
    ctx.obj["database"] = database
    ctx.obj["config"] = _resolve_config(versions_dir, snapshots_dir)


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show current migration status."""
    info = get_status(
        ctx.obj["driver"], database=ctx.obj["database"], config=ctx.obj["config"]
    )

    click.echo(f"\nHead: {info['head'] or '(none)'}")
    click.echo(f"Applied: {info['applied_count']}  |  Pending: {info['pending_count']}")

    if info["applied"]:
        click.echo("\n  Applied migrations:")
        for m in info["applied"]:
            click.echo(f"    {click.style(m['revision'], fg='green')}  {m['title']}")

    if info["pending"]:
        click.echo("\n  Pending migrations:")
        for m in info["pending"]:
            click.echo(f"    {click.style(m['revision'], fg='yellow')}  {m['title']}")

    click.echo()


@cli.command()
@click.option("--target", default=None, help="Apply up to this revision")
@click.pass_context
def apply(ctx: click.Context, target: str | None) -> None:
    """Apply all pending migrations."""
    applied = apply_migrations(
        ctx.obj["driver"],
        target=target,
        database=ctx.obj["database"],
        config=ctx.obj["config"],
    )
    if applied:
        click.echo(
            f"\n{click.style(f'Applied {len(applied)} migration(s)', fg='green')}"
        )
        for rev in applied:
            click.echo(f"  {rev}")
    else:
        click.echo("Nothing to apply.")


@cli.command()
@click.option("--steps", default=1, help="Number of migrations to roll back")
@click.option("--target", default=None, help="Roll back to this revision (exclusive)")
@click.pass_context
def rollback(ctx: click.Context, steps: int, target: str | None) -> None:
    """Roll back the most recent migration(s)."""
    rolled = rollback_migrations(
        ctx.obj["driver"],
        steps=steps,
        target=target,
        database=ctx.obj["database"],
        config=ctx.obj["config"],
    )
    if rolled:
        click.echo(
            f"\n{click.style(f'Rolled back {len(rolled)} migration(s)', fg='red')}"
        )
        for rev in rolled:
            click.echo(f"  {rev}")
    else:
        click.echo("Nothing to roll back.")


@cli.command()
@click.pass_context
def history(ctx: click.Context) -> None:
    """Show full migration history."""
    info = get_status(
        ctx.obj["driver"], database=ctx.obj["database"], config=ctx.obj["config"]
    )

    if not info["applied"]:
        click.echo("No migrations applied yet.")
        return

    click.echo(f"\nMigration history ({info['applied_count']} applied):\n")
    for m in info["applied"]:
        click.echo(
            f"  {click.style(m['revision'], fg='green', bold=True)}  "
            f"{m['title']}\n"
            f"    applied: {m['applied_at']}  by: {m.get('author', '-')}"
        )
    click.echo()


@cli.command()
@click.argument("slug")
@click.option("--description", "-d", default="", help="Migration description")
@click.option("--source", "-s", default="manual", help="Source identifier")
@click.option("--author", "-a", default="system", help="Author")
@click.pass_context
def new(ctx: click.Context, slug: str, description: str, source: str, author: str) -> None:
    """Generate a new migration file."""
    path = generate_migration_file(
        slug=slug,
        description=description,
        source=source,
        author=author,
        config=ctx.obj["config"],
    )
    click.echo(f"Created: {path}")
    click.echo("Edit upgrade() and downgrade(), then run: graphver apply")


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
