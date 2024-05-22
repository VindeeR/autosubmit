import os
import pytest
from pathlib import Path
from pytest import MonkeyPatch
from sqlalchemy import Connection, text
from testcontainers.postgres import PostgresContainer
from typing import Type

from autosubmit.database.session import create_engine
from autosubmitconfigparser.config.basicconfig import BasicConfig

PG_USER = "postgres"
PG_PASS = "mysecretpassword"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "autosubmit_test"

DEFAULT_DATABASE_CONN_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
"""Default Postgres connection URL."""

_identity_value = lambda value=None: lambda *ignore_args, **ignore_kwargs: value
"""A type of identity function; returns a function that returns ``value``."""


@pytest.fixture
def as_db_sqlite(monkeypatch: MonkeyPatch, tmp_path: Path) -> Type[BasicConfig]:
    """Overwrites the BasicConfig to use SQLite database for testing.

    Args:
        monkeypatch: Monkey Patcher.
    Returns:
        BasicConfig class.
    """
    monkeypatch.setattr(BasicConfig, "read", _identity_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(tmp_path / 'autosubmit.db'))

    return BasicConfig


@pytest.fixture(scope="session")
def run_test_pg_db() -> PostgresContainer:
    """Run a TestContainer for PostgreSQL.

    It is started for the test session, and stopped at the end of such.

    Returns:
        Postgres test container instance.
    """
    with PostgresContainer(
            image="postgres:16-bookworm",
            port=PG_PORT,
            username=PG_USER,
            password=PG_PASS,
            dbname=PG_DB,
            driver=None
    ).with_env(
        "POSTGRES_HOST_AUTH_METHOD", "trust"
    ).with_bind_ports(
        5432, 5432
    ) as postgres:
        yield postgres


def _setup_pg_db(conn: Connection) -> None:
    """Reset the database.

    Drops all schemas except the system ones and restoring the public schema.

    Args:
        conn: Database connection.
    """
    # Get all schema names that are not from the system
    results = conn.execute(
        text("""SELECT schema_name FROM information_schema.schemata
               WHERE schema_name NOT LIKE 'pg_%'
               AND schema_name != 'information_schema'""")
    ).all()
    schema_names = [res[0] for res in results]

    # Drop all schemas
    for schema_name in schema_names:
        conn.execute(text(f"""DROP SCHEMA IF EXISTS "{schema_name}" CASCADE"""))

    # Restore default public schema
    conn.execute(text("CREATE SCHEMA public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO postgres"))


@pytest.fixture
def as_db_postgres(monkeypatch: MonkeyPatch, run_test_pg_db) -> BasicConfig:
    """Fixture to set up and tear down a Postgres database for testing.

    It will overwrite the ``BasicConfig`` to use Postgres.

    It uses the environment variable ``PYTEST_DATABASE_CONN_URL`` to connect to the database.
    If the variable is not set, it uses the default connection URL.

    Args:
        monkeypatch: Monkey Patcher.
        run_test_pg_db: Fixture that starts the Postgres container.
    Returns:
        Autosubmit configuration for Postgres.
    """

    # Apply patch BasicConfig
    monkeypatch.setattr(BasicConfig, "read", _identity_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "postgres")
    monkeypatch.setattr(
        BasicConfig,
        "DATABASE_CONN_URL",
        os.environ.get("PYTEST_DATABASE_CONN_URL", DEFAULT_DATABASE_CONN_URL),
    )

    # Setup database
    with create_engine().connect() as conn:
        _setup_pg_db(conn)
        conn.commit()

    yield BasicConfig

    # Teardown database
    with create_engine().connect() as conn:
        _setup_pg_db(conn)
        conn.commit()
