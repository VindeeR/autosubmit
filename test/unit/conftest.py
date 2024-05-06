import os
import pytest
from pytest import MonkeyPatch
from autosubmitconfigparser.config.basicconfig import BasicConfig
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from autosubmit.database import session
from test.unit.utils import custom_return_value

DEFAULT_DATABASE_CONN_URL = (
    "postgresql://postgres:mysecretpassword@localhost:5432/autosubmit_test"
)


@pytest.fixture
def fixture_sqlite(monkeypatch: MonkeyPatch):
    """
    Fixture that overwrites the BasicConfig to use SQLite database for testing.
    """
    monkeypatch.setattr(BasicConfig, "read", custom_return_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    yield BasicConfig


def _setup_pg_db(conn: Connection):
    """
    Resets database by dropping all schemas except the system ones and restoring the public schema
    """
    # Get all schema names that are not from the system
    results = conn.execute(
        text(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'"
        )
    ).all()
    schema_names = [res[0] for res in results]

    # Drop all schemas
    for schema_name in schema_names:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))

    # Restore default public schema
    conn.execute(text("CREATE SCHEMA public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO postgres"))


@pytest.fixture
def fixture_postgres(monkeypatch: MonkeyPatch):
    """
    Fixture to setup and teardown a Postgres database for testing.
    It will overwrite the BasicConfig to use Postgres.

    It uses the environment variable PYTEST_DATABASE_CONN_URL to connect to the database.
    If the variable is not set, it uses the default connection URL
    """

    # Apply patch BasicConfig
    monkeypatch.setattr(BasicConfig, "read", custom_return_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "postgres")
    monkeypatch.setattr(
        BasicConfig,
        "DATABASE_CONN_URL",
        os.environ.get("PYTEST_DATABASE_CONN_URL", DEFAULT_DATABASE_CONN_URL),
    )
    MockSession = scoped_session(
        sessionmaker(
            bind=create_engine(
                os.environ.get("PYTEST_DATABASE_CONN_URL", DEFAULT_DATABASE_CONN_URL)
            )
        )
    )
    monkeypatch.setattr(session, "Session", custom_return_value(MockSession))

    # Setup database
    with MockSession().bind.connect() as conn:
        _setup_pg_db(conn)
        conn.commit()

    yield BasicConfig

    # Teardown database
    with MockSession().bind.connect() as conn:
        _setup_pg_db(conn)
        conn.commit()
