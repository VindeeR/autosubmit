# Fixtures available to multiple test files must be created in this file.

import pytest
from dataclasses import dataclass
from pathlib import Path
from ruamel.yaml import YAML
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import Any, Dict, Callable, List

from autosubmit.autosubmit import Autosubmit
from autosubmit.platforms.slurmplatform import SlurmPlatform, ParamikoPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

import os
from pytest import MonkeyPatch
from sqlalchemy import Connection, create_engine, text
from testcontainers.postgres import PostgresContainer
from typing import Type

@dataclass
class AutosubmitExperiment:
    """This holds information about an experiment created by Autosubmit."""
    expid: str
    autosubmit: Autosubmit
    exp_path: Path
    tmp_dir: Path
    aslogs_dir: Path
    status_dir: Path
    platform: ParamikoPlatform


@pytest.fixture(scope='function')
def autosubmit_exp(autosubmit: Autosubmit, request: pytest.FixtureRequest) -> Callable:
    """Create an instance of ``Autosubmit`` with an experiment."""

    original_root_dir = BasicConfig.LOCAL_ROOT_DIR
    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)

    def _create_autosubmit_exp(expid: str):
        # directories used when searching for logs to cat
        root_dir = tmp_path
        BasicConfig.LOCAL_ROOT_DIR = str(root_dir)
        exp_path = root_dir / expid
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        status_dir = exp_path / 'status'
        aslogs_dir.mkdir(parents=True, exist_ok=True)
        status_dir.mkdir(parents=True, exist_ok=True)

        platform_config = {
            "LOCAL_ROOT_DIR": BasicConfig.LOCAL_ROOT_DIR,
            "LOCAL_TMP_DIR": str(exp_tmp_dir),
            "LOCAL_ASLOG_DIR": str(aslogs_dir)
        }
        platform = SlurmPlatform(expid=expid, name='slurm_platform', config=platform_config)
        platform.job_status = {
            'COMPLETED': [],
            'RUNNING': [],
            'QUEUING': [],
            'FAILED': []
        }
        submit_platform_script = aslogs_dir / 'submit_local.sh'
        submit_platform_script.touch(exist_ok=True)

        return AutosubmitExperiment(
            expid=expid,
            autosubmit=autosubmit,
            exp_path=exp_path,
            tmp_dir=exp_tmp_dir,
            aslogs_dir=aslogs_dir,
            status_dir=status_dir,
            platform=platform
        )

    def finalizer():
        BasicConfig.LOCAL_ROOT_DIR = original_root_dir
        if tmp_path and tmp_path.exists():
            rmtree(tmp_path)

    request.addfinalizer(finalizer)

    return _create_autosubmit_exp


@pytest.fixture(scope='module')
def autosubmit() -> Autosubmit:
    """Create an instance of ``Autosubmit``.

    Useful when you need ``Autosubmit`` but do not need any experiments."""
    autosubmit = Autosubmit()
    return autosubmit


@pytest.fixture(scope='function')
def create_as_conf() -> Callable:
    def _create_as_conf(autosubmit_exp: AutosubmitExperiment, yaml_files: List[Path], experiment_data: Dict[str, Any]):
        conf_dir = autosubmit_exp.exp_path / 'conf'
        conf_dir.mkdir(parents=False, exist_ok=False)
        basic_config = BasicConfig
        parser_factory = YAMLParserFactory()
        as_conf = AutosubmitConfig(
            expid=autosubmit_exp.expid,
            basic_config=basic_config,
            parser_factory=parser_factory
        )
        for yaml_file in yaml_files:
            with open(conf_dir / yaml_file.name, 'w+') as f:
                f.write(yaml_file.read_text())
                f.flush()
        # add user-provided experiment data
        with open(conf_dir / 'conftest_as.yml', 'w+') as f:
            yaml = YAML()
            yaml.indent(sequence=4, offset=2)
            yaml.dump(experiment_data, f)
            f.flush()
        return as_conf

    return _create_as_conf


DEFAULT_DATABASE_CONN_URL = (
    "postgresql://postgres:mysecretpassword@localhost:5432/autosubmit_test"
)

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
