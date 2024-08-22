import os
from pathlib import Path
from typing import Type
import pytest
from sqlalchemy import inspect

from autosubmit.history.database_managers import database_models as Models
from autosubmit.database.tables import ExperimentStatusTable
from autosubmit.history.database_managers.experiment_status_db_manager import (
    ExperimentStatusDbManager,
    SqlAlchemyExperimentStatusDbManager,
    create_experiment_status_db_manager,
)


@pytest.mark.parametrize(
    "db_engine,options,clazz",
    [
        # postgres
        pytest.param(
            "postgres",
            {"expid": "test_schema_status"},
            SqlAlchemyExperimentStatusDbManager,
            marks=[pytest.mark.postgres],
        ),
        # sqlite
        (
            "sqlite",
            {"expid": "test_schema_status", "main_db_name": "autosubmit.db"},
            ExperimentStatusDbManager,
        ),
    ],
)
def test_experiment_status_db_manager(
    tmp_path: Path,
    db_engine: str,
    options: dict,
    clazz: Type,
    request: pytest.FixtureRequest,
):
    """
    Test status database manager using the old (SQLite) and new (SQLAlchemy) implementations.
    """
    is_sqlalchemy = db_engine != "sqlite"

    # Temporary test directory
    tmp_test_dir = os.path.join(str(tmp_path), "test_status")
    os.mkdir(tmp_test_dir)

    if not is_sqlalchemy:
        options["db_dir_path"] = tmp_test_dir
        options["local_root_dir_path"] = tmp_test_dir

    # Dynamically load the fixture for that DB
    request.getfixturevalue(f"as_db_{db_engine}")

    # Assert type of database manager
    database_manager = create_experiment_status_db_manager(db_engine, **options)
    assert isinstance(database_manager, clazz)

    # Test initialization of the table (is possible is created by some previous test)
    if is_sqlalchemy:
        inspector = inspect(database_manager.engine)
        assert inspector.has_table(ExperimentStatusTable.name, schema="public")
    else:
        assert os.path.exists(database_manager._as_times_file_path)

    # Test methods
    ## Create as RUNNING
    experiment = Models.ExperimentRow(
        id=1, name=options["expid"], autosubmit_version="4.1.10", description="test"
    )
    database_manager.create_experiment_status_as_running(experiment)

    exp_status: Models.ExperimentStatusRow = (
        database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id)
    )
    exp_status.status == "RUNNING"

    ## Update status
    database_manager.update_exp_status(experiment.name, "READY")
    exp_status: Models.ExperimentStatusRow = (
        database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id)
    )
    exp_status.status == "READY"

    ## Set back to RUNNING
    database_manager.set_existing_experiment_status_as_running(exp_status.name)
    exp_status: Models.ExperimentStatusRow = (
        database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id)
    )
    exp_status.status == "RUNNING"
