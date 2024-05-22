from unittest import TestCase

import os
import sys
from mock import MagicMock
from mock import patch
from autosubmit.database.db_manager import DbManager, DatabaseManager, SqlAlchemyDbManager, create_db_manager

import pytest
import random
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from sqlite3 import OperationalError
from typing import Optional, Type, Union, cast

from autosubmit.database.tables import DBVersionTable


class TestDbManager(TestCase):
    def test_create_table_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        table_fields = ['dummy1', 'dummy2', 'dummy3']
        expected_command = 'CREATE TABLE IF NOT EXISTS tests (dummy1, dummy2, dummy3)'
        # act
        command = DbManager.generate_create_table_command(table_name, table_fields)
        # assert
        self.assertEqual(expected_command, command)

    def test_insert_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        columns = ['col1, col2, col3']
        values = ['dummy1', 'dummy2', 'dummy3']
        expected_command = 'INSERT INTO tests(col1, col2, col3) VALUES ("dummy1", "dummy2", "dummy3")'
        # act
        command = DbManager.generate_insert_command(table_name, columns, values)
        # assert
        self.assertEqual(expected_command, command)

    def test_insert_many_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        num_of_values = 3
        expected_command = 'INSERT INTO tests VALUES (?,?,?)'
        # act
        command = DbManager.generate_insert_many_command(table_name, num_of_values)
        # assert
        self.assertEqual(expected_command, command)

    def test_select_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        where = ['test=True', 'debug=True']
        expected_command = 'SELECT * FROM tests WHERE test=True AND debug=True'
        # act
        command = DbManager.generate_select_command(table_name, where)
        # assert
        self.assertEqual(expected_command, command)

    def test_when_database_already_exists_then_is_not_initialized_again(self):
        sys.modules['os'].path.exists = MagicMock(return_value=True)
        connection_mock = MagicMock()
        cursor_mock = MagicMock()
        cursor_mock.side_effect = Exception('This method should not be called')
        connection_mock.cursor = MagicMock(return_value=cursor_mock)
        original_connect = sys.modules['sqlite3'].connect
        sys.modules['sqlite3'].connect = MagicMock(return_value=connection_mock)
        DbManager('dummy-path', 'dummy-name', 999)
        connection_mock.cursor.assert_not_called()
        sys.modules['sqlite3'].connect = original_connect


@pytest.mark.parametrize(
    'db_engine,options,clazz,expected_exception',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, SqlAlchemyDbManager, None, marks=[pytest.mark.postgres]),
        # sqlite
        ('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, DbManager, None),
        # invalid engine
        ('super-duper-database', {}, None, ValueError)
    ])
def test_db_manager(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        clazz: Type,
        expected_exception: BaseException,
        request
):
    """Regression tests for ``DbManager`` and ``SqlAlchemy``.

    Tests for regression issues in ``DbManager``, and for compatibility issues
    with the new ``SqlAlchemyDbManager``.

    The parameters allow the test to be run with different engine+options,
    accepting also the expected database type (``clazz``). You can also
    mark certain tests belonging to a group (e.g. postgres) so that they
    are skipped/executed selectively in CICD environments.
    """

    is_sqlalchemy = db_engine != 'sqlite'
    if not is_sqlalchemy:
        # N.B.: We do it here, as we don't know the temporary path name until the fixture exists,
        #       and because it's harmless to the Postgres test to have the tmp_path fixture.
        options['root_path'] = str(tmp_path)

    # In this test we will create a random table, to show that it works with any table,
    # not only the ones we have in the ``database.tables`` package.
    table_name = DBVersionTable.name

    # The database manager under test will be either the old type ``DbManager``, or
    # of the new type ``SqlAlchemyManager``.
    db_manager: Optional[Union[DbManager, SqlAlchemyDbManager]] = None

    try:
        # Is this parametrized test expected to fail?
        if expected_exception is not None:
            with pytest.raises(expected_exception):  # type: ignore  # TODO: [testing] how to type this?
                create_db_manager(db_engine, **options)
            return

        # If not, then now we dynamically load the fixture for that DB,
        # ref: https://stackoverflow.com/a/64348247.
        request.getfixturevalue(f'as_db_{db_engine}')

        database_manager: DatabaseManager = create_db_manager(db_engine, **options)

        # The database manager created has the right type?
        assert isinstance(database_manager, clazz)
        db_manager: clazz = cast(clazz, database_manager)

        # The database manager was constructed right?
        if is_sqlalchemy:
            assert db_engine in db_manager.engine.dialect.name
            assert db_manager.schema == options['schema']
        else:
            assert db_manager.root_path == options['root_path']
            assert db_manager.db_name == options['db_name']
            assert db_manager.db_version == options['db_version']

        # In both cases, we must have a connection set up now
        assert db_manager.connection is not None

        # NOTE: From this part forward, the behaviour MUST be the same for
        #       SQLite and Postgres or any other engine. This is the test
        #       that verifies that whatever we do with SQLite, works the
        #       same with another engine.

        # Create table
        # TODO: get the fields dynamically?
        db_manager.create_table(table_name, ['version'])

        # Table should be empty
        records = db_manager.select_all(table_name)
        assert isinstance(records, list) and len(records) == 0

        # Insert N items
        rand_len = random.randint(10, 50)
        # db_manager.insertMany(table_name, [{"version": i} for i in range(rand_len)])
        db_manager.insertMany(table_name, [[i] for i in range(rand_len)])
        records = db_manager.select_all(table_name)
        assert isinstance(records, list) and len(records) == rand_len

        # Delete table items (only available in the new SQLAlchemy DB Manager)
        if is_sqlalchemy:
            db_manager.delete_all(table_name)
            records = db_manager.select_all(table_name)
            assert isinstance(records, list) and len(records) == 0

        # Drop table
        db_manager.drop_table(table_name)
        with pytest.raises((SQLAlchemyError, OperationalError)):  # type: ignore # different errors, but expected...
            # Table not exist
            _ = db_manager.select_all(table_name)
    finally:
        if db_manager is not None:
            db_manager.disconnect()
