import os
import random
import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig
from sqlalchemy import Column, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy
from autosubmit.database import tables
from autosubmit.database.db_manager import (
    create_db_table_manager,
    SQLiteDbTableManager,
    PostgresDbTableManager,
)
import tempfile


class TestDbManager:
    def test_sqlite(self, fixture_sqlite: BasicConfig):
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Creates a new SQLite db file
            db_filepath = os.path.join(tmpdirname, "tmp.db")
            db_manager = create_db_table_manager(tables.DBVersionTable, db_filepath)
            assert isinstance(db_manager, SQLiteDbTableManager)

            # Test __init__
            assert db_manager.engine.dialect.name == "sqlite"
            assert isinstance(db_manager.table, Table)
            assert db_manager.table.name == tables.DBVersionTable.__tablename__

            with db_manager.get_connection() as conn:
                # Create table
                db_manager.create_table(conn)

                # Table should be empty
                records = db_manager.select_all(conn)
                assert isinstance(records, list) and len(records) == 0

                # Insert N items
                rand_len = random.randint(10, 50)
                db_manager.insert_many(conn, [{"version": i} for i in range(rand_len)])
                records = db_manager.select_all(conn)
                assert isinstance(records, list) and len(records) == rand_len

                # Delete table items
                db_manager.delete_all(conn)
                records = db_manager.select_all(conn)
                assert isinstance(records, list) and len(records) == 0

                # Drop table
                db_manager.drop_table(conn)
                with pytest.raises(SQLAlchemyError):
                    # Table not exist
                    records = db_manager.select_all(conn)

    def test_postgres(self, fixture_postgres: BasicConfig):
        table = Table(
            "test_postgres_table",
            MetaData(),
            Column("version", sqlalchemy.Integer, primary_key=True),
        )
        db_manager = create_db_table_manager(table, "", "test_schema")
        assert isinstance(db_manager, PostgresDbTableManager)

        # Test __init__
        assert db_manager.engine.dialect.name == "postgresql"
        assert isinstance(db_manager.table, Table)
        assert db_manager.table.name == table.name

        with db_manager.get_connection() as conn:
            # Create table
            db_manager.create_table(conn)

            # Table should be empty
            records = db_manager.select_all(conn)
            assert isinstance(records, list) and len(records) == 0

            # Insert N items
            rand_len = random.randint(10, 50)
            db_manager.insert_many(conn, [{"version": i} for i in range(rand_len)])
            records = db_manager.select_all(conn)
            assert isinstance(records, list) and len(records) == rand_len

            # Delete table items
            db_manager.delete_all(conn)
            records = db_manager.select_all(conn)
            assert isinstance(records, list) and len(records) == 0

            # Drop table
            db_manager.drop_table(conn)
            with pytest.raises(SQLAlchemyError):
                # Table not exist
                records = db_manager.select_all(conn)
