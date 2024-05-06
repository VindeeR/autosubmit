#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""
Module containing functions to manage autosubmit's database.
"""
import os
import multiprocessing
import traceback
from typing import List, Tuple
from log.log import Log, AutosubmitCritical
from autosubmitconfigparser.config.basicconfig import BasicConfig
from sqlalchemy import delete, select, text, Connection, insert, update, func
from sqlalchemy.schema import CreateTable
from autosubmit.database.session import create_sqlite_engine
from autosubmit.database import tables, session

Log.get_logger("Autosubmit")

CURRENT_DATABASE_VERSION = 1
TIMEOUT = 10


def create_db_pg():
    try:
        conn = open_conn(False)
    except Exception as exc:
        Log.error(traceback.format_exc())
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(exc)
        )

    try:
        conn.execute(CreateTable(tables.ExperimentTable.__table__, if_not_exists=True))
        conn.execute(CreateTable(tables.DBVersionTable.__table__, if_not_exists=True))
        conn.execute(delete(tables.DBVersionTable.__table__))
        conn.execute(insert(tables.DBVersionTable.__table__).values({"version": 1}))
    except Exception as exc:
        conn.rollback()
        close_conn(conn)
        raise AutosubmitCritical("Database can not be created", 7004, str(exc))

    close_conn(conn)

    return True


def create_db(qry: str) -> bool:
    """
    Creates a new database for autosubmit

    :param qry: query to create the new database
    :type qry: str    """

    try:
        conn = open_conn(False)
    except DbException as exc:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(exc)
        )

    try:
        for stmnt in qry.split(";"):
            if len(stmnt) > 0:
                conn.execute(text(stmnt))
    except Exception as exc:
        close_conn(conn)
        raise AutosubmitCritical("Database can not be created", 7004, str(exc))

    conn.commit()
    close_conn(conn)
    return True


def check_db() -> bool:
    """
    Checks if database file exist

    :return: None if exists, terminates program if not
    """
    BasicConfig.read()
    if BasicConfig.DATABASE_BACKEND == "sqlite" and not os.path.exists(BasicConfig.DB_PATH):
        raise AutosubmitCritical(
            'DB path does not exists: {0}'.format(BasicConfig.DB_PATH), 7003)
    return True


def open_conn(check_version=True) -> Connection:
    """
    Opens a connection to database

    :param check_version: If true, check if the database is compatible with this autosubmit version
    :type check_version: bool
    :return: connection object, cursor object
    :rtype: sqlite3.Connection, sqlite3.Cursor
    """
    BasicConfig.read()
    if BasicConfig.DATABASE_BACKEND == "postgres":
        conn = session.Session().bind.connect()
    else:
        conn = create_sqlite_engine(BasicConfig.DB_PATH).connect()

    # Getting database version
    if check_version and BasicConfig.DATABASE_BACKEND == "sqlite":
        try:
            row = conn.execute(select(tables.DBVersionTable)).one()
            version = row.version
        except Exception:
            # If this exception is thrown it's because db_version does not exist.
            # Database is from 2.x or 3.0 beta releases
            try:
                conn.execute(text('SELECT type FROM experiment;'))
                # If type field exists, it's from 2.x
                version = -1
            except Exception:
                # If raises and error , it's from 3.0 beta releases
                version = 0

        # If database version is not the expected, update database....
        if version < CURRENT_DATABASE_VERSION:
            if not _update_database(version, conn):
                raise AutosubmitCritical(
                    'Database version does not match', 7001)

        # ... or ask for autosubmit upgrade
        elif version > CURRENT_DATABASE_VERSION:
            raise AutosubmitCritical('Database version is not compatible with this autosubmit version. Please execute pip install '
                                     'autosubmit --upgrade', 7002)
    return conn


def close_conn(conn: Connection):
    """
    Commits changes and close connection to database

    :param conn: connection to close
    :type conn: sqlite3.Connection
    :param cursor: cursor to close
    :type cursor: sqlite3.Cursor
    """
    conn.commit()
    conn.close()

def fn_wrapper(database_fn, queue, *args):
    # TODO: We can also implement the anti-lock mechanism as function decorators in a next iteration.
    result = database_fn(*args)
    queue.put(result)
    queue.close()

def save_experiment(name, description, version):
    """
    Stores experiment in database. Anti-lock version.  

    :param version:
    :type version: str
    :param name: experiment's name
    :type name: str
    :param description: experiment's description
    :type description: str    
    """
    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(_save_experiment, queue, name, description, version))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except BaseException:
        raise AutosubmitCritical(
            "The database process exceeded the timeout limit {0}s. Your experiment {1} couldn't be stored in the database.".format(TIMEOUT, name))
    finally:
        proc.terminate()
    return result

def check_experiment_exists(name, error_on_inexistence=True):
    """ 
    Checks if exist an experiment with the given name. Anti-lock version.  

    :param error_on_inexistence: if True, adds an error log if experiment does not exist
    :type error_on_inexistence: bool
    :param name: Experiment name
    :type name: str
    :return: If experiment exists returns true, if not returns false
    :rtype: bool
    """
    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(_check_experiment_exists, queue, name, error_on_inexistence))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except BaseException:
        raise AutosubmitCritical(
            "The database process exceeded the timeout limit {0}s. Check if experiment {1} exists failed to complete.".format(TIMEOUT, name))
    finally:
        proc.terminate()
    return result

def update_experiment_descrip_version(name, description=None, version=None):
    """
    Updates the experiment's description and/or version. Anti-lock version.  

    :param name: experiment name (expid)  
    :rtype name: str  
    :param description: experiment new description  
    :rtype description: str  
    :param version: experiment autosubmit version  
    :rtype version: str  
    :return: If description has been update, True; otherwise, False.  
    :rtype: bool
    """
    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(_update_experiment_descrip_version, queue, name, description, version))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except BaseException:
        raise AutosubmitCritical(
            "The database process exceeded the timeout limit {0}s. Update experiment {1} version failed to complete.".format(TIMEOUT, name))
    finally:
        proc.terminate()
    return result

def get_autosubmit_version(expid):
    """
    Get the minimum autosubmit version needed for the experiment. Anti-lock version.

    :param expid: Experiment name
    :type expid: str
    :return: If experiment exists returns the autosubmit version for it, if not returns None
    :rtype: str
    """    
    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(_get_autosubmit_version, queue, expid))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except BaseException:
        raise AutosubmitCritical(
            "The database process exceeded the timeout limit {0}s. Get experiment {1} version failed to complete.".format(TIMEOUT, expid))
    finally:
        proc.terminate()
    return result

def last_name_used(test=False, operational=False):
    """
    Gets last experiment identifier used. Anti-lock version.  

    :param test: flag for test experiments
    :type test: bool
    :param operational: flag for operational experiments
    :type test: bool
    :return: last experiment identifier used, 'empty' if there is none
    :rtype: str
    """
    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(_last_name_used, queue, test, operational))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except BaseException as e:
        raise AutosubmitCritical(
            "The database process exceeded the timeout limit {0}s. Get last named used failed to complete.".format(TIMEOUT),7000,str(e))
    finally:
        proc.terminate()
    return result

def delete_experiment(experiment_id):
    """
    Removes experiment from database. Anti-lock version.  

    :param experiment_id: experiment identifier
    :type experiment_id: str
    :return: True if delete is successful
    :rtype: bool
    """
    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(_delete_experiment, queue, experiment_id))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except BaseException:
        raise AutosubmitCritical(
            "The database process exceeded the timeout limit {0}s. Delete experiment {1} failed to complete.".format(TIMEOUT, experiment_id))
    finally:
        proc.terminate()
    return result

def _save_experiment(name: str, description: str, version: str) -> bool:
    """
    Stores experiment in database

    :param version:
    :type version: str
    :param name: experiment's name
    :type name: str
    :param description: experiment's description
    :type description: str
    """
    if not check_db():
        return False
    try:
        conn = open_conn()
    except DbException as exc:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(exc))
    try:
        conn.execute(
            insert(tables.ExperimentTable).values(
                {
                    "name": name,
                    "description": description,
                    "autosubmit_version": version,
                }
            )
        )
    except Exception as exc:
        close_conn(conn)
        raise AutosubmitCritical(
            'Could not register experiment', 7005, str(exc))

    conn.commit()
    close_conn(conn)
    return True


def _check_experiment_exists(name: str, error_on_inexistence: bool = True) -> bool:
    """
    Checks if exist an experiment with the given name.

    :param error_on_inexistence: if True, adds an error log if experiment does not exist
    :type error_on_inexistence: bool
    :param name: Experiment name
    :type name: str
    :return: If experiment exists returns true, if not returns false
    :rtype: bool
    """

    if not check_db():
        return False
    try:
        conn = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(e))

    stmnt = select(tables.ExperimentTable).where(tables.ExperimentTable.name == name)
    row = conn.execute(stmnt).one_or_none()
    close_conn(conn)

    if row is None:
        if error_on_inexistence:
            raise AutosubmitCritical(
                'The experiment name "{0}" does not exist yet!!!'.format(name), 7005)
        if os.path.exists(os.path.join(BasicConfig.LOCAL_ROOT_DIR, name)):
            try:
                _save_experiment(name, 'No description', "3.14.0")
            except  BaseException as e:
                pass
            return True
        return False
    return True


def get_experiment_descrip(expid: str) -> List[Tuple[str]]:
    if not check_db():
        return False
    try:
        conn = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to the database.", 7001, str(e))

    stmnt = select(tables.ExperimentTable).where(tables.ExperimentTable.name == expid)
    row = conn.execute(stmnt).one_or_none()
    close_conn(conn)

    if row:
        return [[row.description]]
    return []


def _update_experiment_descrip_version(
    name: str, description: str = None, version: str = None
) -> bool:
    """
    Updates the experiment's description and/or version

    :param name: experiment name (expid)
    :rtype name: str
    :param description: experiment new description
    :rtype description: str
    :param version: experiment autosubmit version
    :rtype version: str
    :return: If description has been update, True; otherwise, False.
    :rtype: bool
    """
    # Conditional update statement
    if description is None and version is None:
        raise AutosubmitCritical("Not enough data to update {}.".format(name), 7005)

    stmnt = update(tables.ExperimentTable).where(tables.ExperimentTable.name == name)
    vals = {}
    if isinstance(description, str):
        vals["description"] = description
    if isinstance(version, str):
        vals["autosubmit_version"] = version
    stmnt = stmnt.values(vals)

    if not check_db():
        return False

    try:
        conn = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to the database.", 7001, str(e)
        )

    result = conn.execute(stmnt)
    close_conn(conn)

    if result.rowcount == 0:
        raise AutosubmitCritical("Update on experiment {} failed.".format(name), 7005)
    return True


def _get_autosubmit_version(expid: str) -> str:
    """
    Get the minimum autosubmit version needed for the experiment

    :param expid: Experiment name
    :type expid: str
    :return: If experiment exists returns the autosubmit version for it, if not returns None
    :rtype: str
    """
    if not check_db():
        return False

    try:
        conn = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(e))

    stmnt = select(tables.ExperimentTable).where(tables.ExperimentTable.name == expid)
    row = conn.execute(stmnt).one_or_none()
    close_conn(conn)

    if row is None:
        raise AutosubmitCritical(
            'The experiment "{0}" does not exist'.format(expid), 7005)
    return row.autosubmit_version


def _last_name_used(test: bool = False, operational: bool = False) -> str:
    """
    Gets last experiment identifier used

    :param test: flag for test experiments
    :type test: bool
    :param operational: flag for operational experiments
    :type test: bool
    :return: last experiment identifier used, 'empty' if there is none
    :rtype: str
    """
    if not check_db():
        return ""

    if test:
        condition = tables.ExperimentTable.name.like("t%")
    elif operational:
        condition = tables.ExperimentTable.name.like("o%")
    else:
        condition = tables.ExperimentTable.name.not_like(
            "t%"
        ) & tables.ExperimentTable.name.not_like("o%")

    sub_stmnt = select(func.max(tables.ExperimentTable.id).label("id")).where(
        condition
        & tables.ExperimentTable.autosubmit_version.is_not(None)
        & tables.ExperimentTable.autosubmit_version.not_like("%3.0.0b%")
    ).scalar_subquery()
    stmnt = select(tables.ExperimentTable.name).where(
        tables.ExperimentTable.id == sub_stmnt
    )

    try:
        conn = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(e)
        )

    row = conn.execute(stmnt).one_or_none()
    close_conn(conn)

    if row is None:
        return "empty"

    # If starts by number (during 3.0 beta some jobs starting with numbers where created), returns empty.
    try:
        if row.name.isnumeric():
            return "empty"
        else:
            return row.name
    except ValueError:
        return row.name


def _delete_experiment(experiment_id: str) -> bool:
    """
    Removes experiment from database

    :param experiment_id: experiment identifier
    :type experiment_id: str
    :return: True if delete is successful
    :rtype: bool
    """
    if not check_db():
        return False
    if not _check_experiment_exists(experiment_id, False): # Reference the no anti-lock version.
        return True
    try:
        conn = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(e))

    stmnt = delete(tables.ExperimentTable).where(
        tables.ExperimentTable.name == experiment_id
    )
    result = conn.execute(stmnt)
    close_conn(conn)

    if result.rowcount > 0:
        Log.debug("The experiment {0} has been deleted!!!", experiment_id)
    return True


def _update_database(version: int, conn: Connection):
    Log.info("Autosubmit's database version is {0}. Current version is {1}. Updating...",
             version, CURRENT_DATABASE_VERSION)
    try:
        # For databases from Autosubmit 2
        if version <= -1:
            conn.execute(
                text(
                    "CREATE TABLE experiment_backup(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                    "name VARCHAR NOT NULL, type VARCHAR, autosubmit_version VARCHAR, "
                    "description VARCHAR NOT NULL, model_branch VARCHAR, template_name VARCHAR, "
                    "template_branch VARCHAR, ocean_diagnostics_branch VARCHAR);"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO experiment_backup (name,type,description,model_branch,template_name,"
                    "template_branch,ocean_diagnostics_branch) SELECT name,type,description,model_branch,"
                    "template_name,template_branch,ocean_diagnostics_branch FROM experiment;"
                )
            )
            conn.execute(
                text('UPDATE experiment_backup SET autosubmit_version = "2";')
            )
            conn.execute(text("DROP TABLE experiment;"))
            conn.execute(
                text("ALTER TABLE experiment_backup RENAME TO experiment;")
            )

        if version <= 0:
            # Autosubmit beta version. Create db_version table
            conn.execute(CreateTable(tables.DBVersionTable.__table__, if_not_exists=True))
            conn.execute(text("INSERT INTO db_version (version) VALUES (1);"))
            conn.execute(
                text(
                    "ALTER TABLE experiment ADD COLUMN autosubmit_version VARCHAR;"
                )
            )
            conn.execute(
                text(
                    'UPDATE experiment SET autosubmit_version = "3.0.0b" '
                    "WHERE autosubmit_version NOT NULL;"
                )
            )

        conn.execute(
            text("UPDATE db_version SET version={0};".format(CURRENT_DATABASE_VERSION))
        )
    except Exception as exc:
        raise AutosubmitCritical(
            'unable to update database version', 7001, str(exc))
    Log.info("Update completed")
    return True


class DbException(Exception):
    """
    Exception class for database errors
    """

    def __init__(self, message):
        self.message = message
