#!/usr/bin/env python3

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

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

import os
import pickle
from sys import setrecursionlimit
import shutil
from autosubmit.database import tables
from autosubmit.database.session import create_sqlite_engine
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy import select, insert
from log.log import Log
from contextlib import suppress


class JobListPersistence(object):
    """
    Class to manage the persistence of the job lists

    """

    def save(self, persistence_path, persistence_file, job_list , graph):
        """
        Persists a job list
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str

        """
        raise NotImplementedError

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from persistence
        :param persistence_file: str
        :param persistence_path: str

        """
        raise NotImplementedError


class JobListPersistencePkl(JobListPersistence):
    """
    Class to manage the pickle persistence of the job lists

    """

    EXT = '.pkl'

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from a pkl file
        :param persistence_file: str
        :param persistence_path: str

        """
        path = os.path.join(persistence_path, persistence_file + '.pkl')
        path_tmp = os.path.join(persistence_path[:-3]+"tmp", persistence_file + f'.pkl.tmp_{os.urandom(8).hex()}')

        try:
            open(path).close()
        except PermissionError:
            Log.warning(f'Permission denied to read {path}')
            raise
        except FileNotFoundError:
            Log.warning(f'File {path} does not exist. ')
            raise
        else:
            # copy the path to a tmp file randomseed to avoid corruption
            try:
                shutil.copy(path, path_tmp)
                with open(path_tmp, 'rb') as fd:
                    graph = pickle.load(fd)
            finally:
                os.remove(path_tmp)
            for u in ( node for node in graph ):
                # Set after the dependencies are set
                graph.nodes[u]["job"].children = set()
                graph.nodes[u]["job"].parents = set()
                # Set in recovery/run
                graph.nodes[u]["job"]._platform = None
                graph.nodes[u]["job"]._serial_platform = None
                graph.nodes[u]["job"].submitter = None
            return graph

    def save(self, persistence_path, persistence_file, job_list, graph):
        """
        Persists a job list in a pkl file
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str

        """

        path = os.path.join(persistence_path, persistence_file + '.pkl' + '.tmp')
        with suppress(FileNotFoundError, PermissionError):
            os.remove(path)

        setrecursionlimit(500000000)
        Log.debug("Saving JobList: " + path)
        with open(path, 'wb') as fd:
            pickle.dump(graph, fd, pickle.HIGHEST_PROTOCOL)
        os.replace(path, path[:-4])
        Log.debug(f'JobList saved in {path[:-4]}')


class JobListPersistenceDb(JobListPersistence):
    """
    Class to manage the SQLite database persistence of the job lists

    """

    VERSION = 3
    JOB_LIST_TABLE = 'job_list'
    TABLE_FIELDS = [
        "name",
        "id",
        "status",
        "priority",
        "section",
        "date",
        "member",
        "chunk",
        "split",
        "local_out",
        "local_err",
        "remote_out",
        "remote_err",
        "wrapper_type",
    ]

    def __init__(self, persistence_path: str, persistence_file: str):
        self.engine = create_sqlite_engine(
            os.path.join(persistence_file, persistence_path) + ".db"
        )
        with self.engine.connect() as conn:
            conn.execute(CreateTable(tables.JobListTable.__table__, if_not_exists=True))
            conn.commit()

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from a database
        :param persistence_file: str
        :param persistence_path: str
        """
        # TODO return DiGraph
        with self.engine.connect() as conn:
            rows = conn.execute(select(tables.JobListTable)).all()
        return [r.tuple() for r in rows]

    def save(self, persistence_path, persistence_file, job_list, graph):
        """
        Persists a job list in a database
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str
        """
        self._reset_table()
        jobs_data = [
            {
                "name": job.name,
                "id": job.id,
                "status": job.status,
                "priority": job.priority,
                "section": job.section,
                "date": job.date,
                "member": job.member,
                "chunk": job.chunk,
                "split": job.split,
                "local_out": job.local_logs[0],
                "local_err": job.local_logs[1],
                "remote_out": job.remote_logs[0],
                "remote_err": job.remote_logs[1],
                "wrapper_type": job.wrapper_type,
            }
            for job in job_list
        ]

        with self.engine.connect() as conn:
            conn.execute(insert(tables.JobListTable), jobs_data)
            conn.commit()

    def _reset_table(self):
        """
        Drops and recreates the database
        """
        with self.engine.connect() as conn:
            conn.execute(DropTable(tables.JobListTable.__table__, if_exists=True))
            conn.execute(CreateTable(tables.JobListTable.__table__, if_not_exists=True))
            conn.commit()
