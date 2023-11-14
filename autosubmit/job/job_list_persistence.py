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

import os
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
import pickle
from sys import setrecursionlimit

from autosubmit.database.db_manager import DbManager
from log.log import Log


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
        if os.path.exists(path):
            with open(path, 'rb') as fd:
                graph = pickle.load(fd)
            # add again the children as it is deleted when saving the graph ( otherwise it raises a segvfault during pickle)
            resetted_nodes = []
            for i, u in enumerate(graph):
                u_nbrs = set(graph[u])
                # Get JOB node atributte of all neighbors of current node
                # and add it to current node as job_children
                #debug
                if graph.nodes[u]["job"] not in resetted_nodes:
                    resetted_nodes.append(graph.nodes[u]["job"])
                    graph.nodes[u]["job"].children = set()
                    graph.nodes[u]["job"].parents = set()
                graph.nodes[u]["job"].add_children([graph.nodes[v]["job"] for v in u_nbrs])
            return graph
        else:
            Log.printlog('File {0} does not exist'.format(path),Log.WARNING)
            return list()

    def save(self, persistence_path, persistence_file, job_list, graph):
        """
        Persists a job list in a pkl file
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str

        """
        path = os.path.join(persistence_path, persistence_file + '.pkl')
        setrecursionlimit(500000000)
        Log.debug("Saving JobList: " + path)
        #jobs_data = [(job.name, job.id, job.status,
        #              job.priority, job.section, job.date,
        #              job.member, job.chunk, job.split,
        #              job.local_logs[0], job.local_logs[1],
        #              job.remote_logs[0], job.remote_logs[1],job.wrapper_type) for job in job_list]

        with open(path, 'wb') as fd:
            pickle.dump(graph, fd, pickle.HIGHEST_PROTOCOL)
        Log.debug('Job list saved')


class JobListPersistenceDb(JobListPersistence):
    """
    Class to manage the database persistence of the job lists

    """

    VERSION = 3
    JOB_LIST_TABLE = 'job_list'
    TABLE_FIELDS = ['name', 'id', 'status', 'priority',
                    'section', 'date', 'member', 'chunk',
                    'local_out', 'local_err',
                    'remote_out', 'remote_err']

    def __init__(self, persistence_path, persistence_file):
        self.db_manager = DbManager(persistence_path, persistence_file, self.VERSION)

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from a database
        :param persistence_file: str
        :param persistence_path: str

        """
        return self.db_manager.select_all(self.JOB_LIST_TABLE)

    def save(self, persistence_path, persistence_file, job_list, graph):
        """
        Persists a job list in a database
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str

        """
        self._reset_table()
        jobs_data = [(job.name, job.id, job.status,
                      job.priority, job.section, job.date,
                      job.member, job.chunk, job.split,
                      job.local_logs[0], job.local_logs[1],
                      job.remote_logs[0], job.remote_logs[1],job.wrapper_type) for job in job_list]
        self.db_manager.insertMany(self.JOB_LIST_TABLE, jobs_data)

    def _reset_table(self):
        """
        Drops and recreates the database

        """
        self.db_manager.drop_table(self.JOB_LIST_TABLE)
        self.db_manager.create_table(self.JOB_LIST_TABLE, self.TABLE_FIELDS)
