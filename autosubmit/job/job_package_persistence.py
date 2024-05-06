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
from typing import List
from autosubmit.database import tables
from autosubmit.database.db_manager import create_db_table_manager
from autosubmitconfigparser.config.basicconfig import BasicConfig


class JobPackagePersistence(object):
    """
    Class that handles packages workflow.

    Create Packages Table, Wrappers Table.

    :param expid: Experiment id
    """

    VERSION = 1
    JOB_PACKAGES_TABLE = "job_package"
    WRAPPER_JOB_PACKAGES_TABLE = "wrapper_job_package"
    TABLE_FIELDS = ["exp_id", "package_name", "job_name"]

    def __init__(self, expid: str):
        BasicConfig.read()
        db_filepath = (
            os.path.join(
                os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                "job_packages_" + expid,
            )
            + ".db"
        )

        # Setup job_package manager
        self.packages_db_manager = create_db_table_manager(
            tables.JobPackageTable, db_filepath, expid
        )
        # Setup wrapper_job_package manager
        self.wrapper_db_manager = create_db_table_manager(
            tables.WrapperJobPackageTable, db_filepath, expid
        )

        # Create tables
        with self.packages_db_manager.get_connection() as conn:
            self.packages_db_manager.create_table(conn)

        with self.wrapper_db_manager.get_connection() as conn:
            self.wrapper_db_manager.create_table(conn)

    def load(self, wrapper=False):
        """
        Loads package of jobs from a database
        :param: wrapper: boolean
        :return: dictionary of jobs per package
        """
        if not wrapper:
            with self.packages_db_manager.get_connection() as conn:
                rows = self.packages_db_manager.select_all(conn)
        else:
            with self.wrapper_db_manager.get_connection() as conn:
                rows = self.wrapper_db_manager.select_all(conn)

        return [(row.exp_id, row.package_name, row.job_name) for row in rows]

    def save(self, package_name: str, jobs: List, exp_id: str, wrapper=False):
        """
        Persists a job list in a database
        :param package_name: str
        :param jobs: list of jobs
        :param exp_id: str
        :param wrapper: boolean
        """
        job_packages_data = []
        for job in jobs:
            job_packages_data.append(
                {"exp_id": exp_id, "package_name": package_name, "job_name": job.name}
            )

        if not wrapper:
            with self.packages_db_manager.get_connection() as conn:
                self.packages_db_manager.insert_many(conn, job_packages_data)

        with self.wrapper_db_manager.get_connection() as conn:
            self.wrapper_db_manager.insert_many(conn, job_packages_data)

    def reset_table(self, wrappers=False):
        """
        Drops and recreates the database
        """
        if not wrappers:
            with self.packages_db_manager.get_connection() as conn:
                self.packages_db_manager.drop_table(conn)
                self.packages_db_manager.create_table(conn)

        with self.wrapper_db_manager.get_connection() as conn:
            self.wrapper_db_manager.drop_table(conn)
            self.wrapper_db_manager.create_table(conn)
