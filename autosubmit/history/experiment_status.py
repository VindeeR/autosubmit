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

import os
import traceback
from sqlalchemy import delete, insert, select
from autosubmit.database import tables
from autosubmit.database.db_manager import create_db_table_manager
from autosubmit.history.internal_logging import Logging
from autosubmitconfigparser.config.basicconfig import BasicConfig
import autosubmit.history.utils as HUtils
from autosubmit.history.database_managers import database_models as Models


class ExperimentStatus:
    """Represents the Experiment Status Mechanism that keeps track of currently active experiments"""

    def __init__(self, expid: str):
        self.expid = expid
        BasicConfig.read()
        try:
            self.db_manager = create_db_table_manager(
                tables.ExperimentTable, BasicConfig.DB_PATH
            )
            self.as_times_manager = create_db_table_manager(
                tables.ExperimentStatusTable,
                os.path.join(BasicConfig.DB_DIR, BasicConfig.AS_TIMES_DB),
            )
        except Exception as exc:
            message = (
                "Error while trying to update {0} in experiment_status. {1}".format(
                    str(self.expid), str(exc)
                )
            )
            Logging(self.expid, BasicConfig.HISTORICAL_LOG_DIR).log(
                message, traceback.format_exc()
            )
            self.as_times_manager = None

    def set_as_running(self):
        """Set the status of the experiment in experiment_status of as_times.db as RUNNING. Creates the database, table and row if necessary."""
        # Get experiment data
        with self.db_manager.get_connection() as conn:
            row = conn.execute(
                select(tables.ExperimentTable).where(
                    tables.ExperimentTable.name == self.expid
                )
            ).one_or_none()
        if not row:
            raise ValueError("Experiment {0} not found".format(self.expid))
        exp_id: int = row.id

        # Set status on as_times
        with self.as_times_manager.get_connection() as conn:
            # Ensure table is created
            self.as_times_manager.create_table(conn)
            # Upsert: Delete/Insert Strategy
            conn.execute(
                delete(tables.ExperimentStatusTable).where(
                    tables.ExperimentStatusTable.exp_id == exp_id
                )
            )
            conn.execute(
                insert(tables.ExperimentStatusTable).values(
                    {
                        "exp_id": exp_id,
                        "expid": self.expid,
                        "status": Models.RunningStatus.RUNNING,
                        "seconds_diff": 0,
                        "modified": HUtils.get_current_datetime(),
                    }
                )
            )
            conn.commit()
