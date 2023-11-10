# Copyright 2015-2023 Earth Sciences Department, BSC-CNS
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import pytest

import autosubmit.database.db_structure as DbStructure
from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_utils import SubJobManager, SubJob
from autosubmit.statistics.statistics import Statistics
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig


# from autosubmit.database.db_jobdata import JobDataStructure, ExperimentGraphDrawing

@pytest.mark.skip("TODO: looks like this test was used by devs to run an existing experiment a49z")
def test_normal_execution():
    print("Testing normal execution")
    expid = "a49z"
    period_fi = ""
    period_ini = ""
    ft = "Any"
    results = None
    subjobs = list()
    BasicConfig.read()
    path_structure = BasicConfig.STRUCTURES_DIR
    path_local_root = BasicConfig.LOCAL_ROOT_DIR
    as_conf = AutosubmitConfig(expid)
    as_conf.reload(force_load=True)
    job_list = Autosubmit.load_job_list(expid, as_conf, False)
    jobs_considered = [job for job in job_list.get_job_list() if job.status not in [
        Status.READY, Status.WAITING]]
    job_to_package, package_to_jobs, _, _ = JobList.retrieve_packages(
        BasicConfig, expid, [job.name for job in job_list.get_job_list()])
    queue_time_fixes = {}
    if job_to_package:
        current_table_structure = DbStructure.get_structure(expid, BasicConfig.STRUCTURES_DIR)
        subjobs = []
        for job in job_list.get_job_list():
            job_info = JobList.retrieve_times(job.status, job.name, job._tmp_path, make_exception=False,
                                              job_times=None, seconds=True, job_data_collection=None)
            time_total = (job_info.queue_time + job_info.run_time) if job_info else 0
            subjobs.append(
                SubJob(job.name,
                       job_to_package.get(job.name, None),
                       job_info.queue_time if job_info else 0,
                       job_info.run_time if job_info else 0,
                       time_total,
                       job_info.status if job_info else Status.UNKNOWN)
            )
        queue_time_fixes = SubJobManager(subjobs, job_to_package, package_to_jobs,
                                         current_table_structure).get_collection_of_fixes_applied()

    if len(jobs_considered) > 0:
        print("Get results")
        exp_stats = Statistics(jobs_considered, period_ini, period_fi, queue_time_fixes)
        exp_stats.calculate_statistics()
        exp_stats.calculate_summary()
        exp_stats.make_old_format()
        print(exp_stats.get_summary_as_list())
        failed_jobs_dict = exp_stats.build_failed_jobs_only_list()
    else:
        raise Exception(
            "Autosubmit API couldn't find jobs that match your search criteria (Section: {0}) in the period from {1} to {2}.".format(
                ft, period_ini, period_fi))
    return results
