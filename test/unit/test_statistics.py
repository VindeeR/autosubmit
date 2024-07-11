from datetime import datetime, timedelta
import time

import pytest  # TODO: need to import something or add to project requirements?
from typing import List, Any
from random import seed, randint, choice

from pyparsing import Dict

from autosubmit.job.job import Job
from autosubmit.statistics.jobs_stat import JobStat
from autosubmit.statistics.statistics import Statistics
from autosubmit.statistics.stats_summary import StatsSummary
from autosubmit.statistics.utils import timedelta2hours

POSSIBLE_STATUS = ["UNKNOWN", "WAITING", "DELAYED", "READY", "PREPARED", "SUBMITED", "HELD", "QUEUING", "RUNNING",
                   "SKIPPED", "COMPLETED", "FAILED", "SUSPENDED"]

NUM_JOBS = 1000  # modify this value to test with different job number
MAX_NUM_RETRIALS_PER_JOB = 20  # modify this value to test with different retrials number


@pytest.fixture(scope="function")
def jobs_instances():
    # type: () -> List[Job]
    jobs = []
    seed(time.time())
    submit_time = datetime(2023, 1, 1, 10, 0, 0)
    start_time = datetime(2023, 1, 1, 10, 30, 0)
    end_time = datetime(2023, 1, 1, 11, 0, 0)
    completed_retrial = [submit_time, start_time, end_time, "COMPLETED"]
    partial_retrials = [
        [submit_time, start_time, end_time, ""],
        [submit_time, start_time, ""],
        [submit_time, ""],
        [""]
    ]
    for i in range(NUM_JOBS):
        status = POSSIBLE_STATUS[i % len(POSSIBLE_STATUS)]  # random status
        job_aux = Job(name="example_name_" + str(i), job_id="example_id_" + str(i), status=status, priority=i)

        # Custom values for job attributes
        job_aux.processors = str(i)
        job_aux.wallclock = '00:05'
        job_aux.section = "example_section_" + str(i)
        job_aux.member = "example_member_" + str(i)
        job_aux.chunk = "example_chunk_" + str(i)
        job_aux.processors_per_node = str(i)
        job_aux.tasks = str(i)
        job_aux.nodes = str(i)
        job_aux.exclusive = "example_exclusive_" + str(i)

        num_retrials = randint(1, MAX_NUM_RETRIALS_PER_JOB)  # random number of retrials, grater than 0
        retrials = []

        for j in range(num_retrials):
            if j < num_retrials - 1:
                retrial = completed_retrial
            else:
                if job_aux.status == "COMPLETED":
                    retrial = completed_retrial
                else:
                    retrial = choice(partial_retrials)
                    if len(retrial) == 1:
                        retrial[0] = job_aux.status
                    elif len(retrial) == 2:
                        retrial[1] = job_aux.status
                    elif len(retrial) == 3:
                        retrial[2] = job_aux.status
                    else:
                        retrial[3] = job_aux.status
            retrials.append(retrial)
        job_aux.get_last_retrials = lambda: retrials  # override get_last_retrials method, similar to mock
        jobs.append(job_aux)

    return jobs


@pytest.fixture(scope="function")
def job_stats_instance():
    # type: () -> List[JobStat]
    job_stats_list = []
    for i in range(NUM_JOBS):
        job_stat = JobStat("example_name" + str(i), 0, 0.0, "", "", "", "", "", "", "", "")
        job_stat._name = "example_name" + str(i)
        job_stat._processors = i
        job_stat._wallclock = float(i)
        job_stat.submit_time = datetime(2023, 1, 1, 10, 0, 0)
        job_stat.start_time = datetime(2023, 1, 1, 10, 30, 0)
        job_stat.finish_time = datetime(2023, 1, 1, 11, 0, 0)
        job_stat.completed_queue_time = timedelta()
        job_stat.completed_run_time = timedelta()
        job_stat.failed_queue_time = timedelta()
        job_stat.failed_run_time = timedelta()
        job_stat.retrial_count = i
        job_stat.completed_retrial_count = i
        job_stat.failed_retrial_count = i
        job_stats_list.append(job_stat)
    return job_stats_list


@pytest.fixture(scope="function")
def statistics_instance(jobs_instances, job_stats_instance):
    # type: (List[Job], List[JobStat]) -> Statistics
    stats = Statistics(jobs=jobs_instances, start=datetime(2023, 1, 1, 10, 0, 0),
                       end=datetime(2023, 1, 1, 11, 0, 0), queue_time_fix={})
    stats.jobs_stat = job_stats_instance
    return stats


@pytest.fixture(params=[{
    "submitted_count": (NUM_JOBS * (NUM_JOBS - 1)) // 2,
    "run_count": (NUM_JOBS * (NUM_JOBS - 1)) // 2,
    "completed_count": (NUM_JOBS * (NUM_JOBS - 1)) // 2,
    "failed_count": (NUM_JOBS * (NUM_JOBS - 1)) // 2,
    "expected_consumption": (NUM_JOBS * (NUM_JOBS - 1)) / 2,
    "real_consumption": timedelta2hours(timedelta() + timedelta()) * NUM_JOBS,
    "failed_real_consumption": timedelta2hours(timedelta() + timedelta()) * NUM_JOBS,
    "expected_cpu_consumption": NUM_JOBS * (NUM_JOBS - 1) * (2 * NUM_JOBS - 1) / 6,
    "cpu_consumption": sum(
        timedelta2hours(i * timedelta()) + timedelta2hours(i * timedelta()) for i in range(NUM_JOBS)),
    "failed_cpu_consumption": sum(timedelta2hours(i * timedelta()) for i in range(NUM_JOBS)),
    "total_queue_time": sum(timedelta2hours(timedelta() + timedelta()) for _ in range(NUM_JOBS)),
    "cpu_consumption_percentage": 0.0
}], scope="function")
def summary_instance(request):
    summary = StatsSummary()
    data = request.param
    summary.submitted_count = data["submitted_count"]
    summary.run_count = data["run_count"]
    summary.completed_count = data["completed_count"]
    summary.failed_count = data["failed_count"]
    summary.expected_consumption = data["expected_consumption"]
    summary.real_consumption = data["real_consumption"]
    summary.failed_real_consumption = data["failed_real_consumption"]
    summary.expected_cpu_consumption = data["expected_cpu_consumption"]
    summary.cpu_consumption = data["cpu_consumption"]
    summary.failed_cpu_consumption = data["failed_cpu_consumption"]
    summary.total_queue_time = data["total_queue_time"]
    summary.cpu_consumption_percentage = data["cpu_consumption_percentage"]
    return summary


@pytest.fixture(scope="function")
def summary_instance_as_list(summary_instance):
    # type: (StatsSummary) -> List[str]
    return [
        "Summary: ",
        "{}  :  {}".format("CPU Consumption Percentage", str(summary_instance.cpu_consumption_percentage) + "%"),
        "{}  :  {:,} hrs.".format("Total Queue Time", round(summary_instance.total_queue_time, 2)),
        "{}  :  {:,}".format("Submitted Count", summary_instance.submitted_count),
        "{}  :  {:,}".format("Run Count", summary_instance.run_count),
        "{}  :  {:,}".format("Completed Count", summary_instance.completed_count),
        "{}  :  {:,}".format("Failed Count", summary_instance.failed_count),
        "{}  :  {:,} hrs.".format("Expected Consumption", round(summary_instance.expected_consumption, 4)),
        "{}  :  {:,} hrs.".format("Real Consumption", round(summary_instance.real_consumption, 4)),
        "{}  :  {:,} hrs.".format("Failed Real Consumption", round(summary_instance.failed_real_consumption, 4)),
        "{}  :  {:,} hrs.".format("Expected CPU Consumption", round(summary_instance.expected_cpu_consumption, 4)),
        "{}  :  {:,} hrs.".format("CPU Consumption", round(summary_instance.cpu_consumption, 4)),
        "{}  :  {:,} hrs.".format("Failed CPU Consumption", round(summary_instance.failed_cpu_consumption, 4))
    ]


@pytest.fixture(scope="function")
def make_old_format_instance():
    # type: () -> Dict[str, Any]
    return_dict = {}

    return_dict["start_times"] = [datetime(2023, 1, 1, 10, 30, 0) for _ in range(NUM_JOBS)]
    return_dict["end_times"] = [datetime(2023, 1, 1, 11, 0, 0) for _ in range(NUM_JOBS)]
    return_dict["queued"] = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    return_dict["run"] = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    return_dict["failed_jobs"] = [i for i in range(NUM_JOBS)]
    return_dict["max_fail"] = 0 if len(return_dict["failed_jobs"]) == 0 else max(return_dict["failed_jobs"])
    return_dict["fail_run"] = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    return_dict["fail_queued"] = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    return_dict["wallclocks"] = [float(i) for i in range(NUM_JOBS)]
    return_dict["threshold"] = 0.0 if len(return_dict["wallclocks"]) == 0 else max(return_dict["wallclocks"])
    max_queue = 0.0 if len(return_dict["queued"]) == 0 else max(return_dict["queued"])
    max_run = 0.0 if len(return_dict["run"]) == 0 else max(return_dict["run"])
    max_fail_queue = 0.0 if len(return_dict["fail_queued"]) == 0 else max(return_dict["fail_queued"])
    max_fail_run = 0.0 if len(return_dict["fail_run"]) == 0 else max(return_dict["fail_run"])
    return_dict["max_time"] = max(max_queue, max_run, max_fail_queue, max_fail_run, return_dict["threshold"])

    return return_dict


@pytest.fixture(scope="function")
def failed_jobs_only_list_instance(job_stats_instance):
    failed_jobs_only_list = [i for i in range(NUM_JOBS)]
    return_dict = {}
    for i in range(NUM_JOBS):
        if failed_jobs_only_list[i] > 0:
            return_dict[job_stats_instance[i].name] = failed_jobs_only_list[i]
    return return_dict


def test_working_functions(jobs_instances):
    # type: (List[Job]) -> None
    exp_stats = Statistics(jobs=jobs_instances, start=datetime(2023, 1, 1, 10, 0, 0),
                           end=datetime(2023, 1, 1, 11, 0, 0), queue_time_fix={})
    exp_stats.calculate_statistics()
    exp_stats.calculate_summary()
    exp_stats.get_summary_as_list()
    exp_stats.get_statistics()
    exp_stats.make_old_format()
    exp_stats.build_failed_jobs_only_list()


def test_calculate_statistics(statistics_instance, jobs_instances):
    # type: (Statistics, List[Job]) -> None
    stats = statistics_instance
    job_list = jobs_instances
    job_stats = stats.calculate_statistics()

    assert len(job_stats) == len(job_list)
    for index, job_stat in enumerate(job_stats):
        original_retrials = job_list[index].get_last_retrials()
        last_retrial = original_retrials[(len(original_retrials) - 1)]

        assert job_stat.retrial_count == len(original_retrials)
        assert job_stat.completed_retrial_count == len(
            [retrial for retrial in original_retrials
             if len(retrial) == 4 and retrial[3] == "COMPLETED"])

        assert job_stat.failed_retrial_count == len(
            [retrial for retrial in original_retrials
             if (len(retrial) == 4 and retrial[3] != "COMPLETED")
             or (len(retrial) < 4)])

        assert job_stat.submit_time == (
            last_retrial[0] if (len(last_retrial) == 4 or len(last_retrial) == 3 or len(last_retrial) == 2) else None)
        assert job_stat.start_time == (last_retrial[1] if (len(last_retrial) == 4 or len(last_retrial) == 3) else None)
        assert job_stat.finish_time == (last_retrial[2] if (len(last_retrial) == 4) else None)

        # TODO: by making retrials creation random it is "imposible" to predict the results of:
        # TODO: completed_queue_time, completed_run_time, failed_queue_time, failed_run_time
        # TODO: idea, remove randomness and create a fixed dataset dependending on a constant, easier to test



def test_calculate_summary(statistics_instance, summary_instance):
    # type: (Statistics, StatsSummary) -> None
    statistics_instance.calculate_summary()
    summary = statistics_instance.summary

    # Counter
    assert summary.submitted_count == summary_instance.submitted_count
    assert summary.run_count == summary_instance.run_count
    assert summary.completed_count == summary_instance.completed_count
    # Consumption
    assert summary.expected_consumption == summary_instance.expected_consumption
    assert summary.real_consumption == summary_instance.real_consumption
    assert summary.failed_real_consumption == summary_instance.failed_real_consumption
    # CPU Consumption
    assert summary.expected_cpu_consumption == summary_instance.expected_cpu_consumption
    assert summary.cpu_consumption == summary_instance.cpu_consumption
    assert summary.failed_cpu_consumption == summary_instance.failed_cpu_consumption
    assert summary.total_queue_time == summary_instance.total_queue_time
    assert summary.cpu_consumption_percentage == summary_instance.cpu_consumption_percentage


def test_get_summary_as_list(statistics_instance, summary_instance_as_list):
    # type: (Statistics, List[str]) -> None
    statistics_instance.calculate_summary()
    summary_as_list = statistics_instance.summary.get_as_list()

    assert summary_as_list == summary_instance_as_list


def test_make_old_format(statistics_instance, make_old_format_instance):
    # type: (Statistics, Dict[str, Any]) -> None
    statistics_instance.make_old_format()
    assert statistics_instance.start_times == make_old_format_instance["start_times"]
    assert statistics_instance.end_times == make_old_format_instance["end_times"]
    assert statistics_instance.queued == make_old_format_instance["queued"]
    assert statistics_instance.run == make_old_format_instance["run"]
    assert statistics_instance.failed_jobs == make_old_format_instance["failed_jobs"]
    assert statistics_instance.max_fail == make_old_format_instance["max_fail"]
    assert statistics_instance.fail_run == make_old_format_instance["fail_run"]
    assert statistics_instance.fail_queued == make_old_format_instance["fail_queued"]
    assert statistics_instance.wallclocks == make_old_format_instance["wallclocks"]
    assert statistics_instance.threshold == make_old_format_instance["threshold"]
    assert statistics_instance.max_time == make_old_format_instance["max_time"]


def test_build_failed_job_only(statistics_instance, failed_jobs_only_list_instance):
    # type: (Statistics, Dict[str, int]) -> None
    statistics_instance.make_old_format()
    statistics_instance.build_failed_jobs_only_list()

    assert statistics_instance.failed_jobs_dict == failed_jobs_only_list_instance
