import pytest
import shutil
from random import randrange

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmitconfigparser.config.basicconfig import BasicConfig


@pytest.fixture
def prepare_basic_config(tmpdir):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (tmpdir / "exp_root")
    BasicConfig.DB_FILE = "debug.db"
    BasicConfig.LOCAL_ROOT_DIR = (tmpdir / "exp_root")
    BasicConfig.LOCAL_TMP_DIR = "tmp"
    BasicConfig.LOCAL_ASLOG_DIR = "ASLOGS"
    BasicConfig.LOCAL_PROJ_DIR = "proj"
    BasicConfig.DEFAULT_PLATFORMS_CONF = ""
    BasicConfig.CUSTOM_PLATFORMS_PATH = ""
    BasicConfig.DEFAULT_JOBS_CONF = ""
    BasicConfig.SMTP_SERVER = ""
    BasicConfig.MAIL_FROM = ""
    BasicConfig.ALLOWED_HOSTS = ""
    BasicConfig.DENIED_HOSTS = ""
    BasicConfig.CONFIG_FILE_FOUND = False
    return basic_conf


@pytest.fixture
def setup_job_list(create_as_conf, tmpdir, mocker, prepare_basic_config):
    experiment_id = 'random-id'
    as_conf = create_as_conf
    as_conf.experiment_data = dict()
    as_conf.experiment_data["JOBS"] = dict()
    as_conf.jobs_data = as_conf.experiment_data["JOBS"]
    as_conf.experiment_data["PLATFORMS"] = dict()
    job_list = JobList(experiment_id, prepare_basic_config, YAMLParserFactory(),
                       JobListPersistenceDb(tmpdir, 'db'), as_conf)
    dummy_serial_platform = mocker.MagicMock()
    dummy_serial_platform.name = 'serial'
    dummy_platform = mocker.MagicMock()
    dummy_platform.serial_platform = dummy_serial_platform
    dummy_platform.name = 'dummy_platform'

    jobs = {
        "completed": [create_dummy_job_with_status(Status.COMPLETED, dummy_platform) for _ in range(4)],
        "submitted": [create_dummy_job_with_status(Status.SUBMITTED, dummy_platform) for _ in range(3)],
        "running": [create_dummy_job_with_status(Status.RUNNING, dummy_platform) for _ in range(2)],
        "queuing": [create_dummy_job_with_status(Status.QUEUING, dummy_platform)],
        "failed": [create_dummy_job_with_status(Status.FAILED, dummy_platform) for _ in range(4)],
        "ready": [create_dummy_job_with_status(Status.READY, dummy_platform) for _ in range(3)],
        "waiting": [create_dummy_job_with_status(Status.WAITING, dummy_platform) for _ in range(2)],
        "unknown": [create_dummy_job_with_status(Status.UNKNOWN, dummy_platform)]
    }

    job_list._job_list = [job for job_list in jobs.values() for job in job_list]
    waiting_job = jobs["waiting"][0]
    waiting_job.parents.update(jobs["ready"] + jobs["completed"] + jobs["failed"] + jobs["submitted"] + jobs["running"] + jobs["queuing"])

    yield job_list, waiting_job, jobs
    shutil.rmtree(tmpdir)


def create_dummy_job_with_status(status, platform):
    job_name = str(randrange(999999, 999999999))
    job_id = randrange(1, 999)
    job = Job(job_name, job_id, status, 0)
    job.type = randrange(0, 2)
    job.platform = platform
    return job


def test_add_edge_job(setup_job_list):
    _, waiting_job, _ = setup_job_list
    special_variables = {"STATUS": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0}
    for p in waiting_job.parents:
        waiting_job.add_edge_info(p, special_variables)
    for parent in waiting_job.parents:
        assert waiting_job.edge_info[special_variables["STATUS"]][parent.name] == (parent, special_variables.get("FROM_STEP", 0))


def test_add_edge_info_joblist(setup_job_list):
    job_list, waiting_job, jobs = setup_job_list
    special_conditions = {"STATUS": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0}
    job_list._add_edges_map_info(waiting_job, special_conditions["STATUS"])
    assert len(job_list.jobs_edges.get(Status.VALUE_TO_KEY[Status.COMPLETED], [])) == 1
    job_list._add_edges_map_info(jobs["waiting"][1], special_conditions["STATUS"])
    assert len(job_list.jobs_edges.get(Status.VALUE_TO_KEY[Status.COMPLETED], [])) == 2


def test_check_special_status(setup_job_list):
    job_list, waiting_job, jobs = setup_job_list
    waiting_job.edge_info = dict()
    job_list.jobs_edges = dict()
    statuses = [Status.VALUE_TO_KEY[Status.COMPLETED], Status.VALUE_TO_KEY[Status.READY], Status.VALUE_TO_KEY[Status.RUNNING], Status.VALUE_TO_KEY[Status.SUBMITTED], Status.VALUE_TO_KEY[Status.QUEUING], Status.VALUE_TO_KEY[Status.FAILED]]
    for status in statuses:
        job_list._add_edges_map_info(waiting_job, status)
    special_variables = dict()
    for p in waiting_job.parents:
        special_variables["STATUS"] = Status.VALUE_TO_KEY[p.status]
        special_variables["FROM_STEP"] = 0
        waiting_job.add_edge_info(p, special_variables)
    jobs_to_check = job_list.check_special_status()
    for job in jobs_to_check:
        tmp = [parent for parent in job.parents if parent.status == Status.COMPLETED or parent in job_list.jobs_edges["ALL"]]
        assert len(tmp) == len(job.parents)
    waiting_job.add_parent(jobs["waiting"][1])
    for job in jobs_to_check:
        tmp = [parent for parent in job.parents if parent.status == Status.COMPLETED or parent in job_list.jobs_edges["ALL"]]
        assert len(tmp) == len(job.parents)


