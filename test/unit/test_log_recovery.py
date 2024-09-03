import time
import pytest
from pathlib import Path
import os
import pwd
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig

def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


@pytest.fixture
def current_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp(f'tests')
    os.mkdir(folder.join('scratch'))
    os.mkdir(folder.join('scheduler_tmp_dir'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner
    return folder


@pytest.fixture
def prepare_test(current_tmpdir):
    # touch as_misc
    platforms_path = Path(f"{current_tmpdir.strpath}/platforms_t000.yml")
    jobs_path = Path(f"{current_tmpdir.strpath}/jobs_t000.yml")
    project = "whatever"
    scratch_dir = f"{current_tmpdir.strpath}/scratch"
    Path(f"{scratch_dir}/{project}/{current_tmpdir.owner}").mkdir(parents=True, exist_ok=True)

    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {current_tmpdir.owner}
        project: {project}
        scratch_dir: {scratch_dir}
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write(f"""
JOBS:
    base:
        SCRIPT: |
            echo "Hello World"
            echo sleep 5
        QUEUE: hpc
        PLATFORM: pytest-ps
        RUNNING: once
        wallclock: 00:01
EXPERIMENT:
    # List of start dates
    DATELIST: '20000101'
    # List of members.
    MEMBERS: fc0
    # Unit of the chunk size. Can be hour, day, month, or year.
    CHUNKSIZEUNIT: month
    # Size of each chunk.
    CHUNKSIZE: '4'
    # Number of chunks of the experiment.
    NUMCHUNKS: '2'
    CHUNKINI: ''
    # Calendar used for the experiment. Can be standard or noleap.
    CALENDAR: standard
  """)
    return current_tmpdir


@pytest.fixture
def local(prepare_test):
    # Init Local platform
    from autosubmit.platforms.locplatform import LocalPlatform
    config = {
        'LOCAL_ROOT_DIR': f"{prepare_test}/scratch",
        'LOCAL_TMP_DIR': f"{prepare_test}/scratch",
    }
    local = LocalPlatform(expid='t000', name='local', config=config)
    return local


@pytest.fixture
def as_conf(prepare_test):
    as_conf = AutosubmitConfig("t000")
    as_conf.experiment_data = as_conf.load_config_file(as_conf.experiment_data, Path(prepare_test.join('platforms_t000.yml')))
    as_conf.misc_data = {"AS_COMMAND": "run"}
    return as_conf


def test_log_recovery_no_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=1)
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    time.sleep(2)
    assert local.log_recovery_process.is_alive() is False


def test_log_recovery_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=3)
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(3)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(3)
    assert local.log_recovery_process.is_alive()
    time.sleep(3)
    assert local.log_recovery_process.is_alive() is False


def test_log_recovery_keep_alive_cleanup(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=3)
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(3)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    local.cleanup_event.set()
    time.sleep(3)
    assert local.log_recovery_process.is_alive() is False