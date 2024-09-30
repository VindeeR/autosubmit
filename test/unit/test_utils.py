import os
import socket
import pytest
from pathlib import Path
from autosubmit.database.repositories.locks import (
    create_locks_repository,
    LocksRepository,
    LocksFileRepository,
)
from autosubmit.helpers.utils import ASLock
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import AutosubmitCritical


@pytest.fixture
def utils_tmp_path(tmp_path_factory):
    return tmp_path_factory.mktemp("utils")


class MockBasicConfig(BasicConfig):
    def read(self):
        pass


@pytest.fixture
def mock_basic_config(monkeypatch, utils_tmp_path):
    MockBasicConfig.LOCAL_ROOT_DIR = utils_tmp_path
    monkeypatch.setattr("autosubmit.helpers.utils.BasicConfig", MockBasicConfig)
    return MockBasicConfig


def test_aslock(mock_basic_config):
    """
    Test acquiring and releasing the lock
    """
    lock_id = "test_aslock"
    current_hostname = socket.gethostname()
    current_pid = os.getpid()

    # Test acquiring the lock
    lock_type = None
    with ASLock(lock_id) as locker:
        assert isinstance(locker.locks_repository, LocksRepository)

        if isinstance(locker.locks_repository, LocksFileRepository):
            # Check the lock file
            lock_type = "file"
            lock_file_path = Path(locker.locks_repository.dir_path) / lock_id
            assert lock_file_path.exists(), "Lock file should exist"
            assert lock_file_path.is_file(), "Lock file should be a file"
        # TODO: Add more lock types (SQLAlchemy)

        # Get and check the lock
        curr_lock = locker.locks_repository.get_lock(lock_id)
        assert curr_lock.hostname == current_hostname, "Hostname should match"
        assert curr_lock.pid == current_pid, "PID should match"

    # Test releasing the lock
    if lock_type == "file":
        assert not lock_file_path.exists(), "Lock file should not exist"
    # TODO: Add more lock types (SQLAlchemy)

    locks_repository = create_locks_repository()
    curr_lock = locks_repository.get_lock(lock_id)
    assert curr_lock is None


def test_aslock_error(mock_basic_config):
    """
    Test acquiring the lock multiple times should raise an error
    """
    lock_id = "test_aslock_error"

    # Test acquiring the lock
    with ASLock(lock_id):
        # Snapshot the current lock
        locks_repository = create_locks_repository()
        lock_snapshot = locks_repository.get_lock(lock_id)

        # Try creating lock multiple times
        for i in range(10):
            with pytest.raises(AutosubmitCritical):
                with ASLock(lock_id):
                    pass

            curr_lock = locks_repository.get_lock(lock_id)
            assert curr_lock.hostname == lock_snapshot.hostname, "Hostname should match"
            assert curr_lock.pid == lock_snapshot.pid, "PID should match"
            assert (
                curr_lock.created == lock_snapshot.created
            ), "Created datetime should match"
