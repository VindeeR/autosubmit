from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import json
import sqlite3
from typing import Optional
from autosubmitconfigparser.config.basicconfig import BasicConfig


@dataclass
class LockModel:
    """
    Data model of the Autosubmit Lock
    """

    lock_id: str
    hostname: str
    pid: int
    created: datetime = datetime.now(timezone.utc)


class LocksRepository(ABC):
    """
    Interface for Autosubmit Locks Repository
    """

    @abstractmethod
    def get_lock(self, lock_id: str) -> Optional[LockModel]:
        """
        Get the lock by lock_id. Return None if the lock does not exist.
        """

    @abstractmethod
    def insert_lock_from_model(self, lock: LockModel):
        """
        Insert the lock
        """

    @abstractmethod
    def insert_lock(self, lock_id: str, hostname: str, pid: int):
        """
        Insert the lock. Create a LockModel object first.
        """

    @abstractmethod
    def delete_lock(self, lock_id: str):
        """
        Delete the lock
        """


class LocksFileRepository(LocksRepository):
    """
    Repository of Autosubmit Locks using files.
    It creates a lock file for each lock which is stored in the directory.
    """

    def __init__(self, dir_path: str):
        self.dir_path = dir_path
        self._initialize()

    def _initialize(self):
        """
        Create the locks directory if it does not exist
        """
        os.makedirs(self.dir_path, exist_ok=True)

    def get_lock(self, lock_id: str) -> Optional[LockModel]:
        """
        Get the lock by lock_id. Return None if the lock does not exist.
        """
        lock_file = os.path.join(self.dir_path, lock_id)
        if not os.path.exists(lock_file):
            return None

        # Load from JSON format
        with open(lock_file, "r") as f:
            lock_data = json.load(f)
            return LockModel(
                lock_data["lock_id"],
                lock_data["hostname"],
                lock_data["pid"],
                datetime.fromisoformat(lock_data["created"]),
            )

    def insert_lock_from_model(self, lock: LockModel):
        """
        Insert the lock into the directory. Save content in JSON format.
        """
        lock_file = os.path.join(self.dir_path, lock.lock_id)
        # Save in JSON format
        with open(lock_file, "w") as f:
            json.dump(
                {
                    "lock_id": lock.lock_id,
                    "hostname": lock.hostname,
                    "pid": lock.pid,
                    "created": lock.created.isoformat(),
                },
                f,
            )

    def insert_lock(self, lock_id: str, hostname: str, pid: int):
        """
        Insert the lock into the directory. Create a LockModel object first.
        """
        new_lock = LockModel(lock_id, hostname, pid, datetime.now(timezone.utc))
        self.insert_lock_from_model(new_lock)

    def delete_lock(self, lock_id: str):
        """
        Delete the lock from the directory
        """
        lock_file = os.path.join(self.dir_path, lock_id)
        os.remove(lock_file)


class LocksDBRepository(LocksRepository):
    """
    Repository of Autosubmit Locks using SQLite
    """

    # TODO: Generalize with SQLAlchemy

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._initialize()

    def _initialize(self):
        """
        Create the locks table if it does not exist
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    lock_id TEXT PRIMARY KEY,
                    hostname TEXT,
                    pid INTEGER,
                    created TEXT
                )
            """)
            conn.commit()

    def get_lock(self, lock_id: str) -> Optional[LockModel]:
        """
        Get the lock by lock_id
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT lock_id, hostname, pid, created FROM locks WHERE lock_id = ?",
                (lock_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return LockModel(
                row[0],  # lock_id
                row[1],  # hostname
                row[2],  # pid
                datetime.fromisoformat(row[3]),  # created
            )

    def insert_lock_from_model(self, lock: LockModel):
        """
        Insert the lock into the database
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO locks VALUES (?, ?, ?, ?)",
                (lock.lock_id, lock.hostname, lock.pid, lock.created.isoformat()),
            )
            conn.commit()

    def insert_lock(self, lock_id: str, hostname: str, pid: int):
        """
        Insert the lock into the database. Create a LockModel object first.
        """
        new_lock = LockModel(lock_id, hostname, pid, datetime.now(timezone.utc))
        self.insert_lock_from_model(new_lock)

    def delete_lock(self, lock_id: str):
        """
        Delete the lock from the database
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM locks WHERE lock_id = ?", (lock_id,))
            conn.commit()


def create_locks_repository() -> LocksRepository:
    """
    Factory function to create the LocksRepository.

    """
    basic_config = BasicConfig()
    basic_config.read()

    # TODO: Use the BasicConfig to determine the repository type. For now, it is hardcoded to use the file repository.

    locks_dir_path = os.path.join(basic_config.LOCAL_ROOT_DIR, "locks")

    return LocksFileRepository(locks_dir_path)
