from typing import Type, Union
from sqlalchemy import (
    MetaData,
    Integer,
    String,
    Table,
    Text,
    Float,
    UniqueConstraint,
    Column,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

metadata_obj = MetaData()


## SQLAlchemy ORM tables
class BaseTable(DeclarativeBase):
    metadata = metadata_obj


class ExperimentTable(BaseTable):
    """
    Is the main table, populated by Autosubmit. Should be read-only by the API.
    """

    __tablename__ = "experiment"

    id: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    autosubmit_version: Mapped[str] = mapped_column(String)


class DBVersionTable(BaseTable):
    __tablename__ = "db_version"

    version: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)


class ExperimentStructureTable(BaseTable):
    """
    Table that holds the structure of the experiment jobs
    """

    __tablename__ = "experiment_structure"

    e_from: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    e_to: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)


class ExperimentStatusTable(BaseTable):
    """
    Stores the status of the experiments
    """

    __tablename__ = "experiment_status"

    exp_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    seconds_diff: Mapped[int] = mapped_column(Integer, nullable=False)
    modified: Mapped[str] = mapped_column(Text, nullable=False)


class JobPackageTable(BaseTable):
    """
    Stores a mapping between the wrapper name and the actual job in slurm
    """

    __tablename__ = "job_package"

    exp_id: Mapped[str] = mapped_column(Text)
    package_name: Mapped[str] = mapped_column(Text, primary_key=True)
    job_name: Mapped[str] = mapped_column(Text, primary_key=True)


class WrapperJobPackageTable(BaseTable):
    """
    It is a replication. It is only created/used when using inspectand create or monitor
    with flag -cw in Autosubmit.\n
    This replication is used to not interfere with the current autosubmit run of that experiment
    since wrapper_job_package will contain a preview, not the real wrapper packages
    """

    __tablename__ = "wrapper_job_package"

    exp_id: Mapped[str] = mapped_column(Text)
    package_name: Mapped[str] = mapped_column(Text, primary_key=True)
    job_name: Mapped[str] = mapped_column(Text, primary_key=True)


# Reserved name "metadata" with Declarative API, SQLAlchemy Core Table should be used
experiment_run_table = Table(
    "experiment_run",
    metadata_obj,
    Column("run_id", Integer, primary_key=True),
    Column("created", Text, nullable=False),
    Column("modified", Text, nullable=False),
    Column("start", Integer, nullable=False),
    Column("finish", Integer),
    Column("chunk_unit", Text, nullable=False),
    Column("chunk_size", Integer, nullable=False),
    Column("completed", Integer, nullable=False),
    Column("total", Integer, nullable=False),
    Column("failed", Integer, nullable=False),
    Column("queuing", Integer, nullable=False),
    Column("running", Integer, nullable=False),
    Column("submitted", Integer, nullable=False),
    Column("suspended", Integer, nullable=False, default=0),
    Column("metadata", Text),
)


class JobDataTable(BaseTable):
    __tablename__ = "job_data"

    id: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    counter: Mapped[int] = mapped_column(Integer, nullable=False)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    created: Mapped[str] = mapped_column(Text, nullable=False)
    modified: Mapped[str] = mapped_column(Text, nullable=False)
    submit: Mapped[int] = mapped_column(Integer, nullable=False)
    start: Mapped[int] = mapped_column(Integer, nullable=False)
    finish: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    rowtype: Mapped[int] = mapped_column(Integer, nullable=False)
    ncpus: Mapped[int] = mapped_column(Integer, nullable=False)
    wallclock: Mapped[str] = mapped_column(Text, nullable=False)
    qos: Mapped[str] = mapped_column(Text, nullable=False)
    energy: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[str] = mapped_column(Text, nullable=False)
    section: Mapped[str] = mapped_column(Text, nullable=False)
    member: Mapped[str] = mapped_column(Text, nullable=False)
    chunk: Mapped[int] = mapped_column(Integer, nullable=False)
    last: Mapped[int] = mapped_column(Integer, nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[int] = mapped_column(Integer, nullable=False)
    extra_data: Mapped[str] = mapped_column(Text, nullable=False)
    nnodes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    run_id: Mapped[int] = mapped_column(Integer)
    MaxRSS: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    AveRSS: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    out: Mapped[str] = mapped_column(Text, nullable=False)
    err: Mapped[str] = mapped_column(Text, nullable=False)
    rowstatus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    children: Mapped[str] = mapped_column(Text)
    platform_output: Mapped[str] = mapped_column(Text)
    UniqueConstraint()

    # SQLAlchemy Composed unique constraint of ("counter", "job_name")
    __table_args__ = (UniqueConstraint("counter", "job_name"),)


def table_change_schema(
    schema: str, source: Union[Type[DeclarativeBase], Table]
) -> Table:
    """
    Copy the source table and change the schema of that SQLAlchemy table into a new table instance
    """
    if isinstance(source, type) and issubclass(source, DeclarativeBase):
        _source_table: Table = source.__table__
    elif isinstance(source, Table):
        _source_table = source
    else:
        raise RuntimeError("Invalid source type on table schema change")

    metadata = MetaData(schema=schema)
    dest_table = Table(_source_table.name, metadata)

    for col in _source_table.columns:
        dest_table.append_column(col.copy())

    return dest_table


class JobListTable(BaseTable):
    # TODO review column typing

    __tablename__ = "job_list"

    name: Mapped[str] = mapped_column(String, primary_key=True)
    id: Mapped[int] = mapped_column(Integer)
    status: Mapped[int] = mapped_column(Integer)
    priority: Mapped[int] = mapped_column(Integer)
    section: Mapped[str] = mapped_column(String)
    date: Mapped[str] = mapped_column(String)
    member: Mapped[str] = mapped_column(String)
    chunk: Mapped[int] = mapped_column(Integer)
    split: Mapped[int] = mapped_column(Integer)
    local_out: Mapped[str] = mapped_column(String)
    local_err: Mapped[str] = mapped_column(String)
    remote_out: Mapped[str] = mapped_column(String)
    remote_err: Mapped[str] = mapped_column(String)
