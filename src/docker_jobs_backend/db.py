"""Storage: jobs, their runs, and the attempts of each run

SQLite on the backend's volume, shared by the API's worker threads and the
scheduler thread. Three settings make that safe:

* WAL, so readers are not blocked by the writer;
* BEGIN IMMEDIATE for every transaction: the write lock is taken up front, so
  a read-then-write never fails half-way with "database is locked" (a
  deferred transaction cannot upgrade once another connection wrote) and two
  writers queue for busy_timeout instead;
* foreign_keys=ON, for the ON DELETE CASCADE of runs and attempts.

SQLite keeps no timezone: every datetime goes through UTCDateTime, stored as
naive UTC, read back as aware UTC, and a naive one is refused rather than
guessed. Ids are AUTOINCREMENT: a deleted run's id is never given to a new
one, so a service name (<stack>_<job>_<run>_<try>) never means two runs.
"""

import datetime
import enum
import uuid
from typing import List, Optional

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.types import TypeDecorator

__all__ = [
    "Attempt",
    "AttemptState",
    "Base",
    "Job",
    "Reason",
    "Run",
    "RunState",
    "RunType",
    "TERMINAL_RUN_STATES",
    "UTCDateTime",
    "create_session_factory",
    "new_token",
    "utcnow",
]

UTC = datetime.UTC


def utcnow() -> datetime.datetime:
    """now, aware, in UTC"""
    return datetime.datetime.now(tz=UTC)


def new_token() -> str:
    """an attempt's identity on its service: a label no other attempt shares"""
    return uuid.uuid4().hex


class UTCDateTime(TypeDecorator):
    """aware UTC datetimes over SQLite's naive ones"""

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(f"naive datetime {value!r}: give it a timezone")
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value, dialect):
        return None if value is None else value.replace(tzinfo=UTC)


class RunState(enum.StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_RUN_STATES = frozenset({RunState.SUCCESS, RunState.FAILED, RunState.CANCELLED})


class AttemptState(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    LOST = "lost"
    CANCELLED = "cancelled"


class RunType(enum.StrEnum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"


class Reason(enum.StrEnum):
    """why an attempt was made"""

    SCHEDULED = "scheduled"
    MANUAL = "manual"
    RETRY = "retry"
    RERUN = "rerun"


class Base(DeclarativeBase):
    pass


class Job(Base):
    """a container to run on a cron schedule, and how"""

    __tablename__ = "jobs"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True)
    description: Mapped[str] = mapped_column(Text, default="")
    image: Mapped[str] = mapped_column(String(512))
    entrypoint: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    command: Mapped[list] = mapped_column(JSON, default=list)
    env: Mapped[dict] = mapped_column(JSON, default=dict)
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    schedule: Mapped[str] = mapped_column(String(128))
    start_date: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    end_date: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    enabled: Mapped[bool] = mapped_column(default=True)
    catchup: Mapped[bool] = mapped_column(default=True)
    max_active_runs: Mapped[int] = mapped_column(default=1)
    retries: Mapped[int] = mapped_column(default=0)
    retry_delay_seconds: Mapped[int] = mapped_column(default=300)
    timeout_seconds: Mapped[int] = mapped_column(default=3600)
    networks: Mapped[list] = mapped_column(JSON, default=list)
    constraints: Mapped[list] = mapped_column(JSON, default=list)
    secrets: Mapped[list] = mapped_column(JSON, default=list)
    configs: Mapped[list] = mapped_column(JSON, default=list)
    mounts: Mapped[list] = mapped_column(JSON, default=list)
    cpu_limit: Mapped[Optional[float]] = mapped_column(nullable=True)
    memory_limit_mb: Mapped[Optional[int]] = mapped_column(nullable=True)
    # end of the last interval the scheduler created a run for, or skipped
    schedule_cursor: Mapped[Optional[datetime.datetime]] = mapped_column(
        UTCDateTime, nullable=True
    )
    created_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    updated_by: Mapped[str] = mapped_column(String(128), default="")

    runs: Mapped[List["Run"]] = relationship(
        back_populates="job", cascade="all, delete-orphan", passive_deletes=True
    )


class Run(Base):
    """one interval of a job -- or a manual run of one -- and its outcome"""

    __tablename__ = "runs"
    __table_args__ = (
        # the scheduler's idempotency: one scheduled run per interval;
        # manual runs may repeat one
        Index(
            "uq_runs_scheduled_interval",
            "job_id",
            "logical_date",
            unique=True,
            sqlite_where=text("run_type = 'scheduled'"),
        ),
        Index("ix_runs_state", "state"),
        {"sqlite_autoincrement": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    # = data_interval_start
    logical_date: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    data_interval_end: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    run_type: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    max_tries: Mapped[int]
    created_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    started_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    finished_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    requested_by: Mapped[str] = mapped_column(String(128), default="")
    note: Mapped[str] = mapped_column(Text, default="")

    job: Mapped[Job] = relationship(back_populates="runs")
    attempts: Mapped[List["Attempt"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Attempt.try_number",
    )

    @property
    def try_number(self) -> int:
        """the number of the latest attempt"""
        return max((attempt.try_number for attempt in self.attempts), default=0)


class Attempt(Base):
    """one try of a run: one Swarm service"""

    __tablename__ = "attempts"
    __table_args__ = (
        UniqueConstraint("run_id", "try_number"),
        Index("ix_attempts_state", "state"),
        {"sqlite_autoincrement": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    try_number: Mapped[int]
    token: Mapped[str] = mapped_column(String(32), unique=True, default=new_token)
    reason: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    requested_by: Mapped[str] = mapped_column(String(128), default="")
    note: Mapped[str] = mapped_column(Text, default="")
    not_before: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    service_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    service_name: Mapped[Optional[str]] = mapped_column(String(63), nullable=True)
    rendered_entrypoint: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    rendered_command: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    exit_code: Mapped[Optional[int]] = mapped_column(nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    node_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    launched_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    started_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    finished_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)

    run: Mapped[Run] = relationship(back_populates="attempts")


def _sqlite_connect(dbapi_connection, _record) -> None:
    # SQLAlchemy emits BEGIN itself (_sqlite_begin), not pysqlite
    dbapi_connection.isolation_level = None
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


def _sqlite_begin(connection) -> None:
    connection.exec_driver_sql("BEGIN IMMEDIATE")


def create_session_factory(database_url: str) -> sessionmaker:
    """the engine, the schema if it is missing, and sessions over them"""
    sqlite = database_url.startswith("sqlite")
    engine = create_engine(
        database_url, connect_args={"check_same_thread": False} if sqlite else {}
    )

    if sqlite:
        event.listen(engine, "connect", _sqlite_connect)
        event.listen(engine, "begin", _sqlite_begin)

    Base.metadata.create_all(engine)

    return sessionmaker(engine, expire_on_commit=False)
