"""Storage: timezones, uniqueness, cascades, concurrent writers"""

import datetime
import threading

import pytest
from factories import HOUR, T0, make_job, make_run
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, StatementError

from docker_jobs_backend.db import Attempt, Job, Run, RunType


def test_datetimes_come_back_aware_utc(session_factory):
    paris = datetime.timezone(datetime.timedelta(hours=2))
    with session_factory() as session:
        job_id = make_job(session, start_date=datetime.datetime(2026, 10, 9, 2, tzinfo=paris)).id

    with session_factory() as session:
        stored = session.get(Job, job_id)

    assert stored.start_date == T0
    assert stored.start_date.utcoffset() == datetime.timedelta(0)


def test_a_naive_datetime_is_refused(session):
    with pytest.raises(StatementError, match="timezone"):
        make_job(session, start_date=datetime.datetime(2026, 10, 9))


def test_one_scheduled_run_per_interval_but_manual_runs_may_repeat_it(session):
    job = make_job(session)
    session.add_all(
        [
            make_run(job, run_type=RunType.MANUAL),
            make_run(job, run_type=RunType.MANUAL),
            make_run(job),
        ]
    )
    session.commit()

    session.add(make_run(job))
    with pytest.raises(IntegrityError):
        session.commit()


def test_deleting_a_job_deletes_its_runs_and_attempts(session):
    job = make_job(session)
    session.add(make_run(job, attempts=2))
    session.commit()

    session.delete(job)
    session.commit()

    assert session.scalar(select(func.count()).select_from(Run)) == 0
    assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_ids_are_never_reused(session):
    job_id = make_job(session).id
    run = make_run(session.get(Job, job_id))
    session.add(run)
    session.commit()
    first = run.id

    session.delete(run)
    session.commit()
    session.expire_all()
    again = make_run(session.get(Job, job_id), start=T0 + HOUR)
    session.add(again)
    session.commit()

    assert again.id > first


def test_attempts_are_ordered_and_tokened(session):
    job = make_job(session)
    run = make_run(job, attempts=3)
    session.add(run)
    session.commit()

    assert [attempt.try_number for attempt in run.attempts] == [1, 2, 3]
    assert run.try_number == 3
    tokens = {attempt.token for attempt in run.attempts}
    assert len(tokens) == 3 and all(len(token) == 32 for token in tokens)


def test_concurrent_read_modify_write_loses_nothing(session_factory):
    with session_factory() as session:
        job_id = make_job(session, max_active_runs=0).id

    def bump():
        for _ in range(20):
            with session_factory() as session:
                job = session.get(Job, job_id)
                job.max_active_runs += 1
                session.commit()

    threads = [threading.Thread(target=bump) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with session_factory() as session:
        assert session.get(Job, job_id).max_active_runs == 40


def test_an_interval_of_another_length_may_share_its_start(session):
    job = make_job(session)
    hourly = make_run(job)
    two_hours = make_run(job)
    two_hours.data_interval_end = T0 + 2 * HOUR
    session.add_all([hourly, two_hours])

    session.commit()
