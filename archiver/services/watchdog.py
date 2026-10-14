"""
Safety net for crawl tasks whose worker disappeared (SIGKILL, OOM, redeploy,
host reboot, lost DB connection ...).

In those cases neither `except` nor `finally` of the task code runs, so the
Task stays `running` in the DB and on the frontend forever. Two pieces here:

* crawl_job_failed          - RQ `on_failure` callback (best effort)
* reap_stale_crawl_tasks    - periodic watchdog (the actual safety net)

reap_stale_crawl_tasks must run on a worker that has access to the Docker
socket (the `management` queue worker), because it stops orphaned containers.
"""
import logging
from datetime import timedelta

import django_rq
import docker
from django.db import close_old_connections
from django.utils import timezone
from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.worker import Worker

from archiver.models import Snapshot, Task, TaskStatus
from archiver.utils import TERMINAL_TASK_STATUSES

logger = logging.getLogger(__name__)

# Must be well above tasks.HEARTBEAT_INTERVAL_SECONDS (60 s).
STALE_AFTER = timedelta(minutes=10)

_UNFINISHED_SNAPSHOT_STATUSES = (
    Snapshot.STATUS_PENDING,
    Snapshot.STATUS_CRAWLING,
    Snapshot.STATUS_SUSPENDED,
)
_QUEUED_JOB_STATUSES = ("queued", "deferred", "scheduled")
_UNKNOWN = "unknown"


def _redis():
    return django_rq.get_connection("management")  # all queues share one Redis


def _job_status(job_id):
    """RQ job status as plain str, None if the job no longer exists, 'unknown' if Redis fails."""
    if not job_id:
        return None
    try:
        job = Job.fetch(job_id, connection=_redis())
        status = job.get_status()
        return getattr(status, "value", status)
    except NoSuchJobError:
        return None
    except Exception:
        logger.exception("Could not fetch RQ job %s", job_id)
        return _UNKNOWN


def _is_executed_by_live_worker(job_id) -> bool:
    """True if a registered (heartbeating) RQ worker reports this job as its current job."""
    for worker in Worker.all(connection=_redis()):
        if worker.get_current_job_id() == job_id:
            return True
    return False


def _stop_orphan_container(snapshot: Snapshot) -> None:
    if snapshot.replay_collection_id is None:
        return
    name = f"crawl_{snapshot.replay_collection_id}"
    try:
        container = docker.from_env().containers.get(name)
        if container.status == "paused":
            container.unpause()
        if container.status in ("running", "paused"):
            container.stop(timeout=30)   # graceful; container is kept for its logs
            logger.warning("Stopped orphaned crawl container %s", name)
    except docker.errors.NotFound:
        pass
    except Exception:
        logger.exception("Could not stop orphaned container %s", name)


def reap_stale_crawl_tasks(stale_after: timedelta = STALE_AFTER) -> int:
    """
    Mark `running` crawl tasks FAILED (and notify the frontend) when nothing is
    executing their RQ job anymore. Returns the number of reaped tasks.
    """
    close_old_connections()
    cutoff = timezone.now() - stale_after
    candidates = (
        Task.objects
        .filter(status=TaskStatus.RUNNING, action="crawl_run",
                snapshot__isnull=False, updateTime__lt=cutoff)
        .select_related("snapshot")
    )

    reaped = 0
    for task in candidates:
        try:
            snapshot = task.snapshot
            job_id = snapshot.rq_job_id
            job_status = _job_status(job_id)

            if job_status == _UNKNOWN or job_status in _QUEUED_JOB_STATUSES:
                continue
            if job_status == "started" and _is_executed_by_live_worker(job_id):
                logger.warning("Task %s: heartbeat is stale but its job is still "
                               "executed by a live worker - not reaping", task.uid)
                continue

            finished_ok = (job_status == "finished"
                           and snapshot.status == Snapshot.STATUS_COMPLETED)
            now = timezone.now()
            task.finishTime = task.finishTime or now

            if finished_ok:
                task.result = TaskStatus.SUCCESS
                task.send_quick_status_message(
                    TaskStatus.SUCCESS,
                    "Crawl finished, but the final task state was not stored "
                    "(recovered by watchdog).")
            else:
                _stop_orphan_container(snapshot)
                if snapshot.status in _UNFINISHED_SNAPSHOT_STATUSES:
                    snapshot.status = Snapshot.STATUS_FAILED
                    snapshot.crawlStopTimestamp = now
                    snapshot.save(update_fields=["status", "crawlStopTimestamp"])
                task.result = TaskStatus.FAILED
                task.send_quick_status_message(
                    TaskStatus.FAILED,
                    f"Crawl worker was lost (RQ job: {job_status or 'missing'}). "
                    f"The crawl container was stopped; partial data may exist in "
                    f"collection {snapshot.replay_collection_id}.")
            reaped += 1
        except Exception:
            logger.exception("Watchdog failed for task %s", task.uid)

    if reaped:
        logger.warning("Watchdog reaped %d stale crawl task(s)", reaped)
    return reaped


def crawl_job_failed(job, connection, exc_type, exc_value, exc_tb):
    """
    RQ `on_failure` callback. Best effort: it covers failures where the task code
    could not report itself (e.g. job timeout that killed the work-horse).
    It is a no-op when the task already has a final state.
    """
    try:
        close_old_connections()
        task_uid = (job.kwargs or {}).get("task_uid")
        task = Task.objects.filter(uid=task_uid).first() if task_uid else None
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return
        task.result = TaskStatus.FAILED
        task.finishTime = task.finishTime or timezone.now()
        name = getattr(exc_type, "__name__", exc_type)
        task.send_quick_status_message(TaskStatus.FAILED, f"RQ job failed: {name}: {exc_value}")
    except Exception:
        logger.exception("crawl_job_failed callback failed")
