import os
import re
import signal
import socket
from typing import Optional

import django_rq
from rq.job import Job

from archiver.models import Snapshot, Website, Task, TaskStatus
from archiver.tasks import (
    start_crawl_task,
    admin_platform_lock_task,
    admin_platform_unlock_task,
    crawl_throttle_task,
    crawl_unthrottle_task,
    website_publish_all_task,
    website_unpublish_all_task,
    website_group_set_schedule_task,
    website_group_set_crawl_config_task,
    website_group_priority_crawl_task,
    export_zosia_task,
    repopulate_snapshot_to_production_task,
    replay_unpublish_task,
    replay_publish_task
)

UUID_REGEX = re.compile(r"^[0-9a-fA-F-]{32,36}$")
redis_conn = django_rq.get_connection("crawls")

# ------------------------
# INTERNAL HELPERS
# ------------------------
def _queue_exists(job_id: str) -> bool:
    return redis_conn.exists(f"crawl:{job_id}") == 1


def _send_control(job_id: str, command: str):
    redis_conn.set(f"crawl:{job_id}:control", command)


def resolve_job_or_website(identifier: str) -> Snapshot:
    """
    Accepts any of:
      - Snapshot.id  (integer)
      - rq_job_id    (UUID string)
      - website id
      - website name
    Returns a Snapshot instance.
    """

    # ----------------------------------------------------------
    # 1. If input is integer → treat as Snapshot primary key
    # ----------------------------------------------------------
    try:
        int_id = int(identifier)  # will raise ValueError if not an int
        return Snapshot.objects.get(id=int_id)
    except (ValueError, Snapshot.DoesNotExist):
        pass

    # ----------------------------------------------------------
    # 2. If UUID-like string → treat as rq_job_id
    # ----------------------------------------------------------
    if isinstance(identifier, str) and UUID_REGEX.match(identifier):
        try:
            return Snapshot.objects.get(rq_job_id=identifier)
        except Snapshot.DoesNotExist:
            pass

    # ----------------------------------------------------------
    # 3. Try as website-id
    # ----------------------------------------------------------
    try:
        website_id = int(identifier)
        return Snapshot.objects.filter(website_id=website_id).latest("id")
    except:
        pass

    # ----------------------------------------------------------
    # 4. Try as website-name
    # ----------------------------------------------------------
    try:
        website = Website.objects.get(name=identifier)
        return Snapshot.objects.filter(website=website).latest("id")
    except:
        pass

    # ----------------------------------------------------------
    # 5. No match → error
    # ----------------------------------------------------------
    raise Snapshot.DoesNotExist(f"No Snapshot found for identifier={identifier}")


def queue_crawl(website_id: int, task: Task = None, queue_name: str = "crawls") -> str:
    """
    Enqueue a crawl job for the given website.
    Also creates a Snapshot database entry storing the RQ job ID.
    """

    # Validate website
    website = Website.objects.get(id=website_id)
    snapshot = Snapshot.objects.create(
        website=website,
        status=Snapshot.STATUS_PENDING,
    )
    if not task:
        task = Task.create_for_website(
            snapshot=snapshot,
            action="crawl_run",
        )
    else:
        task.snapshot = snapshot
    task.status = TaskStatus.CREATED
    task.save()

    # Enqueue the RQ job
    queue = django_rq.get_queue(queue_name)
    job = queue.enqueue(start_crawl_task, snapshot_uid=snapshot.uid, task_uid=task.uid)

    # Attach the RQ job ID and save
    snapshot.rq_job_id = job.id
    snapshot.save(update_fields=["rq_job_id"])
    snapshot.send_create_response()

    return job.id


def get_crawl_status(job_id: str, queue_name: str = "crawls") -> dict:
    """
    Get the status of a crawl job.
    Returns dictionary with id, status, and result if finished.
    """
    try:
        job = Job.fetch(job_id, connection=django_rq.get_connection(queue_name))
    except Exception:
        return {"error": "Unknown job id"}

    return {
        "id": job.id,
        "status": job.get_status(),
        "result": job.result if job.is_finished else None,
    }


# ------------------------
# STOP CRAWL
# ------------------------
def stop_crawl(identifier: str) -> bool:
    cj = resolve_job_or_website(identifier)
    job_id = cj.rq_job_id

    # Prefer redis control (remote-safe)
    if _queue_exists(job_id):
        _send_control(job_id, "stop")
        cj.status = "cancelled"
        cj.save(update_fields=["status"])
        return True

    # Fallback: local kill if this is the right machine
    if cj.machine == socket.gethostname() and cj.process_id:
        try:
            os.kill(cj.process_id, signal.SIGTERM)
            cj.status = "cancelled"
            cj.save(update_fields=["status"])
            return True
        except ProcessLookupError:
            return False

    return False


# ------------------------
# SUSPEND CRAWL
# ------------------------
def suspend_crawl(identifier: str) -> bool:
    cj = resolve_job_or_website(identifier)
    job_id = cj.rq_job_id

    if _queue_exists(job_id):
        _send_control(job_id, "suspend")
        cj.status = "suspended"
        cj.save(update_fields=["status"])
        return True

    if cj.machine == socket.gethostname() and cj.process_id:
        try:
            os.kill(cj.process_id, signal.SIGSTOP)
            cj.status = "suspended"
            cj.save(update_fields=["status"])
            return True
        except ProcessLookupError:
            return False

    return False


# ------------------------
# RESUME CRAWL
# ------------------------
def resume_crawl(identifier: str) -> bool:
    cj = resolve_job_or_website(identifier)
    job_id = cj.rq_job_id

    if _queue_exists(job_id):
        _send_control(job_id, "resume")
        cj.status = "running"
        cj.save(update_fields=["status"])
        return True

    if cj.machine == socket.gethostname() and cj.process_id:
        try:
            os.kill(cj.process_id, signal.SIGCONT)
            cj.status = "running"
            cj.save(update_fields=["status"])
            return True
        except ProcessLookupError:
            return False

    return False


# ------------------------
# PLATFORM ADMINISTRATION (QUEUE: management)
# ------------------------

def admin_platform_lock(task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(admin_platform_lock_task, task_uid=task_uid)
    return job.id


def admin_platform_unlock(task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(admin_platform_unlock_task, task_uid=task_uid)
    return job.id


# ------------------------
# GLOBAL CRAWL MANAGEMENT (QUEUE: management)
# ------------------------
def crawl_throttle() -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(crawl_throttle_task)
    return job.id


def crawl_unthrottle() -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(crawl_unthrottle_task)
    return job.id


# ------------------------
# WEBSITE OPERATIONS (QUEUE: management)
# ------------------------
def website_publish_all(website_id: int, task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(website_publish_all_task, website_id=website_id, task_uid=task_uid)
    return job.id


def website_unpublish_all(website_id: int, task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(website_unpublish_all_task, website_id=website_id, task_uid=task_uid)
    return job.id


# ------------------------
# WEBSITE GROUP OPERATIONS (QUEUE: management)
# ------------------------
def website_group_set_schedule(group_id: int, schedule_id: int) -> str:
    pass

def website_group_set_crawl_config(group_id: int, crawl_config_id: int) -> str:
    pass

def website_group_priority_crawl(group_id: int) -> str:
    pass

# ------------------------
# REPLAY OPERATIONS (QUEUE: management)
# ------------------------
def replay_publish(snapshot_uid: int, task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(replay_publish_task, snapshot_uid=snapshot_uid, task_uid=task_uid)
    return job.id

def replay_unpublish(snapshot_uid: int, task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(replay_unpublish_task, snapshot_uid=snapshot_uid, task_uid=task_uid)
    return job.id


def replay_repopulate(website_id: Optional[int] = None, task_uid: str = None) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(repopulate_snapshot_to_production_task, website_id=website_id,
                        task_uid=task_uid)
    return job.id

# ------------------------
# EXPORT OPERATIONS (QUEUE: management)
# ------------------------

def export_zosia(
    website_id: Optional[int] = None,
    schedule: Optional[str] = None
) -> str:
    queue = django_rq.get_queue("management")
    job = queue.enqueue(
        export_zosia_task,
        website_id=website_id,
        schedule=schedule
    )
    return job.id
