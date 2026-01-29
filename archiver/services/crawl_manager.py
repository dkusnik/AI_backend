import re
from typing import Optional

import django_rq
from django.conf import settings
from rq.job import Job

from archiver.models import Snapshot, Task, TaskStatus, Website
from archiver.tasks import (export_zosia_task, replay_publish_task,
                            replay_unpublish_task,
                            repopulate_snapshot_to_production_task,
                            start_crawl_task, website_publish_all_task,
                            website_unpublish_all_task)

UUID_REGEX = re.compile(r"^[0-9a-fA-F-]{32,36}$")
redis_conn = django_rq.get_connection("management")


# ------------------------
# INTERNAL HELPERS
# ------------------------
def _queue_exists(job_id: str) -> bool:
    return redis_conn.exists(f"crawl:{job_id}") == 1


def _send_control(job_id: str, command: str):
    redis_conn.set(f"crawl:{job_id}:control", command)


def _send_crawl_control(snapshot, command: str) -> bool:
    """
    Send control command to running crawl via Redis.
    """
    if not snapshot or not snapshot.rq_job_id:
        return False

    job_id = snapshot.rq_job_id
    control_key = f"crawl:{job_id}:control"
    redis_conn.set(control_key, command)
    return True


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
    except Snapshot.DoesNotExist:
        pass

    # ----------------------------------------------------------
    # 4. Try as website-name
    # ----------------------------------------------------------
    try:
        website = Website.objects.get(name=identifier)
        return Snapshot.objects.filter(website=website).latest("id")
    except (Snapshot.DoesNotExist, Website.DoesNotExist):
        pass

    # ----------------------------------------------------------
    # 5. No match → error
    # ----------------------------------------------------------
    raise Snapshot.DoesNotExist(f"No Snapshot found for identifier={identifier}")


def queue_crawl(website_id: int, task: Task = None, queue_name: str = "crawls_normal") -> str:
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
    if task.priority:
        queue_name = f"crawls_{task.priority}"
    task.status = TaskStatus.CREATED
    task.save()

    # Enqueue the RQ job
    queue = django_rq.get_queue(queue_name)
    job = queue.enqueue(start_crawl_task, snapshot_uid=snapshot.uid, task_uid=task.uid)

    # Attach the RQ job ID and save
    snapshot.rq_job_id = job.id
    snapshot.publicationJustification = task.taskParameters.get('publicationJustification')
    snapshot.crawlJustification = task.taskParameters.get('crawlJustification')
    snapshot.save(update_fields=["rq_job_id", "publicationJustification", "crawlJustification"])
    snapshot.send_create_response()

    return job.id


def get_crawl_status(job_id: str, queue_name: str = "crawls_normal") -> dict:
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


def suspend_crawl(task: Task) -> bool:
    ok = _send_crawl_control(task.snapshot, "suspend")
    if not ok:
        return task.send_quick_status_message(TaskStatus.FAILED,
                                              "Could not send crawl control. Is crawl running?")
    return task.send_quick_status_message(TaskStatus.SUCCESS,
                                          "SUSPEND crawl control sent.")


def stop_crawl(task: Task) -> bool:
    ok = _send_crawl_control(task.snapshot, "stop")
    if not ok:
        return (TaskStatus.FAILED, "Could not force finish crawl. Is crawl running?")
    return (TaskStatus.SUCCESS, "Crawl force finish control sent.")


def cancel_crawl(task: Task) -> bool:
    ok = _send_crawl_control(task.snapshot, "cancel")
    if ok:
        task.send_quick_status_message(TaskStatus.CANCELLED,
                                       "Task CANCELLED by command control sent.")
        return (TaskStatus.SUCCESS, "Crawl Cancelled.")

    queue_name = f"crawls_{task.priority}" if task.priority else "crawls_normal"
    queue = django_rq.get_queue(queue_name)

    if task.snapshot.rq_job_id:
        try:
            job = Job.fetch(task.snapshot.rq_job_id, connection=queue.connection)

            status = job.get_status()

            if status in ("queued", "deferred"):
                job.cancel()
                job.delete(remove_from_queue=True)
                state = (TaskStatus.SUCCESS, "Crawl Cancelled. Removed from queue")

        except Exception:
            state = (TaskStatus.FAILED, "Crawl is not in the queue yet.")

    return state


def resume_crawl(task: Task) -> bool:
    ok = _send_crawl_control(task.snapshot, "stop")
    if not ok:
        return task.send_quick_status_message(TaskStatus.FAILED,
                                              "Could not send crawl control. Is crawl running?")
    return task.send_quick_status_message(TaskStatus.SUCCESS,
                                          "RESUME crawl control sent.")


# ------------------------
# PLATFORM ADMINISTRATION (QUEUE: management)
# ------------------------
def admin_platform_lock(task: Task):

    redis_conn.set(
        settings.PLATFORM_LOCK_KEY,
        "1",
        ex=None  # no expiration → manual unlock required
    )
    paused_queues = []

    for queue_name in settings.RQ_QUEUES.keys():
        if queue_name in (settings.EXCLUDED_LOCK_QUEUES):
            continue

        queue = django_rq.get_queue(queue_name)

        if not queue.is_paused:
            queue.pause()
            paused_queues.append(queue_name)
    return task.send_quick_status_message(TaskStatus.SUCCESS,
                                          "Platform LOCKED QUEUES: " + ','.join(paused_queues))


def admin_platform_unlock(task: Task):
    """Disable maintenance lock."""
    redis_conn.delete(settings.PLATFORM_LOCK_KEY)
    resumed_queues = []

    for queue_name in settings.RQ_QUEUES.keys():
        if queue_name in (settings.EXCLUDED_LOCK_QUEUES):
            continue

        queue = django_rq.get_queue(queue_name)

        if queue.is_paused:
            queue.resume()
            resumed_queues.append(queue_name)
    return task.send_quick_status_message(TaskStatus.SUCCESS,
                                          "Platform UNLOCKED QUEUES: " + ','.join(resumed_queues))


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
