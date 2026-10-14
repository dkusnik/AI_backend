import django_rq
import hashlib
import functools
import logging
import traceback
from django.db import close_old_connections
from django.utils import timezone
from django.conf import settings

from archiver.models import Task, TaskStatus

logger = logging.getLogger(__name__)

# Once a task reaches one of these states nobody may silently overwrite it.
TERMINAL_TASK_STATUSES = frozenset({
    TaskStatus.SUCCESS,
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
    TaskStatus.STOPPED,
})


def parse_pipe_array(request, name):
    v = request.query_params.get(name)
    if not v:
        return None
    return [x for x in v.split("|") if x != ""]


def calculate_sha256(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    """
    Calculate SHA-256 checksum of a file.

    :param file_path: Path to file
    :param chunk_size: Read size in bytes (default: 1 MB)
    :return: Hex-encoded SHA-256 hash
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def _finalize_task(task: Task, outcome: str, error_message: str | None) -> None:
    """
    Persist the final state of `task` and notify the frontend.

    Order matters: the in-memory object is updated first, persistence is best
    effort and the notification is ALWAYS attempted, even when the DB is down.
    """
    # A crawl can run for hours; the DB connection may have died meanwhile.
    try:
        close_old_connections()
    except Exception:
        logger.exception("close_old_connections failed for task %s", task.uid)

    # The wrapped function works on its OWN Task instance and may already have
    # stored a terminal state (FAILED / STOPPED / CANCELLED ...). Reload it so
    # that we do not blindly overwrite it with SUCCESS.
    try:
        task.refresh_from_db()
    except Exception:
        logger.exception("refresh_from_db failed for task %s", task.uid)

    if outcome == TaskStatus.FAILED or task.status not in TERMINAL_TASK_STATUSES:
        task.status = outcome

    response = dict(task.taskResponse or {})
    response["updatedAt"] = str(timezone.now())
    try:
        if task.snapshot_id:
            response["snapshotID"] = str(task.snapshot.uid)
    except Exception:
        logger.exception("Could not resolve snapshot for task %s", task.uid)
    task.taskResponse = response

    if error_message:
        task.updateMessage = error_message
    task.finishTime = task.finishTime or timezone.now()

    try:
        task.save(update_fields=["status", "taskResponse", "finishTime", "updateMessage"])
    except Exception:
        logger.exception("Could not persist final state of task %s", task.uid)

    task.send_task_response()  # never raises


def task_notify(func):
    """
    Decorator for task execution lifecycle:
    - sets RUNNING + startTime and notifies the frontend
    - executes task logic
    - sets SUCCESS or FAILED (unless the task already stored a terminal state)
    - sets finishTime
    - ALWAYS sends the final task response
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        task_uid = kwargs.get("task_uid")

        if not task_uid:
            logger.warning(
                "task_notify: %s called without task_uid kwarg - "
                "lifecycle is NOT tracked / reported", func.__name__)
            return func(*args, **kwargs)

        task = Task.objects.get(uid=task_uid)

        # -------------------------
        # Task start
        # -------------------------
        task.status = TaskStatus.RUNNING
        task.startTime = timezone.now()
        task.save(update_fields=["status", "startTime"])
        task.send_task_response()  # never raises

        outcome = TaskStatus.FAILED   # pessimistic default: covers BaseException too
        error_message = None
        try:
            result = func(*args, **kwargs)
            outcome = TaskStatus.SUCCESS
            return result
        except BaseException:
            error_message = traceback.format_exc()
            raise
        finally:
            _finalize_task(task, outcome, error_message)

    return wrapper


def is_platform_locked() -> bool:
    redis = django_rq.get_connection("management")
    return redis.exists(settings.PLATFORM_LOCK_KEY) == 1


def is_adding_new_tasks_disabled() -> bool:
    redis = django_rq.get_connection("management")
    return redis.exists(settings.PLATFORM_DISABLE_ADDING_NEW_TASKS) == 1
