import django_rq
import hashlib
import functools
import traceback
from django.utils import timezone

from archiver.models import Task, TaskStatus


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


def task_notify(func):
    """
    Decorator for task execution lifecycle:
    - sets RUNNING + startTime
    - executes task logic
    - sets SUCCESS or FAILED
    - sets finishTime
    - sends task responses
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        task_uid = kwargs.get("task_uid")

        if not task_uid:
            print("NE MA task_uid")
            return func(*args, **kwargs)

        print("jest task_uid")

        task = Task.objects.get(uid=task_uid)

        # -------------------------
        # Task start
        # -------------------------
        task.status = TaskStatus.RUNNING
        task.startTime = timezone.now()
        task.save(update_fields=["status", "startTime"])
        task.send_task_response()

        try:
            # -------------------------
            # Execute wrapped function
            # -------------------------
            result = func(*args, **kwargs)

            task.status = TaskStatus.SUCCESS
            task.taskResponse = {'updatedAt': str(timezone.now())}
            if task.snapshot:
                task.taskResponse['snapshotID'] = str(task.snapshot.uid)
            return result

        except Exception:
            task.status = TaskStatus.FAILED
            task.taskResponse = {'updatedAt': str(timezone.now())}
            task.updateMessage = traceback.format_exc()
            raise

        finally:
            # -------------------------
            # Task finish (always)
            # -------------------------
            task.finishTime = timezone.now()
            task.save(
                update_fields=["status", "taskResponse", "finishTime", "updateMessage"]
            )
            task.send_task_response()

    return wrapper

def is_platform_locked() -> bool:
    redis = django_rq.get_connection("management")
    return redis.exists(PLATFORM_LOCK_KEY) == 1


def is_adding_new_tasks_disabled() -> bool:
    redis = django_rq.get_connection("management")
    return redis.exists(settings.PLATFORM_DISABLE_ADDING_NEW_TASKS) == 1