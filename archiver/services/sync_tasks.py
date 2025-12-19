import requests
from django.conf import settings
from django.db import transaction
from django.utils.dateparse import parse_datetime

from archiver.models import Task, Website, Snapshot
from archiver.auth import get_keycloak_access_token
from archiver.services.crawl_manager import queue_crawl


API_TO_MODEL_FIELD_MAP = {
    "action": "action",
    "uid": "uid",
    "user": "user",
    "scheduleTime": "scheduleTime",
    "startTime": "startTime",
    "updateTime": "updateTime",
    "updateMessage": "updateMessage",
    "finishTime": "finishTime",
    "status": "status",
    "result": "result",
    "resultDescription": "resultDescription",
    "runData": "runData",
    "priority": "priority",
    "schedule": "schedule",
    "taskParameters": "taskParameters",
    "taskResponse": "taskResponse",
}


def dispatch_task(task: Task) -> None:
    """
            Defined task actions:

        Platform administration:

            admin_platform_lock / admin_platform_unlock - force scheduled maintenance landing page

        Crawl management:

            crawl_throttle / crawl_unthrottle - disable crawl scheduling, allow completing ongoing crawls
            crawl_run / crawl_suspend / crawl_stop - actions on specific websites/crawls

        Website operations:

            website_publish_all - publish all snapshots of a website and set autoPublish=true
            website_unpublish_all - unpublish all snapshots of a website and set autoPublish=false

        Website group operations:

            website_group_set_schedule - apply schedule config to all websites in group
            website_group_set_crawl_config - apply crawl config to all websites in group
            website_group_priority_crawl - mark all websites in group for priority crawl

        Replay operations:

            replay_publish / replay_unpublish - operations on single WARC
            replay_repopulate - batch recreation of replay collections

        Export:

            export_zosia - trigger / define scheduled ZoSIA export
    """
    if task.action == "crawl_run":
        website = Website.create_from_task_parameters(task.taskParameters)
        queue_crawl(website.id, task)


def fetch_tasks_from_api(start=0, limit=50, where_status=None):
    """
    Fetch a single page of tasks from TASK_RESPONSE_URL.
    """
    token = get_keycloak_access_token()

    params = {
        "start": start,
        "limit": limit,
    }

    if where_status:
        params["whereStatus"] = "|".join(where_status)

    response = requests.get(
        settings.TASK_RESPONSE_URL,
        params=params,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        timeout=10,
    )

    response.raise_for_status()
    return response.json()


def upsert_task_from_api(task_data: dict) -> tuple[Task, bool]:
    """
    Get existing Task by UID or create it if missing.
    Always returns a Task instance.
    """

    print(task_data)
    uid = task_data["uid"]

    fields = {}

    for api_field, model_field in API_TO_MODEL_FIELD_MAP.items():
        value = task_data.get(api_field)

        # Parse ISO timestamps safely
        if model_field.endswith("Time") and value:
            value = parse_datetime(value)

        fields[model_field] = value

    # uid must not be duplicated in defaults
    fields.pop("uid", None)

    task, _created = Task.objects.get_or_create(
        uid=uid,
        defaults=fields,
    )

    return (task, _created)


def sync_tasks_from_cluster(where_status: list | None = None, page_limit=50, dry_run=False) -> int:
    """
    Fetch all tasks from Cluster API and sync locally.

    :return: number of tasks processed
    """
    start = 0
    processed = 0

    while True:
        data = fetch_tasks_from_api(
            start=start,
            limit=page_limit,
            where_status=where_status,
        )
        items = data.get("items")
        if not items:
            break

        for task_data in items:
            processed += 1
            if not dry_run:
                task, created = upsert_task_from_api(task_data)
                if created:
                    dispatch_task(task)

        if len(items) < page_limit:
            break

        start += page_limit

    return processed

def sync_tasks_scheduler() -> int:
    return sync_tasks_from_cluster(["scheduled"])
