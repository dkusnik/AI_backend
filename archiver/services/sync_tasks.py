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

import requests
from django.conf import settings
from django.db import transaction
from django.utils.dateparse import parse_datetime

from archiver.models import Task
from archiver.auth import get_keycloak_access_token


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


def upsert_task_from_api(task_data: dict) -> Task | None:
    """
    Create a Task only if UID does not already exist.
    If UID exists, do nothing and return None.
    """
    uid = task_data["uid"]

    # Fast existence check (cheap)
    if Task.objects.filter(uid=uid).exists():
        return None

    fields = {}

    for api_field, model_field in API_TO_MODEL_FIELD_MAP.items():
        value = task_data.get(api_field)

        # Parse ISO timestamps safely
        if model_field.endswith("Time") and value:
            value = parse_datetime(value)

        fields[model_field] = value

    # Ensure uid is set explicitly
    fields["uid"] = uid

    with transaction.atomic():
        task = Task.objects.create(**fields)

    return task


def sync_tasks_from_cluster(where_status=None, page_limit=50, dry_run=False) -> int:
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

        items = data.get("items") or data.get("results") or data
        if not items:
            break

        for task_data in items:
            processed += 1
            if not dry_run:
                upsert_task_from_api(task_data)

        if len(items) < page_limit:
            break

        start += page_limit

    return processed
