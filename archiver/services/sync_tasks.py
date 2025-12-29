import requests
from django.conf import settings
from django.db import transaction
from django.utils.dateparse import parse_datetime

from archiver.models import Task, Website, Snapshot, WebsiteCrawlParameters, TaskStatus
from archiver.auth import get_keycloak_access_token
from archiver.services.crawl_manager import queue_crawl, replay_publish, replay_unpublish, website_publish_all, website_unpublish_all


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

        # Objects which can be set up from task.parameters
        - crawlConfig + id - OK
        - website + id - OK
        - schedule + id - TODO
    """
    ACTION_HANDLERS = {
        # Platform
        "admin_platform_lock": handle_admin_platform_lock,
        "admin_platform_unlock": handle_admin_platform_unlock,

        # Crawl management
        "crawl_throttle": handle_crawl_throttle,
        "crawl_unthrottle": handle_crawl_unthrottle,
        "crawl_run": handle_crawl_run,
        "crawl_suspend": handle_crawl_suspend,
        "crawl_stop": handle_crawl_stop,

        # Website
        "website_publish_all": handle_website_publish_all,
        "website_unpublish_all": handle_website_unpublish_all,

        # Website group
        "website_group_set_schedule": handle_website_group_set_schedule,
        "website_group_set_crawl_config": handle_website_group_set_crawl_config,
        "website_group_priority_crawl": handle_website_group_priority_crawl,

        # Replay
        "replay_publish": handle_replay_publish,
        "replay_unpublish": handle_replay_unpublish,
        "replay_repopulate": handle_replay_repopulate,

        # Export
        "export_zosia": handle_export_zosia,
    }

    try:
        ACTION_HANDLERS[task.action](task)
        # except KeyError:
        #    raise ValueError(f"Unsupported task action: {task.action}")
        # TODO: rethink if this should be so generic
        # except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
    except (Exception) as e:
        task.status = TaskStatus.FAILED
        task.updateMessage = str(e)
        task.save()
        task.send_task_response()


# =================================================
# Platform administration
# =================================================
def handle_admin_platform_lock(task):
    """Force scheduled maintenance landing page."""
    pass


def handle_admin_platform_unlock(task):
    """Disable maintenance landing page."""
    pass


# =================================================
# Crawl management
# =================================================
def handle_crawl_throttle(task):
    """Disable crawl scheduling; allow running crawls to finish."""
    pass


def handle_crawl_unthrottle(task):
    """Re-enable crawl scheduling."""
    pass


def handle_crawl_run(task):
    """Start crawl for a specific website."""
    params = task.taskParameters
    website = Website.create_from_task_parameters(params)
    if params.get("crawlConfig", None):
        wp, _ = WebsiteCrawlParameters.create_or_update_from_task_parameters(params['crawlConfig'])
    else:
        task.status = TaskStatus.FAILED
        task.updateMessage = "Missing crawlConfig. Task fails"
        task.save()
        task.send_task_response()
        return
    website.website_crawl_parameters = wp
    task.update_task_params({'crawlConfigId': wp.id,
                             'crawlConfig': wp.build_json_response()})
    task.save()
    queue_crawl(website.id, task)


def handle_crawl_suspend(task):
    """Suspend crawl execution for a website or snapshot."""
    pass


def handle_crawl_stop(task):
    """Force-stop an ongoing crawl."""
    pass


# =================================================
# Website operations
# =================================================
def handle_website_publish_all(task):
    """Publish all snapshots of a website and enable auto-publish."""
    website_id = task.taskParameters['websiteId']
    website_publish_all(website_id, task.uid)


def handle_website_unpublish_all(task):
    """Unpublish all snapshots of a website and disable auto-publish."""
    pass


# =================================================
# Website group operations
# =================================================
def handle_website_group_set_schedule(task):
    """Apply schedule config to all websites in a group."""
    pass

def handle_website_group_set_crawl_config(task):
    """Apply crawl config to all websites in a group."""
    pass

def handle_website_group_priority_crawl(task):
    """Mark all websites in a group for priority crawl."""
    pass


# =================================================
# Replay operations
# =================================================
def handle_replay_publish(task):
    """Publish a single replay/WARC."""
    params = task.taskParameters
    replay_publish(params['snapshot']['uid'], task.uid)


def handle_replay_unpublish(task):
    """Unpublish a single replay/WARC."""
    params = task.taskParameters
    replay_unpublish(params['snapshot']['uid'], task.uid)


def handle_replay_repopulate(task):
    """Recreate replay collections in batch."""
    pass


# =================================================
# Export
# =================================================
def handle_export_zosia(task):
    """Trigger or schedule ZoSIA export."""
    pass



def fetch_tasks_from_api(start=0, limit=50, where_status=None):
    """
    Fetch a single page of tasks from FRONTEND_API_URL.
    """
    token = get_keycloak_access_token()

    params = {
        "start": start,
        "limit": limit,
    }

    if where_status:
        params["whereStatus"] = "|".join(where_status)

    response = requests.get(
        f'{settings.FRONTEND_API_URL}/task/api',
        params=params,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        timeout=getattr(settings, "API_RESPONSE_TIMEOUT", 10),
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

    taskParameters = task_data.get('taskParameters', {})
    snapshot = taskParameters.get('snapshot', None)
    if snapshot:
        s = Snapshot.objects.filter(uid=snapshot.get('uid')).first()
        fields['snapshot_id']=s.id if s else None
    # TODO: snapshot i website do defaults z id?
    task, _created = Task.objects.get_or_create(
        uid=uid,
        defaults=fields,
    )

    return (task, _created)


def sync_tasks_from_cluster(where_status: list | None = None, page_limit=50, dry_run=False,
                            only_sync=False) -> int:
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
                if created and not only_sync:
                    dispatch_task(task)

        if len(items) < page_limit:
            break

        start += page_limit

    return processed


def sync_tasks_status_from_cluster(where_status: list | None = None, page_limit=50, dry_run=False,
                            only_sync=False) -> int:
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
                uid = task_data.get('uid')
                task = Task.objects.get(uid=uid)
                if task:
                    task.send_task_response()

        if len(items) < page_limit:
            break

        start += page_limit

    return processed


def sync_tasks_scheduler() -> int:
    return sync_tasks_from_cluster(["scheduled"])
