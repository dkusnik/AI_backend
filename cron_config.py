from rq import cron
from archiver.services.sync_tasks import sync_tasks_scheduler


# def sync_tasks_from_cluster(where_status=None, page_limit=50, dry_run=False) -> int:
#cron.register(sync_tasks_scheduler, queue_name='default', cron='*/5 * * * *')  # Daily at 9:00 AM
cron.register(sync_tasks_scheduler, queue_name='task_sync', interval=15)  # Every 30 seconds
