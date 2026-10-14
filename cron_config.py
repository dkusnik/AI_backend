from rq import cron
from archiver.services.delivery_retry import retry_failed_task_deliveries
from archiver.services.sync_tasks import sync_tasks_scheduler
from archiver.services.watchdog import reap_stale_crawl_tasks


# def sync_tasks_from_cluster(where_status=None, page_limit=50, dry_run=False) -> int:
#cron.register(sync_tasks_scheduler, queue_name='default', cron='*/5 * * * *')  # Daily at 9:00 AM
cron.register(sync_tasks_scheduler, queue_name='task_sync', interval=15)  # Every 30 seconds
cron.register(retry_failed_task_deliveries, queue_name='task_sync', interval=60)  # re-send failed notifications
# 'management' worker has the Docker socket (needed to stop orphaned crawl containers)
cron.register(reap_stale_crawl_tasks, queue_name='management', interval=120)
