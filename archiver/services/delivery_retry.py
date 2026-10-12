"""
Outbox-style retry of failed Task notifications.

Task.send_task_response() makes a single attempt and records it in
TaskResponseDelivery. Nothing re-sent failed attempts before, so a short
frontend / Keycloak outage meant the frontend never learned about the final
state of a task. This job (registered in cron_config.py) re-sends the CURRENT
state of every task whose latest delivery attempt failed.
"""
import logging
from datetime import timedelta

from django.utils import timezone

from archiver.models import Task, TaskResponseDelivery

logger = logging.getLogger(__name__)

LOOKBACK = timedelta(hours=24)          # ignore deliveries older than this
MIN_RETRY_INTERVAL = timedelta(seconds=60)
MAX_ATTEMPTS = 20                       # failed attempts since last success


def retry_failed_task_deliveries(limit: int = 100) -> int:
    """Return number of tasks whose state was re-sent."""
    now = timezone.now()

    task_ids = list(
        TaskResponseDelivery.objects
        .filter(success=False, created_at__gte=now - LOOKBACK)
        .order_by("task_id")            # clear Meta.ordering, otherwise DISTINCT breaks
        .values_list("task_id", flat=True)
        .distinct()[:limit]
    )

    resent = 0
    for task in Task.objects.filter(id__in=task_ids):
        last = task.response_deliveries.first()      # newest (Meta.ordering = -created_at)
        if last is None or last.success:
            continue                                  # frontend already has the latest state
        if now - last.created_at < MIN_RETRY_INTERVAL:
            continue                                  # back off

        failed = task.response_deliveries.filter(success=False)
        last_ok = task.response_deliveries.filter(success=True).first()
        if last_ok:
            failed = failed.filter(created_at__gt=last_ok.created_at)
        if failed.count() >= MAX_ATTEMPTS:
            continue                                  # give up, see admin / TaskResponseDelivery

        task.send_task_response()
        resent += 1

    if resent:
        logger.info("Re-sent %d task notification(s)", resent)
    return resent
