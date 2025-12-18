# archiver/management/commands/register_rq_cronjobs.py

from django.core.management.base import BaseCommand
from django_rq import get_scheduler
from archiver.services.sync_tasks import sync_tasks_from_cluster

# def sync_tasks_from_cluster(where_status=None, page_limit=50, dry_run=False) -> int:

CRON_JOBS = [
    {
        "cron": "*/5 * * * *",
        "func": sync_tasks_from_cluster,
        "queue": "task_sync",
        "id": "poll_remote_tasks_5min",
        "kwargs": {"where_status": "scheduled"},
    },
]


class Command(BaseCommand):
    help = "Register RQ cron jobs (idempotent)"

    def handle(self, *args, **options):
        scheduler = get_scheduler("default")

        existing = {
            job.id: job
            for job in scheduler.get_jobs()
        }

        for job in CRON_JOBS:
            if job["id"] in existing:
                self.stdout.write(
                    f"Cron job already exists: {job['id']}"
                )
                continue

            scheduler.cron(
                job["cron"],
                job["func"],
                queue_name=job["queue"],
                repeat=None,
                job_id=job["id"],
                kwargs=job["kwargs"]
            )

            self.stdout.write(
                f"Registered cron job: {job['id']} ({job['cron']})"
            )
