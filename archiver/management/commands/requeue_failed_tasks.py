from django.core.management.base import BaseCommand
from django.db import transaction

from archiver.models import Task, TaskStatus
from archiver.services.sync_tasks import dispatch_task


class Command(BaseCommand):
    help = "Dispatch all FAILED tasks again."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show tasks that would be dispatched without dispatching",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit number of tasks to dispatch",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        limit = options.get("limit")

        qs = Task.objects.filter(status=TaskStatus.FAILED).order_by("updateTime")

        if limit:
            qs = qs[:limit]

        total = qs.count()
        if total == 0:
            self.stdout.write(self.style.WARNING("No FAILED tasks found."))
            return

        self.stdout.write(
            self.style.SUCCESS(
                f"Found {total} FAILED task(s). Dry-run={dry_run}"
            )
        )

        dispatched = 0

        for task in qs.iterator():
            msg = f"Task uid={task.uid}"

            if dry_run:
                self.stdout.write(self.style.WARNING("[DRY-RUN] " + msg))
                continue

            try:
                with transaction.atomic():
                    dispatch_task(task)
                dispatched += 1
                self.stdout.write(self.style.SUCCESS("Dispatched " + msg))

            except Exception as exc:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to dispatch {msg}: {exc}"
                    )
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Dispatched={dispatched}, Dry-run={dry_run}"
            )
        )
