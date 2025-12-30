from django.core.management.base import BaseCommand, CommandError
from archiver.services.sync_tasks import sync_tasks_from_cluster


class Command(BaseCommand):
    help = "Import and synchronize tasks from FrontEnd"

    def add_arguments(self, parser):
        parser.add_argument(
            "--status",
            nargs="*",
            help="Filter tasks by status (space-separated, e.g. scheduled running finished)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Page size when fetching tasks (default: 50)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch tasks but do not write to database",
        )

        parser.add_argument(
            "--only-sync",
            action="store_true",
            help="Fetch tasks but do not queue",
        )

        parser.add_argument(
            "--force-run",
            action="store_true",
            help="Forces the tasks to be dispatched again ( risky! use only if sure)",
        )

    def handle(self, *args, **options):
        statuses = options.get("status")
        limit = options["limit"]
        dry_run = options["dry_run"]
        only_sync = options["only_sync"]
        force_run = options["force_run"]

        self.stdout.write(self.style.NOTICE("Starting task import from Cluster API"))

        try:
            count = sync_tasks_from_cluster(
                where_status=statuses,
                page_limit=limit,
                dry_run=dry_run,
                only_sync=only_sync,
                force_run=force_run,
            )
        except Exception as exc:
            raise CommandError(f"Task import failed: {exc}") from exc

        if dry_run:
            self.stdout.write(
                self.style.WARNING(f"Dry run completed. {count} tasks fetched.")
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Task import completed. {count} tasks synchronized.")
            )
