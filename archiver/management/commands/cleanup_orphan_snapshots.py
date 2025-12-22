import os

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from archiver.models import Snapshot


class Command(BaseCommand):
    help = (
        "Remove snapshots whose long-term storage directory does not exist "
        "(orphaned snapshots)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be deleted without deleting anything",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        qs = Snapshot.objects.all()
        removed = 0
        checked = 0

        for snapshot in qs.iterator():
            checked += 1

            # Build expected directory path
            base_path = os.path.join(
                settings.LONGTERM_VOLUME,
                str(snapshot.replay_collection_id),
                'archive'
            )

            if os.path.isdir(base_path):
                continue

            msg = (
                f"Snapshot id={snapshot.id}, uid={snapshot.uid} "
                f"missing directory: {base_path}"
            )

            if dry_run:
                self.stdout.write(self.style.WARNING("[DRY-RUN] " + msg))
            else:
                self.stdout.write(self.style.ERROR("Removing " + msg))

                with transaction.atomic():
                    snapshot.delete()

                removed += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Checked={checked}, Removed={removed}, Dry-run={dry_run}"
            )
        )
