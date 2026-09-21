import os

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from archiver.models import Snapshot

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class CrawlRun:
    log_file: Path
    start_ts: datetime
    end_ts: datetime | None = None


COLLECTION_PREFIXES = (
    "domain_",
    "host_",
    "prefix_",
)


def collection_name_to_domain(collection_name: str) -> str:
    name = collection_name

    for prefix in COLLECTION_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    return name.replace("_", ".")


class Command(BaseCommand):
    help = (
        "Remove snapshots whose long-term storage directory does not exist "
        "(orphaned snapshots)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--collection-dir",
            required=True,
            help=(
                "Path to Browsertrix collection directory "
                "(must contain archive/, logs/, indexes/)"
            ),
        )

        parser.add_argument(
            "--website-id",
            type=int,
            help="Website primary key",
        )

        parser.add_argument(
            "--website-url",
            help="Website URL",
        )

        parser.add_argument(
            "--website-name",
            help="Website name",
        )

        parser.add_argument(
            "--collections-dir",
            required=True,
            help=(
                "Directory containing Browsertrix "
                "collection directories"
            ),
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Only show crawl -> warc mapping, "
                "do not create snapshots"
            ),
        )
    LOG_RE = re.compile(r"crawl-(\d{17})\.log$")
    WARC_RE = re.compile(
        r"rec-(\d{17,20})-[^.]+\.warc(?:\.gz)?$"
    )

    def parse_log_timestamp(log_path: Path) -> datetime:
        match = LOG_RE.match(log_path.name)

        if not match:
            raise ValueError(
                f"Invalid crawl log filename: {log_path.name}"
            )

        return datetime.strptime(
            match.group(1)[:14],
            "%Y%m%d%H%M%S",
        )

    def parse_warc_timestamp(warc_path: Path) -> datetime:
        match = WARC_RE.match(warc_path.name)

        if not match:
            raise ValueError(
                f"Invalid warc filename: {warc_path.name}"
            )

        return datetime.strptime(
            match.group(1)[:14],
            "%Y%m%d%H%M%S",
        )

    def discover_crawls(collection_dir: Path) -> list[CrawlRun]:
        logs_dir = collection_dir / "logs"

        runs = []

        for log_file in sorted(logs_dir.glob("crawl-*.log")):
            runs.append(
                CrawlRun(
                    log_file=log_file,
                    start_ts=parse_log_timestamp(log_file),
                )
            )

        runs.sort(key=lambda x: x.start_ts)

        for idx in range(len(runs) - 1):
            runs[idx].end_ts = runs[idx + 1].start_ts

        return runs


    @transaction.atomic
    def create_snapshot(
            website,
            collection_dir: Path,
            crawl: CrawlRun,
            snapshot_warcs: list[Path],
    ):
        if not snapshot_warcs:
            return None

        start_ts = min(
            parse_warc_timestamp(w)
            for w in snapshot_warcs
        )

        stop_ts = max(
            parse_warc_timestamp(w)
            for w in snapshot_warcs
        )

        total_size = sum(
            w.stat().st_size
            for w in snapshot_warcs
        )

        snapshot = Snapshot.objects.create(
            website=website,
            status=Snapshot.STATUS_COMPLETED,
            publication_status=Snapshot.PUBLICATION_INTERNAL,
            crawlStartTimestamp=start_ts,
            crawlStopTimestamp=stop_ts,
            replay_collection_id=(
                f"legacy-{start_ts:%Y%m%d%H%M%S}"
            ),
            warc_path=str(
                collection_dir / "archive"
            ),
            size=total_size,
            crawlWarcSize=total_size,
            item_count=len(snapshot_warcs),
        )

        for warc_file in snapshot_warcs:
            Warc.objects.create(
                snapshot=snapshot,
                filename=warc_file.name,
                path=str(warc_file),
                size_bytes=warc_file.stat().st_size,
                sha256="",
                is_production=True,
            )

        stats, derived = calculate_snapshot_stats(
            collection_dir=collection_dir,
            crawl=crawl,
            snapshot_warcs=snapshot_warcs,
        )

        snapshot.update_snapshot_stats(
            stats,
            derived,
        )

        snapshot.save()

        return snapshot

    from tempfile import TemporaryDirectory

    import json

    def build_stats_from_filtered_cdxj(
            cdxj_file: Path,
            snapshot_warcs: list[Path],
            stats,
    ):
        allowed_warcs = {
            w.name
            for w in snapshot_warcs
        }

        with open(
                cdxj_file,
                "r",
                encoding="utf-8",
                errors="ignore",
        ) as handle:

            for line in handle:

                json_start = line.find("{")

                if json_start < 0:
                    continue

                try:
                    record = json.loads(
                        line[json_start:]
                    )
                except Exception:
                    continue

                filename = record.get("filename")

                if filename not in allowed_warcs:
                    continue

                stats.cdx_lines_parsed += 1
                stats.cdx_bytes_parsed += len(
                    line.encode("utf-8")
                )

                CDXParser._process_cdx_record(
                    record,
                    stats,
                )

        return stats

    def assign_warcs_to_crawls(
            collection_dir: Path,
            crawls: list[CrawlRun],
    ):
        archive_dir = collection_dir / "archive"

        warcs = sorted(
            archive_dir.glob("*.warc.gz"),
            key=parse_warc_timestamp,
        )

        mapping = {}

        for crawl in crawls:

            assigned = []

            for warc in warcs:
                warc_ts = parse_warc_timestamp(warc)

                if crawl.end_ts:
                    if crawl.start_ts <= warc_ts < crawl.end_ts:
                        assigned.append(warc)
                else:
                    if warc_ts >= crawl.start_ts:
                        assigned.append(warc)

            mapping[crawl] = assigned

        return mapping


    def calculate_snapshot_stats(
            collection_dir: Path,
            crawl: CrawlRun,
            snapshot_warcs: list[Path],
    ):
        stats = CrawlStats()

        with TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)

            logs_dir = tmp_dir / "logs"
            logs_dir.mkdir()

            tmp_log = logs_dir / crawl.log_file.name
            tmp_log.write_bytes(
                crawl.log_file.read_bytes()
            )

            stats = BrowsertrixLogParser(
                logs_dir
            ).parse(stats)

            stats = build_stats_from_filtered_cdxj(
                collection_dir / "indexes" / "index.cdxj",
                snapshot_warcs,
                stats,
            )

        derived = CrawlDerivedMetrics()

        return stats, derived
    def import_browsertrix_collection(
            website,
            collection_dir: str,
    ):
        collection_dir = Path(collection_dir)

        crawls = discover_crawls(
            collection_dir
        )

        mapping = assign_warcs_to_crawls(
            collection_dir,
            crawls,
        )

        imported = []

        for crawl, snapshot_warcs in mapping.items():

            snapshot = create_snapshot(
                website=website,
                collection_dir=collection_dir,
                crawl=crawl,
                snapshot_warcs=snapshot_warcs,
            )

            if snapshot:
                imported.append(snapshot)

        return imported

    def resolve_website_for_collection(collection_dir):
        collection_name = collection_dir.name

        domain = collection_name_to_domain(
            collection_name
        )

        candidates = Website.objects.filter(
            Q(url__icontains=domain)
            | Q(name__icontains=domain)
        )

        if candidates.count() == 1:
            return candidates.first()

        exact_matches = [
            w
            for w in candidates
            if domain in (w.url or "")
        ]

        if len(exact_matches) == 1:
            return exact_matches[0]

        return None

    def resolve_website(options):
        provided = [
            bool(options.get("website_id")),
            bool(options.get("website_url")),
            bool(options.get("website_name")),
        ]

        if sum(provided) != 1:
            raise CommandError(
                "Provide exactly one of: "
                "--website-id, "
                "--website-url, "
                "--website-name"
            )

        if options.get("website_id"):
            return Website.objects.get(
                pk=options["website_id"]
            )

        if options.get("website_url"):
            return Website.objects.get(
                url=options["website_url"]
            )

        return Website.objects.get(
            name=options["website_name"]
        )

    def handle(self, *args, **options):
        website = resolve_website(options)

        collection_dir = Path(
            options["collection_dir"]
        )

        crawls = discover_crawls(
            collection_dir
        )

        mapping = assign_warcs_to_crawls(
            collection_dir,
            crawls,
        )

        if options["dry_run"]:

            self.stdout.write("")
            self.stdout.write(
                self.style.WARNING(
                    "DRY RUN MODE"
                )
            )

            for crawl, warcs in mapping.items():

                size_mb = (
                        sum(
                            w.stat().st_size
                            for w in warcs
                        )
                        / 1024
                        / 1024
                )

                self.stdout.write(
                    f"{crawl.log_file.name}"
                )

                self.stdout.write(
                    f"  warcs: {len(warcs)}"
                )

                self.stdout.write(
                    f"  size: {size_mb:.2f} MB"
                )

                if warcs:
                    self.stdout.write(
                        f"  first: {warcs[0].name}"
                    )
                    self.stdout.write(
                        f"  last : {warcs[-1].name}"
                    )

                self.stdout.write("")

            return

        snapshots = import_browsertrix_collection(
            website=website,
            collection_dir=options["collection_dir"],
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Imported {len(snapshots)} snapshots"
            )
        )