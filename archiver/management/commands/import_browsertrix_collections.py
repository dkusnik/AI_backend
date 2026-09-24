import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import requests
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from archiver.models import Snapshot, Website
from archiver.stats import (BrowsertrixLogParser, CDXParser,
                            CrawlDerivedMetrics, CrawlStats)
from archiver.tasks import _generate_cdx_from_warc, move_snapshot_to_longterm


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
SEEDLIST_URL = "http://10.3.82.2/seedlist"
LOG_RE = re.compile(r"crawl-(\d{17})\.log$")
WARC_RE = re.compile(
    r"rec-(\d{17,20})-[^.]+\.warc(?:\.gz)?$"
)

logger = logging.getLogger(__name__)


def normalize_url(url: str) -> str:
    url = url.strip()

    # obsługa formatu:
    # [http://archiwa.gov.pl](http://archiwa.gov.pl)
    if "](" in url:
        url = url.split("](", 1)[1].rstrip(")")

    parsed = urlparse(url)

    host = parsed.netloc.lower()

    if host.startswith("www."):
        host = host[4:]

    return host


def get_or_create_website_from_seed(seed):
    defaults = {
        "name": seed["title"],
        "displayName": seed["title"],
        "url": seed["url"],
        "enabled": True,
        "isDeleted": False,
        "doCrawl": True,
    }

    website, _created = Website.objects.update_or_create(
        id=seed["id"],
        defaults=defaults,
    )

    return website


def resolve_or_create_website(
        seed,
        website_cache,
):
    website = website_cache.get(seed["id"])

    if website:
        return website

    website = Website.objects.create(
        id=seed["id"],
        name=seed["title"],
        displayName=seed["title"],
        url=seed["url"],
        enabled=True,
        isDeleted=False,
        doCrawl=True,
    )

    website_cache[website.id] = website

    return website


def load_seedlist(seedlist_file=None):
    if seedlist_file:
        print(
            f"Loading seedlist from file: "
            f"{seedlist_file}"
        )

        with open(seedlist_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        print(
            f"Loading seedlist from: "
            f"{SEEDLIST_URL}"
        )

        response = requests.get(
            SEEDLIST_URL,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()

    mapping = {}

    for item in data:
        host = normalize_url(item["url"])
        mapping[host] = item

    return mapping


def build_website_map():
    mapping = {}

    for website in Website.objects.all():
        if not website.url:
            continue

        host = normalize_url(website.url)

        mapping[host] = website

    return mapping


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


def collection_name_to_domain(collection_name: str) -> str:
    name = collection_name

    for prefix in COLLECTION_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    return name.replace("_", ".")


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


def generate_collection_cdx(collection_dir: str | Path) -> None:
    """
    Generate warc-cdx/*.cdx files for all WARC files
    found in collection_dir/archive.

    Example:

    collection_dir/
    ├── archive/
    │   ├── rec-1.warc.gz
    │   └── rec-2.warc.gz
    └── warc-cdx/
        ├── rec-1.warc.gz.cdx
        └── rec-2.warc.gz.cdx
    """

    collection_dir = Path(collection_dir)

    archive_dir = collection_dir / "archive"
    cdx_dir = collection_dir / "warc-cdx"

    if not archive_dir.exists():
        raise FileNotFoundError(
            f"Archive directory not found: {archive_dir}"
        )

    cdx_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    warc_files = sorted(archive_dir.glob("*.warc.gz"))

    if not warc_files:
        raise FileNotFoundError(
            f"No WARC files found in {archive_dir}"
        )

    logger.info(
        "Generating CDX files for %s WARC files in %s",
        len(warc_files),
        collection_dir,
    )
    generated = 0
    skipped = 0
    for warc_file in warc_files:
        cdx_file = cdx_dir / f"{warc_file.name}.cdx"

        if cdx_file.exists() and cdx_file.stat().st_size > 0:
            logger.info(
                "Skipping existing CDX: %s",
                cdx_file.name,
            )
            skipped += 1
            continue
        logger.info(
            "Generating CDX for %s",
            warc_file.name,
        )

        _generate_cdx_from_warc(
            warc_path=str(warc_file),
            output_dir=str(cdx_dir),
        )
        generated += 1
    logger.info(
        "CDX generation completed: generated=%s skipped=%s directory=%s",
        generated,
        skipped,
        cdx_dir,
    )


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
        status=Snapshot.STATUS_PENDING,
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
    # this is needed otherwise packets are going as PUT and API returns an error
    snapshot.send_create_response()
    # for warc_file in snapshot_warcs:
    #     Warc.objects.create(
    #         snapshot=snapshot,
    #         filename=warc_file.name,
    #         path=str(warc_file),
    #         size_bytes=warc_file.stat().st_size,
    #         sha256="",
    #         is_production=True,
    #     )

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

    mapping = []

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

        mapping.append(
            (crawl, assigned)
        )

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

    for crawl, snapshot_warcs in mapping:

        existing_snapshot = (
            Snapshot.objects
            .filter(
                website=website,
                result__crawl_log=str(crawl.log_file),
            )
            .first()
        )

        if existing_snapshot:
            if existing_snapshot.status == Snapshot.STATUS_COMPLETED:
                logger.info(
                    "Skipping completed snapshot %s (crawl=%s)",
                    existing_snapshot.id,
                    crawl.log_file,
                )
                continue

            logger.info(
                "Resuming snapshot %s (crawl=%s status=%s)",
                existing_snapshot.id,
                crawl.log_file,
                existing_snapshot.status,
            )

            snapshot = existing_snapshot

        else:
            snapshot = create_snapshot(
                website=website,
                collection_dir=collection_dir,
                crawl=crawl,
                snapshot_warcs=snapshot_warcs,
            )

        if not snapshot:
            continue

        if snapshot:
            finalize_snapshot(snapshot, collection_dir)
            imported.append(snapshot)

    return imported


def finalize_snapshot(snapshot, collection_dir):
    # try:
    #     _reindex_collection(
    #         snapshot.replay_collection_id,
    #         collection_dir
    #     )
    # except Exception:
    #     pass

    Snapshot.objects.filter(pk=snapshot.pk).update(
        replay_collection_id=str(snapshot.id),
        status=Snapshot.STATUS_COMPLETED
    )

    snapshot.replay_collection_id = str(snapshot.id)

    try:
        move_snapshot_to_longterm(
            snapshot.uid,
            source_collection_dir=collection_dir,
        )

        # if snapshot.website.auto_publish:
        #     move_snapshot_to_production(
        #         snapshot.uid
        #     )

    except FileNotFoundError as e:
        logger.warning(
            "Could not move snapshot %s: %s",
            snapshot.uid,
            e,
        )


class Command(BaseCommand):
    help = (
        "Remove snapshots whose long-term storage directory does not exist "
        "(orphaned snapshots)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--collection-dir",
            required=False,
            help=(
                "Path to Browsertrix collection directory "
                "(must contain archive/, logs/, indexes/)"
            ),
        )

        parser.add_argument(
            "--seedlist-file",
            help=(
                "Local seedlist JSON file. "
                "If provided, seedlist API will not be used."
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

    def resolve_website_for_collection(
            self,
            collection_dir,
            seedlist_mapping,
            website_cache,
    ):
        domain = collection_name_to_domain(
            collection_dir.name
        ).lower()

        seed = seedlist_mapping.get(domain)

        if not seed:
            self.stdout.write(
                self.style.WARNING(
                    f"No seedlist entry found for "
                    f"{collection_dir.name} ({domain})"
                )
            )
            return None

        website = website_cache.get(
            seed["id"]
        )

        if website:
            return website

        website = Website.objects.create(
            id=seed["id"],
            name=seed["title"],
            displayName=seed["title"],
            url=seed["url"],
            enabled=True,
            isDeleted=False,
            doCrawl=True,
        )

        website_cache[website.id] = website

        self.stdout.write(
            self.style.WARNING(
                f"Created Website "
                f"id={website.id} "
                f"url={website.url}"
            )
        )

        return website

    def handle(self, *args, **options):
        collections_dir = Path(
            options["collections_dir"]
        )

        seedlist_mapping = load_seedlist(
            options.get("seedlist_file")
        )

        existing_websites = {
            website.id: website
            for website in Website.objects.all()
        }
        imported_total = 0

        for collection_dir in sorted(collections_dir.iterdir()):
            if not collection_dir.is_dir():
                continue

            website = self.resolve_website_for_collection(
                collection_dir=collection_dir,
                seedlist_mapping=seedlist_mapping,
                website_cache=existing_websites,
            )

            if not website:
                self.stdout.write(
                    self.style.WARNING(
                        f"Cannot resolve website for "
                        f"{collection_dir.name}"
                    )
                )
                continue

            self.stdout.write(
                f"Processing {collection_dir.name} "
                f"-> {website.id} ({website.url})"
            )

            crawls = discover_crawls(
                collection_dir
            )

            mapping = assign_warcs_to_crawls(
                collection_dir,
                crawls,
            )

            if options["dry_run"]:

                self.stdout.write(
                    self.style.WARNING(
                        "DRY RUN MODE"
                    )
                )

                for crawl, warcs in mapping:
                    size_mb = sum(w.stat().st_size for w in warcs) / 1024 / 1024
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
                continue

            connection.close()
            generate_collection_cdx(collection_dir)
            snapshots = import_browsertrix_collection(
                website=website,
                collection_dir=str(collection_dir),
            )

            imported_total += len(
                snapshots
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Imported {imported_total} snapshots"
            )
        )
