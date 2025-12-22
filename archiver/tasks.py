import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import django_rq
import docker
import requests
from django.conf import settings
from django.utils import timezone

from archiver.models import Snapshot, Warc, Website, WebsiteGroup, Task, TaskStatus
from archiver.stats import (BrowsertrixLogParser, CDXParser,
                            CrawlMetricsCalculator, CrawlStats,
                            get_browsertrix_container_stats)

from archiver.utils import calculate_sha256


redis_conn = django_rq.get_connection("crawls")


def update_snapshot_process_stats(container, snapshot_id: int) -> dict:
    """
    Collect Docker stats for the Snapshot's container and
    persist them into Snapshot.process_stats as JSON.

    :param snapshot_id: Snapshot PK
    :return: stats dict (same as stored)
    """
    snapshot = Snapshot.objects.get(pk=snapshot_id)

    if not snapshot.process_id:
        raise ValueError("Snapshot has no associated container (process_id is empty)")

    # Collect stats from Docker
    stats = get_browsertrix_container_stats(container)

    # Optional: enrich with Snapshot-level metadata
    stats["snapshot"] = {
        "id": snapshot.id,
        "website_id": snapshot.website_id,
        "status": snapshot.status,
        "updated_at": timezone.now().isoformat(),
    }

    # Persist JSON atomically
    snapshot.process_stats = stats
    snapshot.save(update_fields=["process_stats"])

    return stats


def build_browsertrix_container_args(snapshot: Snapshot, task: Task):
    """
    Build Docker SDK args for Browsertrix crawler using --config.
    """

    config_path = task.build_browsertrix_yaml_config(
        replay_collection_id=snapshot.replay_collection_id,
    )

    command = [
        "crawl",
        "--config", f"/crawls/configs/{config_path.name}",
        "--collection", str(snapshot.replay_collection_id),
        "--generateCDX",
        # "--combineWARC" # TODO: rethink if this is needed
    ]

    return {
        "image": "webrecorder/browsertrix-crawler",

        "command": command,

        "detach": True,
        "remove": False,
        "tty": False,

        "volumes": {
            settings.BROWSERTIX_VOLUME: {
                "bind": "/crawls",
                "mode": "rw",
            }
        },

        "name": f"crawl_{snapshot.replay_collection_id}",

        "labels": {
            "app": "browsertrix",
            "snapshot_id": str(snapshot.replay_collection_id),
        },
    }


def start_crawl_task(task_uid, snapshot_id):
    task = Task.objects.get(uid=task_uid)
    snapshot = Snapshot.objects.get(id=snapshot_id)
    # -------------------------------
    # Mark job as running
    # -------------------------------
    snapshot.status = Snapshot.STATUS_PENDING

    snapshot.machine = getattr(
        settings, "CRAWL_MACHINE_NAME", "docker"
    )
    snapshot.replay_collection_id = snapshot_id
    snapshot.save(update_fields=["status", "machine"])

    # -------------------------------
    # Docker client
    # -------------------------------
    client = docker.from_env()
    container_args = build_browsertrix_container_args(
        snapshot, task
    )
    container = client.containers.run(**container_args)

    # store container id (NOT PID)
    snapshot.process_id = container.id
    task.status = TaskStatus.RUNNING
    task.save(update_fields=["status"])
    snapshot.save(update_fields=["process_id"])

    # -------------------------------
    # Redis control keys
    # -------------------------------
    job_id = snapshot.rq_job_id
    queue_key = f"crawl:{job_id}"
    control_key = f"{queue_key}:control"

    redis_conn.set(queue_key, "running")

    stats = CrawlStats()

    base_path = Path(settings.BROWSERTIX_VOLUME) / "collections" / str(snapshot_id)
    log_parser = BrowsertrixLogParser(base_path / "logs")
    cdx_parser = CDXParser(Path(base_path / "warc-cdx"))
    metrics_calc = CrawlMetricsCalculator(stall_threshold_seconds=60)

    try:
        while True:
            container.reload()
            status = container.status  # running, exited, paused
            # update stats

            # -------------------------------
            # Control commands
            # -------------------------------
            control = redis_conn.get(control_key)
            if control:
                control_cmd = control.decode()

                if control_cmd == "stop":
                    container.stop(timeout=10)
                    snapshot.status = "stopped"

                elif control_cmd == "suspend":
                    container.pause()
                    snapshot.status = "suspended"

                elif control_cmd == "resume":
                    container.unpause()
                    snapshot.status = Snapshot.STATUS_CRAWLING

                snapshot.save(update_fields=["status"])
                redis_conn.delete(control_key)

            stats = log_parser.parse(stats)
            stats = cdx_parser.parse(stats)

            # ---- compute derived metrics ----
            derived = metrics_calc.calculate(stats)

            # ---- reporting / debugging / persistence ----
            report = {
                # control-plane (logs)
                "crawled": stats.crawled,
                "total": stats.total,
                "pending": stats.pending,

                # volume (parser-level, NOT archive size)
                "log_MB_parsed": round(stats.log_bytes_parsed / 1e6, 2),
                "cdx_MB_parsed": round(stats.cdx_bytes_parsed / 1e6, 2),

                # archival truth (CDX)
                "top_mime": stats.by_mime.most_common(5),
                "http_status": dict(stats.by_http_status),

                # derived metrics
                "cdx_entries_per_sec": round(derived.cdx_entries_per_sec, 2),
                "cdx_bytes_per_sec": round(derived.cdx_bytes_per_sec, 1),

                # health
                "health": {
                    "log_stalled": derived.log_stalled,
                    "cdx_stalled": derived.cdx_stalled,
                    "js_heavy": derived.crawler_running_no_cdx,
                }
            }
            print(report)
            snapshot.update_snapshot_stats(stats, derived)
            update_snapshot_process_stats(container, snapshot_id)

            if status == "exited":
                exit_code = container.attrs["State"]["ExitCode"]
                snapshot.status = Snapshot.STATUS_COMPLETED if exit_code == 0 else Snapshot.STATUS_FAILED
                task.status = TaskStatus.SUCCESS if exit_code == 0 else TaskStatus.FAILED
                task.finishTime = timezone.now()
                task.result = TaskStatus.SUCCESS if exit_code == 0 else TaskStatus.FAILED
                snapshot.result = {
                    "exit_code": exit_code,
                    "container_id": container.id,
                }
                task.save(update_fields=["status", "finishTime", "result"])
                snapshot.save(update_fields=["status", "result"])

            # SEND Task status
            # TODO: optimize to sent an aggregated status PUT
            task.update_task_response()
            task.send_task_response()
            if status == "exited" and snapshot.status == Snapshot.STATUS_COMPLETED:
                # we have to split it, so the final message will be send as well
                container.remove()
                break

            time.sleep(5)  # TODO: check if this is not too much computation expensive

    finally:
        redis_conn.delete(queue_key)
        redis_conn.delete(control_key)

    # TODO: check if snapshot is okay
    if snapshot.status == Snapshot.STATUS_COMPLETED:
        queue = django_rq.get_queue("management")
        queue.enqueue(
            move_snapshot_to_longterm,
            snapshot.id
        )
        if snapshot.auto_update:
            queue.enqueue(
                move_snapshot_to_production,
                snapshot.id
            )

    # Return final result
    return {
        "pid": snapshot.process_id,
        "status": snapshot.status,
        "cmd": container_args,
        "result": snapshot.result,
    }


def move_snapshot_to_longterm(snapshot_id: str):
    """
    Copy WARCs and CDXJ indexes from production storage
    to long-term archival storage.
    """

    snapshot = Snapshot.objects.get(pk=snapshot_id)

    src_base = os.path.join(
        settings.BROWSERTIX_VOLUME,
        "collections",
        str(snapshot.replay_collection_id),
    )

    src_archive = os.path.join(src_base, "archive")
    src_indexes = os.path.join(src_base, "indexes")

    if not os.path.isdir(src_archive):
        raise FileNotFoundError(f"Production archive missing: {src_archive}")

    if not os.path.isdir(src_indexes):
        raise FileNotFoundError(f"Production indexes missing: {src_indexes}")

    dst_base = os.path.join(
        settings.LONGTERM_VOLUME,
        str(snapshot_id),
    )

    dst_archive = os.path.join(dst_base, "archive")
    dst_indexes = os.path.join(dst_base, "indexes")

    os.makedirs(dst_archive, exist_ok=True)
    os.makedirs(dst_indexes, exist_ok=True)

    # --------------------------------
    # Copy WARCs
    # --------------------------------
    for fname in os.listdir(src_archive):
        if not fname.endswith((".warc", ".warc.gz")):
            continue

        src_warc = os.path.join(src_archive, fname)
        dst_warc = os.path.join(dst_archive, fname)

        # overwrite-safe copy
        shutil.copy2(src_warc, dst_warc)
        stat = os.stat(dst_warc)

        Warc.objects.update_or_create(
            snapshot=snapshot,
            filename=fname,
            defaults={
                "path": dst_warc,
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_mtime),
                "sha256": calculate_sha256(dst_warc)
            },
        )

    # --------------------------------
    # Copy CDXJ indexes
    # --------------------------------
    for fname in os.listdir(src_indexes):
        if not fname.endswith(".cdxj"):
            continue

        src = os.path.join(src_indexes, fname)
        dst = os.path.join(dst_indexes, fname)

        shutil.copy2(src_warc, dst_warc)

    # TODO: ten warcPath jest niepotrzebny bo i tak lista warcow ma pelne patche
    snapshot.warc_path=src_archive
    snapshot.publication_status = snapshot.PUBLICATION_INTERNAL
    snapshot.save()

    snapshot.task.update_task_params({'snapshot': snapshot.build_json_response()})
    snapshot.task.update_task_response()

    # TODO: TASK bedzie mial chyba tylko 1 snapshot
    snapshot.task.send_task_response()


def remove_snapshot_from_production(snapshot_uid: str, task_uid: str = None):
    """
       - remove CDX entries from OutbackCDX
       - remove WARCs from production storage
       """
    snapshot = Snapshot.objects.get(uid=snapshot_uid)
    task = None
    if task_uid:
        task = Task.objects.get(uid=task_uid)
        task.status = TaskStatus.RUNNING
        task.save()
        task.send_task_response()

    # --------------------------------------------------
    # Production paths
    # --------------------------------------------------
    dst_archive = os.path.join(
        settings.PRODUCTION_VOLUME,
        "default",
        "archive",
    )
    src_indexes = os.path.join(
        settings.LONGTERM_VOLUME,
        str(snapshot.replay_collection_id),
        "indexes",
    )

    # --------------------------------------------------
    # OutbackCDX endpoint (per snapshot)
    # --------------------------------------------------
    outbackcdx_url = settings.OUTBACKCDX_URL.rstrip("/")
    delete_endpoint = f"{outbackcdx_url}/default/delete"

    for fname in sorted(os.listdir(src_indexes)):
        if not fname.endswith(".cdxj"):
            continue

        cdxj_path = os.path.join(src_indexes, fname)

        with open(cdxj_path, "rb") as fh:
            r = requests.post(
                delete_endpoint,
                data=fh,
                headers={"Content-Type": "text/plain"},
                timeout=120,
            )

        if r.status_code != 200:
            raise RuntimeError(
                f"OutbackCDX delete failed for {fname}: "
                f"{r.status_code} {r.text}"
            )

    # --------------------------------------------------
    # 2. Remove WARCs from production
    # --------------------------------------------------
    for warc in snapshot.warcs.objects.filter(is_production=True):
        if warc.path and os.path.exists(warc.path):
            os.remove(warc.path)


    # --------------------------------------------------
    # 3. Update DB state
    # --------------------------------------------------
    snapshot.publication_status = Snapshot.PUBLICATION_INTERNAL
    snapshot.published = False
    snapshot.save(update_fields=["publication_status", "published"])

    # --------------------------------------------------
    # 4. Notify task
    # --------------------------------------------------
    if task:
        task.update_task_response()
        task.send_task_response()


def move_snapshot_to_production(snapshot_uid: str, task_uid: str = None):
    """
    Use pre-generated CDXJ from Browsertrix, ingest into OutbackCDX,
    move WARCs to production, and register them in DB.
    """

    snapshot = Snapshot.objects.get(uid=snapshot_uid)
    task = None
    if task_uid:
        task = Task.objects.get(uid=task_uid)
        task.status = TaskStatus.RUNNING
        task.save()
        task.send_task_response()

    base_path = os.path.join(
        settings.LONGTERM_VOLUME,
        str(snapshot.replay_collection_id),
    )

    src_archive = os.path.join(base_path, "archive")
    src_indexes = os.path.join(base_path, "indexes")

    if not os.path.isdir(src_archive):
        raise FileNotFoundError(f"Archive dir missing: {src_archive}")

    if not os.path.isdir(src_indexes):
        raise FileNotFoundError(f"Indexes dir missing: {src_indexes}")

    # Production target
    dst_archive = os.path.join(
        settings.PRODUCTION_VOLUME,
        "default",
        "archive",
    )
    os.makedirs(dst_archive, exist_ok=True)

    # OutbackCDX endpoint (index per snapshot)
    outbackcdx_url = settings.OUTBACKCDX_URL.rstrip("/")
    cdx_endpoint = f"{outbackcdx_url}/default"

    # --------------------------------------------------
    # 1. Load CDXJ into OutbackCDX (per file)
    # --------------------------------------------------
    for fname in sorted(os.listdir(src_indexes)):
        if not fname.endswith(".cdxj"):
            continue

        cdxj_path = os.path.join(src_indexes, fname)

        with open(cdxj_path, "rb") as fh:
            r = requests.post(
                cdx_endpoint,
                data=fh,
                headers={"Content-Type": "text/plain"},
                timeout=120,
            )

        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"OutbackCDX ingest failed for {fname}: "
                f"{r.status_code} {r.text}"
            )

    # --------------------------------------------------
    # 2. Move WARCs + persist metadata
    # --------------------------------------------------
    warc_list = []
    for fname in os.listdir(src_archive):
        if not fname.endswith((".warc", ".warc.gz")):
            continue

        src_warc = os.path.join(src_archive, fname)
        dst_warc = os.path.join(dst_archive, fname)

        shutil.copy2(src_warc, dst_warc)

        stat = os.stat(dst_warc)

        Warc.objects.update_or_create(
            snapshot=snapshot,
            filename=fname,
            defaults={
                "path": dst_warc,
                "size_bytes": stat.st_size,
                "sha256": calculate_sha256(dst_warc),
                "created_at": datetime.fromtimestamp(stat.st_mtime),
                "is_production": True,
            },
        )
        warc_list.append(dst_warc)

    # TODO: jak z lista warcow
    snapshot.publication_status = snapshot.PUBLICATION_PUBLIC
    snapshot.published = True
    snapshot.save()

    # TODO: TASK bedzie mial chyba tylko 1 snapshot
    if task:
        task.update_task_response()
        task.send_task_response()

def repopulate_snapshot_to_production(website_id: int, task_uid: str):
    for snapshot in Snapshot.objects.filter(website_id=website_id):
        remove_snapshot_from_production(snapshot.uid, task_uid)
        move_snapshot_to_production(snapshot.uid, task_uid)


def trigger_website_cleanup(website_id: int):
    pass

def website_group_run_crawl_task(group_id: int):
    """
    For a website group:
    - fetch all websites
    - filter by enabled / doCrawl / suspendCrawlUntilTimestamp
    - enqueue separate crawl tasks per website
    """
    group = WebsiteGroup.objects.get(id=group_id)
    crawl_queue = django_rq.get_queue("crawls")

    now = timezone.now()
    enqueued = []

    websites = group.websites.filter(
        enabled=True,
        doCrawl=True,
        isDeleted=False,
    )

    for website in websites:
        if website.suspendCrawlUntilTimestamp and website.suspendCrawlUntilTimestamp > now:
            continue

        # Create Snapshot first (same pattern as single crawl)
        snapshot = Snapshot.objects.create(
            website=website,
            status=Snapshot.STATUS_PENDING
        )

        job = crawl_queue.enqueue(
            start_crawl_task,
            website.id,
            snapshot_id=snapshot.id
        )

        snapshot.rq_job_id = job.id
        snapshot.save(update_fields=["rq_job_id"])

        enqueued.append({
            "website_id": website.id,
            "snapshot_id": snapshot.id,
            "job_id": job.id,
        })

    return {
        "group_id": group_id,
        "enqueued": len(enqueued),
        "jobs": enqueued,
    }


def website_publish_all_task(website_id: int):
    """
    Enqueue replay_publish_task for EACH snapshot of a website.
    """
    website = Website.objects.get(id=website_id)
    website.auto_publish = False
    website.save(update_fields=["auto_publish"])

    queue = django_rq.get_queue("management")

    jobs = []
    # snapshot selected ONLY via website -> snapshot relation
    for snapshot in website.snapshot_set.all().order_by("id"):
        job = queue.enqueue(
            replay_unpublish_task,
            snapshot.id
        )
        jobs.append({
            "snapshot_id": snapshot.id,
            "job_id": job.id,
        })

    return {
        "website_id": website_id,
        "snapshots_enqueued": len(jobs),
        "jobs": jobs,
    }

def website_unpublish_all_task(website_id: int):
    """
    Enqueue replay_publish_task for EACH snapshot of a website.
    """
    website = Website.objects.get(id=website_id)
    website.auto_publish = True
    website.save(update_fields=["auto_publish"])

    queue = django_rq.get_queue("management")

    jobs = []
    # snapshot selected ONLY via website -> snapshot relation
    for snapshot in website.snapshot_set.all().order_by("id"):
        job = queue.enqueue(
            replay_publish_task,
            snapshot.id
        )
        jobs.append({
            "snapshot_id": snapshot.id,
            "job_id": job.id,
        })

    return {
        "website_id": website_id,
        "snapshots_enqueued": len(jobs),
        "jobs": jobs,
    }

def admin_platform_lock_task(*args, **kwargs):
    raise NotImplementedError("admin_platform_lock_task is not implemented yet")


def admin_platform_unlock_task(*args, **kwargs):
    raise NotImplementedError("admin_platform_unlock_task is not implemented yet")


def crawl_throttle_task(*args, **kwargs):
    raise NotImplementedError("crawl_throttle_task is not implemented yet")


def crawl_unthrottle_task(*args, **kwargs):
    raise NotImplementedError("crawl_unthrottle_task is not implemented yet")


def website_group_set_schedule_task(*args, **kwargs):
    raise NotImplementedError("website_group_set_schedule_task is not implemented yet")


def website_group_set_crawl_config_task(*args, **kwargs):
    raise NotImplementedError("website_group_set_crawl_config_task is not implemented yet")


def website_group_priority_crawl_task(*args, **kwargs):
    raise NotImplementedError("website_group_priority_crawl_task is not implemented yet")


def export_zosia_task(*args, **kwargs):
    raise NotImplementedError("export_zosia_task is not implemented yet")
