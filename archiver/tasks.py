import json
import os
import signal
import socket
import subprocess
import time
from subprocess import PIPE, Popen

import django_rq
import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from rq import get_current_job

from archiver.models import Snapshot, Website
from archiver.services.crawl_runner import build_docker_command, run_crawl

from .models import Snapshot, Website

redis_conn = django_rq.get_connection("crawls")


def start_crawl_task(website_id, crawl_job_id=None):
    crawl_job = Snapshot.objects.get(id=crawl_job_id)

    # mark job as running
    crawl_job.status = "running"

    # determine machine name
    machine_name = getattr(settings, "CRAWL_MACHINE_NAME", socket.gethostname())
    crawl_job.machine = machine_name
    crawl_job.save(update_fields=["status", "machine"])

    # -------------------------------
    # build Browsertrix command
    # -------------------------------
    website = Website.objects.get(id=website_id)
    cmd = build_docker_command(website, crawl_job_id)

    # -------------------------------
    # run Browsertrix in BACKGROUND
    # -------------------------------
    #proc = Popen(
    #    cmd,
    #    stdout=PIPE,
    #    stderr=PIPE,
    #    text=True,
    #)
    proc = Popen(
        cmd,
        stdout=None,
        stderr=None,
        text=True,
        stdin=None,
        close_fds=True,
        start_new_session=True,
    )
    # save PID
    crawl_job.process_id = proc.pid
    crawl_job.save(update_fields=["process_id"])

    # -----------------------------------------------------
    # CREATE ephemeral worker queue:
    # crawl:<rq_job_id> shows this job is running
    # -----------------------------------------------------
    job_id = crawl_job.rq_job_id
    queue_key = f"crawl:{job_id}"
    control_key = f"{queue_key}:control"

    redis_conn.set(queue_key, "running") # set ex for expiration in seconds

    # -----------------------------------------------------
    # Background monitor loop
    # -----------------------------------------------------
    try:
        while True:
            # Check if process finished
            #print(proc.pid, "before proc.poll()")
            ret = proc.poll()
            #print(ret, "cheking controls")
            if ret is not None:
                crawl_job.result = {
                    "stdout": "",
                    "stderr": "",
                    "cmd": cmd,
                }
                crawl_job.status = "finished" if ret == 0 else "failed"
                crawl_job.save(update_fields=["result", "status"])
                break

            # Check for control commands from Redis
            control = redis_conn.get(control_key)
            if control:
                control_cmd = control.decode()
                if control_cmd == "stop":
                    print("GOT STOP")
                    crawl_job.status = "stopped"
                    os.kill(proc.pid, signal.SIGTERM)
                elif control_cmd == "suspend":
                    print("GOT SUSPEND")
                    os.kill(proc.pid, signal.SIGSTOP)
                    crawl_job.status = "suspended"
                elif control_cmd == "resume":
                    print("GOT RESUME")
                    os.kill(proc.pid, signal.SIGCONT)
                    crawl_job.status = "running"
                crawl_job.save(update_fields=["status"])
                redis_conn.delete(control_key)

            # Sleep to reduce CPU usage
            time.sleep(1)

    finally:
        # Remove ephemeral queue keys
        redis_conn.delete(queue_key)
        redis_conn.delete(control_key)

    # Return final result
    return {
        "pid": crawl_job.process_id,
        "status": crawl_job.status,
        "cmd": cmd,
        "result": crawl_job.result,
    }


def queue_crawl(website_id):
    queue = django_rq.get_queue("crawls")
    job = queue.enqueue(run_crawl, website_id)
    return job.id

import os
import shutil
import requests
from datetime import datetime
from django.conf import settings
from archiver.models import Snapshot, Warc


import os
import shutil
from django.conf import settings
from archiver.models import Snapshot


def move_snapshot_to_longterm(snapshot_id: str):
    """
    Copy WARCs and CDXJ indexes from production storage
    to long-term archival storage.
    """

    snapshot = Snapshot.objects.get(pk=snapshot_id)

    src_base = os.path.join(
        settings.BROWSERTIX_VOLUME,
        str(snapshot_id),
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

        src = os.path.join(src_archive, fname)
        dst = os.path.join(dst_archive, fname)

        # overwrite-safe copy
        shutil.copy2(src, dst)

    # --------------------------------
    # Copy CDXJ indexes
    # --------------------------------
    for fname in os.listdir(src_indexes):
        if not fname.endswith(".cdxj"):
            continue

        src = os.path.join(src_indexes, fname)
        dst = os.path.join(dst_indexes, fname)

        shutil.copy2(src, dst)


import os
import shutil
import requests
from datetime import datetime
from django.conf import settings
from archiver.models import Snapshot, Warc


def move_snapshot_to_production(snapshot_id: str):
    """
    Use pre-generated CDXJ from Browsertrix, ingest into OutbackCDX,
    move WARCs to production, and register them in DB.
    """

    snapshot = Snapshot.objects.get(pk=snapshot_id)

    base_path = os.path.join(
        settings.BROWSERTIX_VOLUME,
        "collections",
        str(snapshot_id),
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
        str(snapshot_id),
        "archive",
    )
    os.makedirs(dst_archive, exist_ok=True)

    # OutbackCDX endpoint (index per snapshot)
    outbackcdx_url = settings.OUTBACKCDX_URL.rstrip("/")
    cdx_endpoint = f"{outbackcdx_url}/{snapshot_id}?badLines=skip"

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
    for fname in os.listdir(src_archive):
        if not fname.endswith((".warc", ".warc.gz")):
            continue

        src_warc = os.path.join(src_archive, fname)
        dst_warc = os.path.join(dst_archive, fname)

        shutil.move(src_warc, dst_warc)

        stat = os.stat(dst_warc)

        Warc.objects.update_or_create(
            snapshot=snapshot,
            filename=fname,
            defaults={
                "path": dst_warc,
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_mtime),
            },
        )


def trigger_website_cleanup(website_id: int):
    pass

def replay_publish_task(snapshot_id: int):
    snap = Snapshot.objects.get(id=snapshot_id)
    snap.published = True
    snap.save(update_fields=["published"])
    return {"published": snapshot_id}


def replay_unpublish_task(snapshot_id: int):
    snap = Snapshot.objects.get(id=snapshot_id)
    snap.published = False
    snap.save(update_fields=["published"])
    return {"unpublished": snapshot_id}


def replay_repopulate_task(website_id: int | None = None):
    qs = Snapshot.objects.all()
    if website_id:
        qs = qs.filter(website_id=website_id)

    for snap in qs.iterator():
        redis_management.rpush("replay:repopulate", snap.id)

    return {"snapshots": qs.count()}

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
            status="queued"
        )

        job = crawl_queue.enqueue(
            start_crawl_task,
            website.id,
            crawl_job_id=snapshot.id
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
