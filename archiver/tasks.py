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

def move_crawl_to_production(job_id: str):
    """
    Trigger production saving for an accepted job.
    Could:
      - copy files to production storage
      - call a remote job to import WARC into an archival system
    """
    try:
        job_obj = Snapshot.objects.get(pk=job_id)
        # example: call production API
        production_api = getattr(settings, 'PRODUCTION_IMPORT_API', None)
        if production_api:
            r = requests.post(f"{production_api}/import", json={"crawl_job_id": str(job_id)})
            if r.status_code not in (200,201):
                job_obj.error = f"production import failed: {r.status_code}"
                job_obj.save(update_fields=['error'])
                return
        # mark accepted already done in view; optionally update more
    except Exception as e:
        job_obj.error = str(e)
        job_obj.save(update_fields=['error'])
        raise

def trigger_website_cleanup(website_id: int):
    pass