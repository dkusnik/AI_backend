import subprocess

from django.conf import settings

from archiver.models import Website


def build_docker_command(website: Website, crawl_job_id: int):
    """
    Build the Browsertrix crawler command for the given website.
    """
    params = website.get_final_params()

    return [
        "docker", "run",
        "-v", f"{settings.BROWSERTIX_VOLUME}:/crawls/",
        "-it",
        "webrecorder/browsertrix-crawler",
        "crawl",
        "--text",
        "--scopeType", params['scope_type'],
        "--generateCDX", # TODO: if params['generate_cdx'] else "",
        "--workers", str(params['workers']),
        "--url", website.url,
        "--collection", str(crawl_job_id),
        "--pageLoadTimeout", str(params['page_load_timeout']),
        "--diskUtilization", str(params['disk_utilization']),
        "--timeLimit", str(params['time_limit']),
    ]


def run_crawl(website_id: int, crawl_job_id: int) -> subprocess.CompletedProcess:
    """
    Runs the Browsertrix crawler via docker.
    """
    website = Website.objects.get(id=website_id)
    cmd = build_docker_command(website, crawl_job_id)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    pid = proc.pid
    stdout, stderr = proc.communicate()

    return {
        "pid": pid,
        "returncode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "cmd": cmd,
    }

