import django_rq
from django.core.management.base import BaseCommand

from archiver.models import Website, WebsiteCrawlParameters
from archiver.services.crawl_manager import (get_crawl_status,
                                             resume_crawl, queue_crawl,
                                             stop_crawl, suspend_crawl)


class Command(BaseCommand):
    help = "Control Browsertrix crawl jobs"

    def add_arguments(self, parser):
        parser.add_argument("action", choices=["start", "status", "stop", "suspend", "resume"],
                            help="Action to perform")
        parser.add_argument("--website", type=str, help="Website ID (required for start)")
        parser.add_argument("--job", type=str, help="Job ID (required for status or stop)")

    def handle(self, *args, **options):
        action = options["action"]

        if action == "start":
            website_arg = options.get("website")
            if not website_arg:
                self.stderr.write("Error: --website parameter required for start")
                return

            # Try to interpret website_arg as an integer ID
            website_id = None
            try:
                website_id = int(website_arg)
            except ValueError:
                pass

            # If integer ID failed - interpret as name
            if website_id is None:
                try:
                    website = Website.objects.get(name=website_arg)
                    website_id = website.id
                except Website.DoesNotExist:
                    self.stderr.write(
                        f"Error: '{website_arg}' is not a valid website ID or name"
                    )
                    return

            if not website.website_crawl_parameters:
                website.website_crawl_parameters = WebsiteCrawlParameters.objects.create()
                website.save()

            # Now website_id is guaranteed to be valid
            job_id = queue_crawl(website_id)
            self.stdout.write(f"Started crawl job for website {website_id}: {job_id}")

        if action == "status":
            job_id = options.get("job")
            # ----------------------------------------------------------
            # 1) If job_id given - show status for this specific job
            # ----------------------------------------------------------
            if job_id:
                status_info = get_crawl_status(job_id)
                self.stdout.write(str(status_info))
                return

            # ----------------------------------------------------------
            # 2) No job_id - list status of ALL jobs in "crawls" queue
            # ----------------------------------------------------------
            queue = django_rq.get_queue("crawls")
            connection = queue.connection

            from rq.job import Job

            job_ids = (
                queue.job_ids  # waiting jobs
            )

            # Also check started/finished jobs
            started_ids = queue.started_job_registry.get_job_ids()
            finished_ids = queue.finished_job_registry.get_job_ids()
            failed_ids = queue.failed_job_registry.get_job_ids()

            all_ids = list(job_ids) + list(started_ids) + list(finished_ids) + list(failed_ids)

            if not all_ids:
                self.stdout.write("No jobs found in queue 'crawls'.")
                return

            for jid in all_ids:
                try:
                    job = Job.fetch(jid, connection=connection)
                    self.stdout.write(
                        f"{jid}: {job.get_status()} | result={job.result if job.is_finished else None}"
                    )

                except Exception:
                    self.stdout.write(f"{jid}: <unable to fetch>")
        if action == "stop":
            ident = options.get("job")
            if not ident:
                return self.stderr.write("--job required")
            return self.stdout.write("OK" if stop_crawl(ident) else "FAILED")

            # SUSPEND
        if action == "suspend":
            ident = options.get("job")
            if not ident:
                return self.stderr.write("--job required")
            return self.stdout.write("OK" if suspend_crawl(ident) else "FAILED")

            # RESUME
        if action == "resume":
            ident = options.get("job")
            if not ident:
                return self.stderr.write("--job required")
            return self.stdout.write("OK" if resume_crawl(ident) else "FAILED")
