"""
Recover tasks that are RUNNING in the local DB but that nobody manages anymore
(worker killed / OOM / redeploy / host reboot ...): restart them or mark them FAILED.

A RUNNING task is considered managed when ANY of these is true:
  * a live RQ worker (fresh heartbeat) is executing a job with this task_uid
  * a job with this task_uid is waiting in an RQ queue
  * (crawl_run only) its Browsertrix container still exists - see the special cases below

Special cases for crawl_run tasks (decided per container state):
  unwatched  container is still running/paused but no worker watches it. The crawl is
             alive, nobody will ever finalize the task.      -> skipped unless --include-unwatched
  finished   container exited with 0/137/143 while nobody watched: the crawl itself
             finished, only finalization is missing.          -> skipped unless --include-finished
  orphan     no container, or it died with another exit code  -> always processed

--mode restart  re-dispatches the task exactly like requeue_failed_tasks does
                (a crawl gets a NEW snapshot; the old one is marked failed).
                Actions that cannot be restarted automatically, and tasks that already
                reached --max-restarts, are marked FAILED instead.
--mode fail     marks task (+ unfinished snapshot) FAILED and notifies the frontend.

Needs access to Redis and to the Docker socket, so run it where the crawl workers run:
    docker compose exec worker_management python manage.py recover_orphan_tasks --mode fail --dry-run
"""
from datetime import timedelta
from datetime import timezone as dt_timezone

import django_rq
import docker
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from rq.worker import Worker

from archiver.models import Snapshot, Task, TaskStatus
from archiver.services.sync_tasks import dispatch_task

# actions whose sync handler really enqueues work (the others are `pass` stubs / quick control tasks)
RESTARTABLE_ACTIONS = {"crawl_run", "replay_publish", "replay_unpublish", "website_publish_all"}
FINISHED_EXIT_CODES = (0, 137, 143)            # same codes start_crawl_task treats as "crawl finished"
WORKER_HEARTBEAT_STALE = timedelta(minutes=3)  # RQ keeps dead workers registered for ~7 minutes
UNFINISHED_SNAPSHOT_STATUSES = (Snapshot.STATUS_PENDING, Snapshot.STATUS_CRAWLING,
                                Snapshot.STATUS_SUSPENDED)


def _aware(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=dt_timezone.utc)


def _task_uid_of(job):
    uid = (getattr(job, "kwargs", None) or {}).get("task_uid")
    return str(uid) if uid else None


def worker_task_uids(connection, now):
    """task_uids executed right now by RQ workers that are really alive."""
    uids = set()
    for worker in Worker.all(connection=connection):
        hb = _aware(getattr(worker, "last_heartbeat", None))
        if hb is not None and now - hb > WORKER_HEARTBEAT_STALE:
            continue                                   # registered, but dead
        try:
            job = worker.get_current_job()
        except Exception:
            continue
        uid = _task_uid_of(job) if job else None
        if uid:
            uids.add(uid)
    return uids


def queued_task_uids():
    uids = set()
    for name in settings.RQ_QUEUES:
        for job in django_rq.get_queue(name).jobs:
            uid = _task_uid_of(job)
            if uid:
                uids.add(uid)
    return uids


def containers_by_collection():
    """replay_collection_id -> Browsertrix container (labels are set in build_browsertrix_container_args)."""
    client = docker.from_env()
    client.ping()
    out = {}
    for c in client.containers.list(all=True, filters={"label": "app=browsertrix"}):
        sid = (c.labels or {}).get("snapshot_id")
        if sid:
            out[str(sid)] = c
    return out


def restarts_of(task) -> int:
    params = task.taskParameters if isinstance(task.taskParameters, dict) else {}
    try:
        return int(params.get("recoveryRestarts", 0) or 0)
    except (TypeError, ValueError):
        return 0


class Command(BaseCommand):
    help = ("Restart or fail RUNNING tasks that no worker/container manages anymore. "
            "Run it where Redis and the Docker socket are reachable (e.g. worker_management).")

    def add_arguments(self, parser):
        parser.add_argument("--mode", required=True, choices=["restart", "fail"],
                            help="What to do with orphaned tasks")
        parser.add_argument("--dry-run", action="store_true",
                            help="Only print the plan, change nothing")
        parser.add_argument("--uid", nargs="*", help="Limit to these task uids")
        parser.add_argument("--only-action", nargs="*", metavar="ACTION",
                            help="Limit to these Task.action values (e.g. crawl_run)")
        parser.add_argument("--min-age", type=int, default=10,
                            help="Ignore tasks changed within the last N minutes (default: 10)")
        parser.add_argument("--max-restarts", type=int, default=2,
                            help="With --mode restart: after N restarts the task is failed (default: 2)")
        parser.add_argument("--include-unwatched", action="store_true",
                            help="Also process crawls whose container is STILL RUNNING but unwatched "
                                 "(the container is stopped first)")
        parser.add_argument("--include-finished", action="store_true",
                            help="Also process crawls whose container already finished successfully")
        parser.add_argument("--skip-docker-check", action="store_true",
                            help="Do not look at Docker at all (DANGEROUS: running crawls look orphaned)")

    # ------------------------------------------------------------------ main
    def handle(self, *args, **o):
        now = timezone.now()
        candidates = list(self._candidates(o, now))
        if not candidates:
            self.stdout.write(self.style.SUCCESS(
                f"No RUNNING tasks idle for more than {o['min_age']} min. Nothing to do."))
            return

        probes = self._probe(candidates, o, now)
        plan = [self._plan(t, probes, o, now) for t in candidates]
        self._print_plan(plan)

        todo = [p for p in plan if p["decision"] != "skip"]
        if o["dry_run"]:
            self.stdout.write(self.style.WARNING(
                f"\nDry run: {len(todo)} task(s) would be changed, nothing was touched."))
            return

        results = [self._execute(p, o, now) for p in todo]
        errors = [r for r in results if r["error"]]
        self.stdout.write("\nResult:")
        for r in results:
            style = self.style.ERROR if r["error"] else self.style.SUCCESS
            self.stdout.write(style(f"  {r['uid']}  {r['outcome']}"))
        self.stdout.write(f"\nProcessed {len(results)}, skipped {len(plan) - len(todo)}, "
                          f"errors {len(errors)}")
        if errors:
            raise CommandError(f"{len(errors)} task(s) could not be recovered")

    # ------------------------------------------------------------ selection
    def _candidates(self, o, now):
        qs = (Task.objects.filter(status=TaskStatus.RUNNING,
                                  updateTime__lt=now - timedelta(minutes=o["min_age"]))
              .select_related("snapshot").order_by("updateTime"))
        if o.get("uid"):
            qs = qs.filter(uid__in=o["uid"])
        if o.get("only_action"):
            qs = qs.filter(action__in=o["only_action"])
        return qs

    def _probe(self, candidates, o, now):
        try:
            conn = django_rq.get_connection("management")
            probes = {"workers": worker_task_uids(conn, now), "queued": queued_task_uids(),
                      "containers": {}}
        except Exception as exc:
            raise CommandError(f"Cannot read RQ state from Redis: {exc}") from exc

        if any(t.action == "crawl_run" for t in candidates) and not o["skip_docker_check"]:
            try:
                probes["containers"] = containers_by_collection()
            except Exception as exc:
                raise CommandError(
                    f"Docker is not reachable ({exc}). Run this where /var/run/docker.sock is "
                    f"mounted (worker_management / crawl workers), or pass --skip-docker-check "
                    f"if you are SURE no crawl container is running.") from exc
        return probes

    # ---------------------------------------------------------------- plan
    def _plan(self, task, probes, o, now):
        uid = str(task.uid)
        snapshot = task.snapshot
        verdict, detail, container = "orphan", "no live worker, no queued job, no container", None

        if uid in probes["workers"]:
            verdict, detail = "managed", "executed by a live RQ worker"
        elif uid in probes["queued"]:
            verdict, detail = "queued", "its RQ job is waiting in a queue"
        elif task.action == "crawl_run" and snapshot is not None and snapshot.replay_collection_id:
            c = probes["containers"].get(str(snapshot.replay_collection_id))
            if c is not None:
                try:
                    c.reload()
                    state = c.status
                    code = (c.attrs.get("State") or {}).get("ExitCode")
                except Exception:
                    c, state, code = None, None, None   # vanished meanwhile -> plain orphan
                if c is not None:
                    container = c
                    if state in ("running", "paused", "restarting"):
                        verdict = "unwatched"
                        detail = f"container is {state}, but no worker watches it"
                    elif state == "exited" and code in FINISHED_EXIT_CODES:
                        verdict = "finished"
                        detail = f"container exited({code}) unattended - the crawl itself finished"
                    else:
                        detail = f"container is {state}" + (f" (exit {code})" if code is not None else "") \
                                 + ", no worker"

        process = (verdict == "orphan"
                   or (verdict == "unwatched" and o["include_unwatched"])
                   or (verdict == "finished" and o["include_finished"]))
        decision, reason = "skip", detail
        if process:
            decision = o["mode"]
            if decision == "restart":
                n = restarts_of(task)
                if task.action not in RESTARTABLE_ACTIONS:
                    decision = "fail"
                    reason += f"; action '{task.action}' cannot be restarted automatically"
                elif n >= o["max_restarts"]:
                    decision = "fail"
                    reason += f"; restart limit reached ({n}/{o['max_restarts']})"
        elif verdict in ("unwatched", "finished"):
            reason += (" -> skipped (use --include-unwatched)" if verdict == "unwatched"
                       else " -> skipped (use --include-finished)")

        idle = int((now - task.updateTime).total_seconds() // 60)
        return {"task": task, "snapshot": snapshot, "uid": uid, "container": container,
                "verdict": verdict, "decision": decision, "reason": reason, "idle_min": idle}

    def _print_plan(self, plan):
        self.stdout.write(f"RUNNING tasks inspected: {len(plan)}\n")
        for p in plan:
            t = p["task"]
            sid = p["snapshot"].pk if p["snapshot"] is not None else "-"
            style = {"skip": self.style.WARNING, "fail": self.style.ERROR,
                     "restart": self.style.NOTICE}[p["decision"]]
            self.stdout.write(style(
                f"{p['decision'].upper():8} {p['uid']}  [{t.action}]  snapshot={sid}  "
                f"idle={p['idle_min']}m  {p['verdict']}: {p['reason']}"))

    # ------------------------------------------------------------- execute
    @staticmethod
    def _claim(task_pk, seen_update_time, new_status):
        """
        Optimistic claim: succeeds only if the task is still RUNNING and untouched since we
        looked at it (no heartbeat, no watchdog, no other run of this command).
        """
        with transaction.atomic():
            fresh = Task.objects.select_for_update().get(pk=task_pk)   # no join: FK is nullable
            if fresh.status != TaskStatus.RUNNING or fresh.updateTime != seen_update_time:
                return None
            fresh.status = new_status
            fresh.save(update_fields=["status"])
            return fresh

    def _stop_container(self, container):
        try:
            container.reload()
            if container.status == "paused":
                container.unpause()
            if container.status in ("running", "paused", "restarting"):
                container.stop(timeout=30)        # graceful; container is kept for its logs
        except Exception as exc:
            self.stderr.write(self.style.WARNING(f"  could not stop container: {exc}"))

    @staticmethod
    def _fail_snapshot(snapshot):
        if snapshot is not None and snapshot.status in UNFINISHED_SNAPSHOT_STATUSES:
            snapshot.status = Snapshot.STATUS_FAILED
            snapshot.crawlStopTimestamp = timezone.now()
            snapshot.save(update_fields=["status", "crawlStopTimestamp"])

    def _execute(self, p, o, now):
        task, uid = p["task"], p["uid"]
        res = {"uid": uid, "outcome": "", "error": False}
        try:
            decision = p["decision"]
            fresh = self._claim(task.pk, task.updateTime,
                                TaskStatus.CREATED if decision == "restart" else TaskStatus.FAILED)
            if fresh is None:
                res["outcome"] = "skipped: task changed meanwhile (somebody is alive / handled it)"
                return res

            if p["container"] is not None and p["verdict"] == "unwatched":
                self._stop_container(p["container"])

            if decision == "fail":
                if fresh.action == "crawl_run":                  # for other actions the snapshot is only a target
                    self._fail_snapshot(p["snapshot"])
                fresh.result = TaskStatus.FAILED
                fresh.finishTime = fresh.finishTime or timezone.now()
                fresh.send_quick_status_message(
                    TaskStatus.FAILED,
                    f"Marked as failed by recover_orphan_tasks: {p['reason']}.")
                res["outcome"] = "marked FAILED and frontend notified"
                return res

            # ---- restart ----
            n = restarts_of(fresh)
            if fresh.action == "crawl_run":
                self._fail_snapshot(p["snapshot"])               # the new run gets a NEW snapshot
            fresh.update_task_params({"recoveryRestarts": n + 1})
            fresh.startTime = None
            fresh.finishTime = None
            fresh.result = None
            fresh.updateMessage = (f"Restarted by recover_orphan_tasks "
                                   f"(attempt {n + 1}/{o['max_restarts']}): {p['reason']}")
            fresh.save(update_fields=["startTime", "finishTime", "result", "updateMessage"])
            dispatch_task(fresh)                                  # never raises, marks FAILED itself on error

            after = Task.objects.get(pk=fresh.pk)
            if after.status == TaskStatus.FAILED:
                res["error"] = True
                res["outcome"] = f"restart FAILED: {after.updateMessage}"
            else:
                res["outcome"] = f"restarted (attempt {n + 1}/{o['max_restarts']}), status={after.status}"
        except Exception as exc:
            res["error"] = True
            res["outcome"] = f"ERROR {type(exc).__name__}: {exc}"
        return res
