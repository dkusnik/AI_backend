"""
Compare Task statuses reported by the FrontEnd with the local DB.

Read-only by default. The local DB is treated as the source of truth for task
status (the backend executes tasks and reports back), so --resend pushes the
LOCAL state to the frontend for every task that differs.

Examples:
    python manage.py compare_task_status
    python manage.py compare_task_status --status running scheduled
    python manage.py compare_task_status --alias finished=success
    python manage.py compare_task_status --resend
    python manage.py compare_task_status --json --fail-on-drift     # cron / monitoring
"""
import contextlib
import io
import json
from collections import defaultdict
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from archiver.models import Task
from archiver.services.sync_tasks import fetch_tasks_from_api

CHUNK = 500          # uids per DB query
SHOW_MISSING = 20    # max "missing locally" rows listed in text mode


def _norm(value, aliases=None):
    s = (value or "").strip().lower()
    return (aliases or {}).get(s, s)


def _describe_delivery(task) -> str:
    """Outcome of the last attempt to push this task to the frontend."""
    d = task.response_deliveries.first()      # Meta.ordering = -created_at
    if d is None:
        return "none"
    if d.success:
        return f"ok({d.http_status})"
    reason = d.http_status or (d.error_message or "unknown")[:60]
    return f"FAILED({reason})"


class Command(BaseCommand):
    help = ("Fetch tasks from the FrontEnd and check that their statuses match "
            "the local DB (read-only unless --resend).")

    def add_arguments(self, parser):
        parser.add_argument("--status", nargs="*",
                            help="Only fetch tasks with these FRONTEND statuses "
                                 "(space-separated, e.g. running scheduled)")
        parser.add_argument("--limit", type=int, default=50,
                            help="Page size when fetching tasks (default: 50)")
        parser.add_argument("--max-pages", type=int, default=200,
                            help="Safety cap on fetched pages (default: 200)")
        parser.add_argument("--alias", action="append", default=[], metavar="FRONT=LOCAL",
                            help="Treat a frontend status as a local one, e.g. "
                                 "--alias finished=success (repeatable)")
        parser.add_argument("--grace", type=int, default=30,
                            help="Mismatches whose local row changed in the last N seconds are "
                                 "reported as 'in flight', not drift (default: 30)")
        parser.add_argument("--local-only", action="store_true",
                            help="Also list local tasks the frontend did not return "
                                 "(ignored when --status is used)")
        parser.add_argument("--days", type=int, default=7,
                            help="Look-back window for --local-only (default: 7 days)")
        parser.add_argument("--resend", action="store_true",
                            help="Push the LOCAL state to the frontend for every drifted task")
        parser.add_argument("--json", dest="as_json", action="store_true",
                            help="Machine-readable output")
        parser.add_argument("--fail-on-drift", action="store_true",
                            help="Exit with code 1 if any drift remains (for cron/monitoring)")

    # ------------------------------------------------------------------ fetch
    def _fetch_all(self, statuses, page_limit, max_pages):
        items, seen, start = [], set(), 0
        for _ in range(max_pages):
            try:
                # fetch_tasks_from_api() print()s the whole response - keep stdout clean
                with contextlib.redirect_stdout(io.StringIO()):
                    data = fetch_tasks_from_api(start=start, limit=page_limit,
                                                where_status=statuses)
            except Exception as exc:
                raise CommandError(f"Could not fetch tasks from the frontend: {exc}") from exc

            page = data.get("items") or []
            new = [i for i in page if i.get("uid") and i["uid"] not in seen]
            if not new:                       # empty page, or frontend ignores `start`
                break
            for i in new:
                seen.add(i["uid"])
                items.append(i)
            if len(page) < page_limit:
                break
            start += page_limit
        else:
            self.stderr.write(self.style.WARNING(
                f"--max-pages={max_pages} reached; the comparison is INCOMPLETE"))
        return items

    @staticmethod
    def _load_local(uids):
        """uid -> [Task, ...] newest first (Task.uid is not unique in the DB)."""
        out = defaultdict(list)
        for i in range(0, len(uids), CHUNK):
            qs = Task.objects.filter(uid__in=uids[i:i + CHUNK]).order_by("-updateTime")
            for t in qs:
                out[t.uid].append(t)
        return out

    @staticmethod
    def _parse_aliases(pairs):
        aliases = {}
        for p in pairs:
            if "=" not in p:
                raise CommandError(f"--alias expects FRONT=LOCAL, got {p!r}")
            front, local = p.split("=", 1)
            aliases[front.strip().lower()] = local.strip().lower()
        return aliases

    # ------------------------------------------------------------------- main
    def handle(self, *args, **opts):
        aliases = self._parse_aliases(opts["alias"])
        statuses = opts["status"] or None
        grace = timedelta(seconds=opts["grace"])
        now = timezone.now()

        items = self._fetch_all(statuses, opts["limit"], opts["max_pages"])
        local = self._load_local([i["uid"] for i in items])

        report = {"ok": 0, "drift": [], "in_flight": [], "missing_locally": [],
                  "duplicate_uids_locally": [], "local_only": []}
        drift_tasks = {}

        for item in items:
            uid = item["uid"]
            front_status = _norm(item.get("status"), aliases)
            rows = local.get(uid)
            if not rows:
                report["missing_locally"].append(
                    {"uid": uid, "action": item.get("action"), "frontend_status": front_status})
                continue

            task = rows[0]
            if len(rows) > 1:
                report["duplicate_uids_locally"].append({"uid": uid, "rows": len(rows)})

            local_status = _norm(task.status)
            if front_status == local_status:
                report["ok"] += 1
                continue

            row = {
                "uid": uid,
                "action": task.action,
                "frontend_status": front_status,
                "local_status": local_status,
                "frontend_updated": item.get("updateTime"),
                "local_updated": task.updateTime.isoformat() if task.updateTime else None,
                "last_delivery": _describe_delivery(task),
                "local_message": (task.updateMessage or "")[:120] or None,
            }
            in_flight = task.updateTime is not None and (now - task.updateTime) < grace
            report["in_flight" if in_flight else "drift"].append(row)
            if not in_flight:
                drift_tasks[uid] = task

        if opts["local_only"]:
            if statuses:
                self.stderr.write(self.style.WARNING(
                    "--local-only ignored: it is meaningless together with --status"))
            else:
                fe_uids = {i["uid"] for i in items}
                since = now - timedelta(days=opts["days"])
                for t in Task.objects.filter(created_at__gte=since):
                    if t.uid not in fe_uids:
                        report["local_only"].append(
                            {"uid": t.uid, "action": t.action, "local_status": _norm(t.status),
                             "created_at": t.created_at.isoformat()})

        if opts["resend"]:
            for row in report["drift"]:
                try:
                    delivery = drift_tasks[row["uid"]].send_task_response()
                    row["resent"] = "ok" if getattr(delivery, "success", False) else "FAILED"
                except Exception as exc:
                    row["resent"] = f"FAILED({type(exc).__name__}: {exc})"

        if opts["as_json"]:
            summary = {"fetched": len(items), "same_status": report["ok"],
                       "drift": len(report["drift"]), "in_flight": len(report["in_flight"]),
                       "missing_locally": len(report["missing_locally"]),
                       "duplicate_uids_locally": len(report["duplicate_uids_locally"]),
                       "local_only": len(report["local_only"])}
            self.stdout.write(json.dumps({"summary": summary, **{k: v for k, v in report.items()
                                          if k != "ok"}}, indent=2, default=str))
        else:
            self._print_text(report, len(items), opts["resend"], opts["local_only"] and not statuses)

        unresolved = [r for r in report["drift"] if r.get("resent") != "ok"]
        if opts["fail_on_drift"] and unresolved:
            raise CommandError(f"{len(unresolved)} task(s) have a different status on the "
                               f"frontend than locally")

    # ----------------------------------------------------------------- output
    def _print_text(self, report, total, resend, local_only):
        w = self.stdout.write
        w(f"Tasks fetched from frontend : {total}")
        w(f"Same status                 : {report['ok']}")
        w(f"DRIFT (frontend != local)   : {len(report['drift'])}")
        w(f"In flight (changed < grace) : {len(report['in_flight'])}")
        w(f"Missing locally             : {len(report['missing_locally'])}")
        w(f"Duplicate uids locally      : {len(report['duplicate_uids_locally'])}")
        if local_only:
            w(f"Local only (not on frontend): {len(report['local_only'])}")

        def line(r):
            s = (f"  {r['uid']}  [{r['action']}]  frontend={r['frontend_status']}  "
                 f"local={r['local_status']}  local_updated={r['local_updated']}  "
                 f"last_delivery={r['last_delivery']}")
            return s + (f"  resent={r['resent']}" if "resent" in r else "")

        if report["drift"]:
            w(self.style.ERROR("\nDRIFT:"))
            for r in report["drift"]:
                w(line(r))
            if not resend:
                w("\nlast_delivery=FAILED(...) means the notification never reached the frontend. "
                  "Re-run with --resend to push the local state.")
        if report["in_flight"]:
            w(self.style.WARNING("\nIN FLIGHT (re-run in a moment):"))
            for r in report["in_flight"]:
                w(line(r))
        if report["missing_locally"]:
            w(self.style.WARNING("\nMISSING LOCALLY (normal for freshly scheduled tasks "
                                 "not imported yet):"))
            for r in report["missing_locally"][:SHOW_MISSING]:
                w(f"  {r['uid']}  [{r['action']}]  frontend={r['frontend_status']}")
            extra = len(report["missing_locally"]) - SHOW_MISSING
            if extra > 0:
                w(f"  ... and {extra} more (use --json for the full list)")
        if report["duplicate_uids_locally"]:
            w(self.style.WARNING("\nDUPLICATE UIDS IN LOCAL DB (newest row was compared):"))
            for r in report["duplicate_uids_locally"]:
                w(f"  {r['uid']}  rows={r['rows']}")
        if report["local_only"]:
            w(self.style.WARNING("\nLOCAL ONLY (frontend did not return them):"))
            for r in report["local_only"]:
                w(f"  {r['uid']}  [{r['action']}]  local={r['local_status']}  "
                  f"created={r['created_at']}")
        if not (report["drift"] or report["in_flight"]):
            w(self.style.SUCCESS("\nAll compared tasks have matching statuses."))
