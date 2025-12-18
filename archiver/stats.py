import json
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import docker
from docker.errors import APIError


@dataclass
class CrawlStats:
    # ---- crawler progress (from logs, authoritative for control) ----
    crawled: int = 0
    total: int = 0
    pending: int = 0
    failed: int = 0
    limit_hit: bool = False

    # ---- parsing volume (NOT content volume) ----
    log_bytes_parsed: int = 0
    log_lines_parsed: int = 0
    cdx_bytes_parsed: int = 0
    cdx_lines_parsed: int = 0

    # ---- log aggregation ----
    by_context: Counter = field(default_factory=Counter)
    by_log_level: Counter = field(default_factory=Counter)

    # ---- errors ----
    error_messages: Counter = field(default_factory=Counter)
    last_error: Optional[str] = None

    # ---- request classification (LOGS ONLY, non-canonical) ----
    by_request_type: Counter = field(default_factory=Counter)

    # ---- CDX (CANONICAL CONTENT STATS) ----
    by_mime: Counter = field(default_factory=Counter)
    by_http_status: Counter = field(default_factory=Counter)
    by_url_extension: Counter = field(default_factory=Counter)

    # ---- time ----
    last_update: Optional[datetime] = None
    last_page: Optional[str] = None


class BrowsertrixLogParser:
    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.file_offsets: Dict[Path, int] = {}

    def parse(self, stats: CrawlStats) -> CrawlStats:
        if not self.log_dir.exists():
            return stats

        for log_file in sorted(self.log_dir.glob("*.log")):
            print("BrowsertrixLogParser", log_file)
            stats = self._parse_file(log_file, stats)

        return stats

    def _parse_file(self, path: Path, stats: CrawlStats) -> CrawlStats:
        offset = self.file_offsets.get(path, 0)

        with path.open("r", encoding="utf-8") as f:
            f.seek(offset)

            for line in f:
                raw_len = len(line.encode("utf-8"))
                stats.log_bytes_parsed += raw_len
                stats.log_lines_parsed += 1

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                self._process_record(record, stats)

            self.file_offsets[path] = f.tell()

        return stats

    def _process_record(self, record: Dict, stats: CrawlStats):
        ts = self._parse_ts(record.get("timestamp"))
        context = record.get("context")
        level = record.get("logLevel")
        message = record.get("message")
        details = record.get("details", {})

        if context:
            stats.by_context[context] += 1
        if level:
            stats.by_log_level[level] += 1

        stats.last_update = ts or stats.last_update

        # ---- authoritative crawl control info ----
        if context == "crawlStatus":
            stats.crawled = details.get("crawled", stats.crawled)
            stats.total = details.get("total", stats.total)
            stats.pending = details.get("pending", stats.pending)
            stats.failed = details.get("failed", stats.failed)
            stats.limit_hit = details.get("limit", {}).get("hit", stats.limit_hit)

        if context == "worker" and message == "Starting page":
            stats.last_page = details.get("page")
            stats.by_request_type["page_start"] += 1

        if context == "pageStatus" and message == "Page Finished":
            stats.by_request_type["page_finished"] += 1

        if level == "error":
            stats.error_messages[message] += 1
            stats.last_error = message

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))


class CDXParser:
    def __init__(self, cdx_dir: Path):
        self.cdx_dir = cdx_dir
        self.file_offsets: Dict[Path, int] = {}

    def parse(self, stats: CrawlStats) -> CrawlStats:
        if not self.cdx_dir.exists():
            return stats

        for cdx_file in sorted(self.cdx_dir.glob("*.cdx*")):
            print("CDXParser", cdx_file)
            stats = self._parse_file(cdx_file, stats)

        return stats

    def _parse_file(self, path: Path, stats: CrawlStats) -> CrawlStats:
        offset = self.file_offsets.get(path, 0)

        with path.open("r", encoding="utf-8") as f:
            f.seek(offset)

            for line in f:
                raw_len = len(line.encode("utf-8"))
                stats.cdx_bytes_parsed += raw_len
                stats.cdx_lines_parsed += 1

                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                self._process_cdx_record(record, stats)

            self.file_offsets[path] = f.tell()

        return stats

    @staticmethod
    def _process_cdx_record(record: dict, stats: CrawlStats):
        url = record.get("url")
        status = record.get("status")
        mime = record.get("mime")

        if status:
            stats.by_http_status[str(status)] += 1
        if mime:
            stats.by_mime[mime] += 1

        if url:
            ext = os.path.splitext(urlparse(url).path)[1].lower()
            stats.by_url_extension[ext or "html"] += 1


@dataclass
class CrawlDerivedMetrics:
    cdx_bytes_per_sec: float = 0.0
    cdx_entries_per_sec: float = 0.0

    document_composition: Dict[str, float] = None
    mime_distribution: Dict[str, float] = None
    http_status_distribution: Dict[str, float] = None

    success_ratio: float = 0.0
    failure_ratio: float = 0.0

    log_stalled: bool = False
    cdx_stalled: bool = False
    crawler_running_no_cdx: bool = False


class CrawlMetricsCalculator:
    def __init__(self, stall_threshold_seconds: int = 30):
        self.prev_stats: Optional[CrawlStats] = None
        self.stall_threshold = timedelta(seconds=stall_threshold_seconds)

    def calculate(self, current: CrawlStats) -> CrawlDerivedMetrics:
        metrics = CrawlDerivedMetrics(
            document_composition={},
            mime_distribution={},
            http_status_distribution={},
        )

        if not self.prev_stats or not current.last_update:
            self.prev_stats = current
            return metrics

        dt = (current.last_update - self.prev_stats.last_update).total_seconds()
        if dt <= 0:
            return metrics

        # ---- throughput (CDX ONLY) ----
        metrics.cdx_bytes_per_sec = (
            (current.cdx_bytes_parsed - self.prev_stats.cdx_bytes_parsed) / dt
        )
        metrics.cdx_entries_per_sec = (
            (current.cdx_lines_parsed - self.prev_stats.cdx_lines_parsed) / dt
        )

        # ---- composition (CDX ONLY) ----
        total_docs = sum(current.by_url_extension.values()) or 1
        for ext, count in current.by_url_extension.items():
            metrics.document_composition[ext] = count / total_docs

        total_mime = sum(current.by_mime.values()) or 1
        metrics.mime_distribution = {
            mime: count / total_mime for mime, count in current.by_mime.items()
        }

        # ---- crawl quality (CDX ONLY) ----
        total_http = sum(current.by_http_status.values()) or 1
        metrics.http_status_distribution = {
            code: count / total_http
            for code, count in current.by_http_status.items()
        }

        success = sum(
            count for code, count in current.by_http_status.items()
            if str(code).startswith("2")
        )
        metrics.success_ratio = success / total_http
        metrics.failure_ratio = 1.0 - metrics.success_ratio

        # ---- health (CORRELATION) ----
        log_advancing = current.log_bytes_parsed > self.prev_stats.log_bytes_parsed
        cdx_advancing = current.cdx_lines_parsed > self.prev_stats.cdx_lines_parsed

        metrics.log_stalled = not log_advancing
        metrics.cdx_stalled = not cdx_advancing
        metrics.crawler_running_no_cdx = log_advancing and not cdx_advancing

        self.prev_stats = current
        return metrics



def get_browsertrix_container_stats(container) -> dict:
    """
    Collect Docker-level statistics for a Browsertrix container.
    Safe for running, exited, or partially-removed containers.
    """

    # -------------------------
    # Reload container metadata
    # -------------------------
    try:
        container.reload()
        attrs = container.attrs or {}
    except APIError:
        # Container may be gone
        return {
            "container": {
                "id": getattr(container, "id", None),
                "name": getattr(container, "name", None),
            },
            "state": {
                "status": "unknown",
            },
            "error": "Container metadata unavailable",
            "timestamps": {
                "collected_at": datetime.utcnow().isoformat() + "Z",
            },
        }

    state = attrs.get("State", {})
    config = attrs.get("Config", {}) or {}

    is_running = state.get("Running", False)

    # -------------------------
    # Runtime stats (optional)
    # -------------------------
    stats = {}
    if is_running:
        try:
            stats = container.stats(stream=False) or {}
        except APIError:
            stats = {}

    # -------------------------
    # CPU (safe)
    # -------------------------
    cpu_percent = 0.0
    try:
        cpu_stats = stats.get("cpu_stats", {})
        precpu = stats.get("precpu_stats", {})

        cpu_delta = (
            cpu_stats.get("cpu_usage", {}).get("total_usage", 0)
            - precpu.get("cpu_usage", {}).get("total_usage", 0)
        )
        system_delta = (
            cpu_stats.get("system_cpu_usage", 0)
            - precpu.get("system_cpu_usage", 0)
        )

        if system_delta > 0 and cpu_delta > 0:
            cpu_count = len(
                cpu_stats.get("cpu_usage", {}).get("percpu_usage", [])
            ) or 1
            cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
    except Exception:
        cpu_percent = 0.0

    # -------------------------
    # Memory (safe)
    # -------------------------
    mem_stats = stats.get("memory_stats", {}) or {}
    mem_usage = mem_stats.get("usage", 0)
    mem_limit = mem_stats.get("limit", 0)
    mem_percent = (
        (mem_usage / mem_limit) * 100.0 if mem_limit else 0.0
    )

    # -------------------------
    # Network (safe)
    # -------------------------
    net_rx = net_tx = 0
    for iface in (stats.get("networks") or {}).values():
        net_rx += iface.get("rx_bytes", 0)
        net_tx += iface.get("tx_bytes", 0)

    # -------------------------
    # Block IO (safe)
    # -------------------------
    blk_read = blk_write = 0
    for entry in (
        stats.get("blkio_stats", {})
        .get("io_service_bytes_recursive", [])
        or []
    ):
        if entry.get("op") == "Read":
            blk_read += entry.get("value", 0)
        elif entry.get("op") == "Write":
            blk_write += entry.get("value", 0)

    # -------------------------
    # Final structured payload
    # -------------------------
    return {
        "container": {
            "id": container.id,
            "short_id": container.short_id,
            "name": container.name,
            "image": config.get("Image"),
            "command": config.get("Cmd"),
            "labels": config.get("Labels", {}),
        },
        "state": {
            "status": container.status,
            "running": state.get("Running"),
            "paused": state.get("Paused"),
            "oom_killed": state.get("OOMKilled"),
            "exit_code": state.get("ExitCode"),
            "error": state.get("Error"),
            "started_at": state.get("StartedAt"),
            "finished_at": state.get("FinishedAt"),
            "restart_count": state.get("RestartCount"),
        },
        "resources": {
            "cpu_percent": round(cpu_percent, 2),
            "memory": {
                "usage_bytes": mem_usage,
                "limit_bytes": mem_limit,
                "percent": round(mem_percent, 2),
            },
            "pids": (stats.get("pids_stats") or {}).get("current"),
        },
        "io": {
            "network": {
                "rx_bytes": net_rx,
                "tx_bytes": net_tx,
            },
            "block": {
                "read_bytes": blk_read,
                "write_bytes": blk_write,
            },
        },
        "timestamps": {
            "collected_at": datetime.utcnow().isoformat() + "Z",
        },
    }
