import json
import uuid
import requests
from pathlib import Path

import yaml
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import models
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.utils.timezone import is_aware

from archiver.stats import CrawlDerivedMetrics, CrawlStats
from archiver.auth import get_keycloak_access_token


class Tag(models.Model):
    name = models.CharField(max_length=255)


class Organisation(models.Model):
    name = models.CharField(max_length=255)
    slug = models.SlugField(unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    parentId = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class UserStatus(models.TextChoices):
    REQUEST = "request"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    CLOSED = "closed"


class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    organisation = models.ForeignKey(
        Organisation,
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    phone = models.CharField(max_length=64, blank=True, null=True)
    emailActivated = models.BooleanField(default=False)
    status = models.CharField(
        max_length=32,
        choices=UserStatus.choices,
        default=UserStatus.REQUEST
    )

    def __str__(self):
        return f"{self.user.username} profile"


CRAWL_SCOPE_CHOICES = [
    ("prefix", "Prefix (URL prefix)"),
    ("domain", "Domain"),
    ("host", "Host"),
    ("page", "Page")
]


class DefaultWebsiteCrawlParameters(models.Model):
    """Global default parameters for Browsertrix crawler."""

    scope_type = models.CharField(
        max_length=16,
        choices=CRAWL_SCOPE_CHOICES,
        default="page"
    )
    generate_cdx = models.BooleanField(default=True)
    workers = models.PositiveIntegerField(default=32)
    page_load_timeout = models.PositiveIntegerField(default=25)
    disk_utilization = models.PositiveIntegerField(default=0)
    time_limit = models.PositiveIntegerField(default=86400)

    def __str__(self):
        return "Global Browsertrix Default Parameters"

    class Meta:
        abstract = True


class WebsiteCrawlParameters(DefaultWebsiteCrawlParameters):
    """
    Concrete Browsertrix crawl configuration.
    Maps directly to Browsertrix YAML (--config).
    """

    ENGINE_BROWSERTRIX = "browsertrix"
    ENGINE_HERITRIX = "heritrix"

    ENGINE_CHOICES = [
        (ENGINE_BROWSERTRIX, "Browsertrix"),
        (ENGINE_HERITRIX, "Heritrix"),
    ]

    name = models.CharField(max_length=128)
    description = models.TextField(blank=True)
    is_default = models.BooleanField(default=False)
    engine_type = models.CharField(max_length=32, choices=ENGINE_CHOICES)

    # --------------------
    # Crawl scope
    # --------------------
    start_urls = ArrayField(
        models.URLField(),
        default=list,
        help_text="Seed URLs (Browsertrix: seeds)",
        null=True,
        blank=True
    )

    include_linked = models.BooleanField(
        default=False,
        help_text="Include linked pages (Page List crawls)",
    )

    max_depth = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Crawl depth",
    )

    url_prefixes = ArrayField(
        models.CharField(max_length=512),
        default=list,
        blank=True,
        help_text="Regex URL prefixes to include",
    )

    additional_pages = ArrayField(
        models.URLField(),
        default=list,
        blank=True,
        help_text="Extra pages outside crawl scope",
    )

    # --------------------
    # Exclusions
    # --------------------
    exclude_text_matches = ArrayField(
        models.CharField(max_length=256),
        default=list,
        blank=True,
        help_text="Exclude URLs containing text",
    )

    exclude_regex_matches = ArrayField(
        models.CharField(max_length=512),
        default=list,
        blank=True,
        help_text="Exclude URLs matching regex",
    )

    # --------------------
    # Crawl limits
    # --------------------
    max_pages = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Maximum number of pages",
    )

    size_limit_gb = models.FloatField(
        null=True,
        blank=True,
        help_text="Maximum crawl size (GB)",
    )

    # --------------------
    # Page behavior
    # --------------------
    auto_scroll = models.BooleanField(default=True)
    auto_click = models.BooleanField(default=False)

    post_load_delay_ms = models.PositiveIntegerField(default=0)
    page_extra_delay_ms = models.PositiveIntegerField(default=0)

    # --------------------
    # Browser settings
    # --------------------
    user_agent = models.CharField(max_length=256, blank=True)
    language = models.CharField(
        max_length=32,
        blank=True,
        help_text="ISO 639[-country] code",
    )
    proxy_server = models.CharField(max_length=256, blank=True)

    # --------------------
    # Advanced / future
    # --------------------
    extra_config = models.JSONField(
        default=dict,
        blank=True,
        help_text="Raw Browsertrix YAML extensions (use only if needed)",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    @classmethod
    def create_or_update_from_task_parameters(
            cls,
            task_parameters: dict,
    ) -> tuple["WebsiteCrawlParameters", bool]:
        """
        Create or update WebsiteCrawlParameters from CrawlConfig taskParameters.
        Returns (instance, created)
        """

        if not isinstance(task_parameters, dict):
            raise TypeError("task_parameters must be a dict")

        crawl_engine = task_parameters.get("crawlEngine")
        engine_config = task_parameters.get("engineConfig") or {}

        if crawl_engine not in {cls.ENGINE_BROWSERTRIX, cls.ENGINE_HERITRIX}:
            raise ValueError(f"Unsupported crawlEngine: {crawl_engine}")

        if not isinstance(engine_config, dict):
            raise ValueError("engineConfig must be an object")

        name = task_parameters.get("name", "Unnamed crawl config")

        # -------------------------------------------------
        # Lookup or create shell instance
        # -------------------------------------------------
        instance, created = cls.objects.get_or_create(
            name=name,
            engine_type=crawl_engine,
            defaults={
                "description": task_parameters.get("description", ""),
                "is_default": False,
            },
        )

        # -------------------------------------------------
        # Common fields (always updated)
        # -------------------------------------------------
        instance.description = task_parameters.get("description", instance.description)

        # =================================================
        # Browsertrix
        # =================================================
        if crawl_engine == cls.ENGINE_BROWSERTRIX:
            crawl_scope = engine_config.get("crawlScope", {})
            crawl_limits = engine_config.get("crawlLimits", {})
            page_behavior = engine_config.get("pageBehavior", {})
            browser_settings = engine_config.get("browserSettings", {})

            exclude_pages = crawl_scope.get("excludePages", {})

            instance.start_urls = crawl_scope.get("startUrls", [])
            instance.include_linked = crawl_scope.get("includeLinked", False)
            instance.max_depth = crawl_scope.get("maxDepth")
            instance.url_prefixes = crawl_scope.get("urlPrefixes", [])
            instance.additional_pages = crawl_scope.get("additionalPages", [])

            instance.exclude_text_matches = exclude_pages.get("textMatches", [])
            instance.exclude_regex_matches = exclude_pages.get("regexMatches", [])

            instance.max_pages = crawl_limits.get("maxPages")
            instance.size_limit_gb = crawl_limits.get("sizeLimitGB")

            instance.auto_scroll = page_behavior.get("autoScroll", True)
            instance.auto_click = page_behavior.get("autoClick", False)
            instance.post_load_delay_ms = page_behavior.get("delayAfterPageLoadMs", 0)
            instance.page_extra_delay_ms = page_behavior.get(
                "delayBeforeNextPageMs", 0
            )

            instance.user_agent = browser_settings.get("userAgent", "")
            instance.language = browser_settings.get("language", "")
            instance.proxy_server = browser_settings.get("proxyServer", "")

            instance.extra_config = {
                k: v
                for k, v in engine_config.items()
                if k
                   not in {
                       "crawlScope",
                       "crawlLimits",
                       "pageBehavior",
                       "browserSettings",
                   }
            }

        # =================================================
        # Heritrix (future)
        # =================================================
        elif crawl_engine == cls.ENGINE_HERITRIX:
            instance.extra_config = engine_config

        instance.full_clean()
        instance.save()

        return instance, created

    def build_json_response(self) -> dict:
        """
        Generate TaskParameters-compatible crawlConfig.engineConfig
        structure according to the new schema.
        """

        engine_config: dict = {}

        # -------------------------------------------------
        # crawlScope
        # -------------------------------------------------
        crawl_scope: dict = {
            "type": self.scope_type,
            "startUrls": self.start_urls,
        }

        if self.include_linked:
            crawl_scope["includeLinked"] = True

        if self.max_depth is not None:
            crawl_scope["maxDepth"] = self.max_depth

        if self.url_prefixes:
            crawl_scope["urlPrefixes"] = self.url_prefixes

        if self.additional_pages:
            crawl_scope["additionalPages"] = self.additional_pages

        exclude_pages: dict = {}

        if self.exclude_text_matches:
            exclude_pages["textMatches"] = self.exclude_text_matches

        if self.exclude_regex_matches:
            exclude_pages["regexMatches"] = self.exclude_regex_matches

        if exclude_pages:
            crawl_scope["excludePages"] = exclude_pages

        engine_config["crawlScope"] = crawl_scope

        # -------------------------------------------------
        # crawlLimits
        # -------------------------------------------------
        crawl_limits: dict = {}

        if self.max_pages:
            crawl_limits["maxPages"] = self.max_pages

        if self.time_limit:
            crawl_limits["timeLimitSeconds"] = self.time_limit

        if self.size_limit_gb is not None:
            crawl_limits["sizeLimitGB"] = self.size_limit_gb

        if crawl_limits:
            engine_config["crawlLimits"] = crawl_limits

        # -------------------------------------------------
        # pageBehavior
        # -------------------------------------------------
        page_behavior: dict = {
            "autoScroll": self.auto_scroll,
            "autoClick": self.auto_click,
        }

        if self.page_load_timeout:
            page_behavior["pageLoadLimitMs"] = self.page_load_timeout * 1000

        if self.post_load_delay_ms:
            page_behavior["delayAfterPageLoadMs"] = self.post_load_delay_ms

        if self.page_extra_delay_ms:
            page_behavior["delayBeforeNextPageMs"] = self.page_extra_delay_ms

        if page_behavior:
            engine_config["pageBehavior"] = page_behavior

        # -------------------------------------------------
        # browserSettings
        # -------------------------------------------------
        browser_settings: dict = {}

        if self.user_agent:
            browser_settings["userAgent"] = self.user_agent

        if self.language:
            browser_settings["language"] = self.language

        if self.proxy_server:
            browser_settings["proxyServer"] = self.proxy_server

        if browser_settings:
            engine_config["browserSettings"] = browser_settings

        # -------------------------------------------------
        # forward-compatible extensions
        # -------------------------------------------------
        if self.extra_config:
            for key, value in self.extra_config.items():
                engine_config[key] = value

        # -------------------------------------------------
        # wrap into crawlConfig (WITH crawlEngine)
        # -------------------------------------------------
        return {
                "crawlEngine": "browsertrix",
                "engineConfig": engine_config,
        }


class Website(models.Model):
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE,
                                     related_name='websites', null=True, blank=True)
    website_crawl_parameters = models.ForeignKey(WebsiteCrawlParameters, on_delete=models.SET_NULL,
                                                 related_name='website_crawl_parameters', null=True,
                                                 blank=True)
    name = models.CharField(max_length=255)
    url = models.URLField()
    isDeleted = models.BooleanField(default=False)
    auto_publish = models.BooleanField(default=False)
    displayName = models.CharField(max_length=255, blank=True, null=True)
    shortDescription = models.TextField(blank=True, null=True)
    longDescription = models.TextField(blank=True, null=True)
    doCrawl = models.BooleanField(default=True)
    suspendCrawlUntilTimestamp = models.DateTimeField(null=True, blank=True)
    tags = models.ManyToManyField(Tag, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    scope_type = models.CharField(
        max_length=16,
        choices=CRAWL_SCOPE_CHOICES,
        null=True,
        blank=True,
        help_text="Optional override. If empty → uses global defaults."
    )
    generate_cdx = models.BooleanField(null=True, blank=True)
    workers = models.PositiveIntegerField(default=1)
    page_load_timeout = models.PositiveIntegerField(default=60)
    disk_utilization = models.PositiveIntegerField(default=0)
    time_limit = models.PositiveIntegerField(default=86400)

    enabled = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.url} ({self.organisation})"

    def build_json_response(self) -> dict:
        """
        Build Website JSON response according to OpenAPI schema.
        """

        def iso(dt):
            if not dt:
                return None
            if is_aware(dt):
                return dt.astimezone().isoformat().replace("+00:00", "Z")
            return dt.isoformat() + "Z"

        return {
            "id": self.id,

            "organisationId": self.organisation_id,

            "url": self.url,
            "isDeleted": self.isDeleted,

            "name": self.name,
            "displayName": self.displayName,
            "shortDescription": self.shortDescription,
            "longDescription": self.longDescription,

            "doCrawl": self.doCrawl,
            "suspendCrawlUntilTimestamp": iso(
                self.suspendCrawlUntilTimestamp
            ),

            "tags": [
                tag.build_json_response()
                for tag in self.tags.all()
            ],

            # ---------------------------------
            # Metadata (explicit placeholders)
            # ---------------------------------
            "metadataArchival": {
                "autoPublish": self.auto_publish,
                "enabled": self.enabled,
            },

            "metadataTechnical": {
                "scopeType": self.scope_type,
                "generateCdx": self.generate_cdx,
                "workers": self.workers,
                "pageLoadTimeout": self.page_load_timeout,
                "diskUtilization": self.disk_utilization,
                "timeLimit": self.time_limit,
            },
        }
    #
    # def get_final_params(self):
    #     """Return final parameters: override → fallback defaults."""
    #     params = self.website_crawl_parameters
    #
    #     if not params:
    #         params = WebsiteCrawlParameters()
    #         self.website_crawl_parameters = params
    #         self.save()
    #     if not params:
    #         raise RuntimeError(
    #             "No WebsiteCrawlParameters found (no website-specific and no default)"
    #         )
    #     if not params.start_urls:
    #         params.start_urls = [self.url]
    #     return params.build_json_response()

    @classmethod
    def create_from_task_parameters(cls, task_parameters: dict) -> "Website":
        """
        Create or resolve a Website instance from TaskParameters JSON.
        Prefers embedded `website` object, falls back to `websiteId`.
        """

        if not isinstance(task_parameters, dict):
            raise ValidationError("TaskParameters must be a JSON object")

        # -------------------------------------------------
        # 1️⃣ Preferred path: embedded website object
        # -------------------------------------------------
        website_data = task_parameters.get("website")
        if website_data:
            website_id = website_data.get("id")

            defaults = {
                "name": website_data.get("name"),
                "url": website_data.get("url"),
                "displayName": website_data.get("displayName"),
                "shortDescription": website_data.get("shortDescription"),
                "longDescription": website_data.get("longDescription"),
                "doCrawl": website_data.get("doCrawl", True),
                "isDeleted": website_data.get("isDeleted", False),
                "suspendCrawlUntilTimestamp": (
                    parse_datetime(website_data["suspendCrawlUntilTimestamp"])
                    if website_data.get("suspendCrawlUntilTimestamp")
                    else None
                ),
                "enabled": not website_data.get("isDeleted", False),
            }

            # Remove empty values (important for partial payloads)
            defaults = {k: v for k, v in defaults.items() if v is not None}

            website, _created = cls.objects.update_or_create(
                id=website_id,
                defaults=defaults,
            )

            return website

        website_id = task_parameters.get("websiteId")
        if website_id:
            try:
                return cls.objects.get(id=website_id)
            except cls.DoesNotExist:
                raise ValidationError(
                    f"Website with id={website_id} does not exist"
                )

        raise ValidationError(
            f"TaskParameters must contain either 'website' or 'websiteId': {task_parameters}"
        )


class WebsiteGroup(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    websites = models.ManyToManyField(Website)


class Snapshot(models.Model):
    # API-aligned status constants
    # - pending
    # - crawling
    # - completed
    # - failed
    # - deleted
    STATUS_PENDING = "pending"
    STATUS_CRAWLING = "crawling"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    STATUS_DELETED = "deleted"
    STATUS_PUBLISHED = "published"
    STATUS_SUSPENDED = "suspended"
    STATUS_STOPPED = "stopped"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_CRAWLING, "Crawling"),
        (STATUS_SUSPENDED, "Suspended"),
        (STATUS_STOPPED, "Stopped"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
        (STATUS_DELETED, "Deleted"),
        (STATUS_PUBLISHED, "Published"),
    ]

    PUBLICATION_INTERNAL = "internal"
    PUBLICATION_PUBLIC = "public"

    STATUS_PUBLICATION = [
        (PUBLICATION_INTERNAL, "internal"),
        (PUBLICATION_PUBLIC, "public"),
    ]

    # PENDING = "pending"
    # CRAWLING = "crawling"
    # COMPLETED = "completed"
    # FAILED = "failed"
    # DELETED = "deleted"

    website = models.ForeignKey(Website, on_delete=models.CASCADE, related_name='snapshots')
    uid = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique delivery UUID (idempotency key)",
    )
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    progress = models.FloatField(default=0.0)  # 0.0 - 100.0
    stats = models.JSONField(default=dict, blank=True)  # e.g. {'fetched': 123, 'bytes': 45678}
    machine = models.CharField(max_length=255, null=True, blank=True)  
    error = models.TextField(null=True, blank=True)
    published = models.BooleanField(default=False)
    auto_update = models.BooleanField(default=False)
    result = models.JSONField(default=dict, blank=True)
    rq_job_id = models.CharField(max_length=255, null=True, blank=True)

    isDeleted = models.BooleanField(default=False)
    publicationStatus = models.CharField(max_length=32, choices=STATUS_PUBLICATION, null=True, blank=True)

    crawlStartTimestamp = models.DateTimeField(null=True, blank=True)
    crawlStopTimestamp = models.DateTimeField(null=True, blank=True)

    size = models.BigIntegerField(null=True, blank=True)
    itemCount = models.BigIntegerField(null=True, blank=True)
    warcPath = models.CharField(max_length=512, blank=True, null=True)
    replayCollectionId = models.CharField(max_length=255, blank=True, null=True)

    process_id = models.CharField(max_length=64, null=True, blank=True)
    process_stats = models.JSONField(
        default=dict,
        blank=True,
        help_text="Docker-level runtime statistics for the crawl container",
    )

    crawl_stats = models.JSONField(
        null=True,
        blank=True,
        help_text="Aggregated crawl statistics and health metrics"
    )

    def mark_running(self):
        self.status = 'running'
        self.started_at = timezone.now()
        self.save(update_fields=['status','started_at'])

    def mark_finished(self, stats=None):
        self.status = 'finished'
        self.finished_at = timezone.now()
        if stats:
            self.stats = stats
        self.save(update_fields=['status','finished_at','stats'])

    def mark_failed(self, error=None):
        self.status = 'failed'
        self.finished_at = timezone.now()
        if error:
            self.error = str(error)
        self.save(update_fields=['status','finished_at','error'])

    def __str__(self):
        return f"CrawlJob {self.id} - {self.website.name} - {self.status} ({self.progress})"

    def update_snapshot_stats(self, stats: CrawlStats, derived: CrawlDerivedMetrics):
        self.crawl_stats = {
            # ---- control-plane ----
            "crawled": stats.crawled,
            "total": stats.total,
            "pending": stats.pending,

            # ---- parsing volume (diagnostic) ----
            "log_mb_parsed": round(stats.log_bytes_parsed / 1e6, 2),
            "cdx_mb_parsed": round(stats.cdx_bytes_parsed / 1e6, 2),

            # ---- archival truth (CDX) ----
            "http_status": dict(stats.by_http_status),
            "mime_distribution": dict(stats.by_mime),
            "url_extensions": dict(stats.by_url_extension),

            # ---- derived metrics ----
            "cdx_entries_per_sec": round(derived.cdx_entries_per_sec, 2),
            "cdx_bytes_per_sec": round(derived.cdx_bytes_per_sec, 1),
            "success_ratio": round(derived.success_ratio, 3),

            # ---- health ----
            "health": {
                "log_stalled": derived.log_stalled,
                "cdx_stalled": derived.cdx_stalled,
                "js_heavy": derived.crawler_running_no_cdx,
            },

            # ---- metadata ----
            "updated_at": timezone.now().isoformat(),
            "last_page": stats.last_page,
        }

        self.save(update_fields=["crawl_stats"])

    def build_json_response(self) -> dict:
        """
        Serialize Snapshot to API-compatible 'snapshot' structure
        """

        def iso(dt):
            if not dt:
                return None
            if not is_aware(dt):
                return dt.isoformat() + "Z"
            return dt.astimezone().isoformat().replace("+00:00", "Z")

        return {
            "uid": self.uid,
            "websiteId": self.website_id,
            "url": getattr(self.website, "url", None),
            "isDeleted": self.isDeleted,

            # Map internal status → API status
            "status": self.status,

            "publicationStatus": self.publicationStatus or "none",

            "crawlStartTimestamp": iso(self.crawlStartTimestamp),
            "crawlStopTimestamp": iso(self.crawlStopTimestamp),

            "size": self.size,
            "itemCount": self.itemCount,
            "warcPath": self.warcPath,
            "replayCollectionId": self.replayCollectionId,

            # Optional / future-safe
            "metadataArchival": {
                "extra": []
            },
        }


# --------------------------------------------------------------------
#  CRAWL CONFIG
# --------------------------------------------------------------------

class CrawlConfigStatus(models.TextChoices):
    DISABLED = "disabled"
    RESTRICTED = "restricted"
    PREDEFINED = "predefined"
    CUSTOM = "custom"


class CrawlConfig(models.Model):
    parentId = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=32, choices=CrawlConfigStatus.choices)
    crawlEngine = models.CharField(max_length=64)
    yamlConfig = models.TextField()


class ScheduleConfigStatus(models.TextChoices):
    DISABLED = "disabled"
    RESTRICTED = "restricted"
    PREDEFINED = "predefined"
    CUSTOM = "custom"


class ScheduleConfig(models.Model):
    parentId = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=32, choices=ScheduleConfigStatus.choices)
    yamlConfig = models.TextField()

# --------------------------------------------------------------------
#  TASKS
# --------------------------------------------------------------------

class TaskStatus(models.TextChoices):
    CREATED = "created"
    SCHEDULED = "scheduled"
    CANCELLED = "cancelled"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PAUSED = "paused"


class Task(models.Model):
    snapshot = models.OneToOneField(Snapshot, on_delete=models.SET_NULL, related_name='task',
                                    null=True, blank=True)
    action = models.CharField(max_length=64)
    uid = models.CharField(max_length=128, unique=True)
    user = models.CharField(max_length=128, null=True, blank=True)
    scheduleTime = models.DateTimeField(null=True, blank=True)
    startTime = models.DateTimeField(auto_now_add=True)
    updateTime = models.DateTimeField(auto_now=True)
    updateMessage = models.TextField(blank=True, null=True)
    finishTime = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=32, choices=TaskStatus.choices)
    result = models.TextField(blank=True, null=True)
    resultDescription = models.TextField(blank=True, null=True)
    runData = models.TextField(blank=True, null=True)

    priority = models.CharField(max_length=32, default="normal")
    schedule = models.CharField(max_length=64, blank=True, null=True)

    # JSON fields (PostgreSQL)
    taskParameters = models.JSONField(null=True, blank=True)
    taskResponse = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.id} - {self.uid} ({self.action} - {self.status})"

    @classmethod
    def build_taskParameters(cls, snapshot) -> dict:

        """
        Generate TaskParameters dict according to OpenAPI spec.
        Embedded objects are preferred; IDs are included as fallback.
        """
        data = {}

        if snapshot:
            data["snapshot"] = snapshot.build_json_response()
            if snapshot.website:
                data["website"] = snapshot.website.build_json_response()
                data["websiteId"] = snapshot.website.id
                if snapshot.website.website_crawl_parameters:
                    data["crawlConfig"] = snapshot.website.website_crawl_parameters.build_json_response()
                    data["crawlConfigId"] = snapshot.website.website_crawl_parameters.id

        # TODO:Schedule

        # if schedule:
        #     data["schedule"] = schedule.build_json_response()
        #     data["scheduleId"] = schedule.id
        # elif schedule_id:
        #     data["scheduleId"] = schedule_id

        return data

    @classmethod
    def create_for_website(
            cls,
            *,
            snapshot: Snapshot,
            action: str,
            user: str | None = None,
            priority: str = "normal",
            schedule: str | None = None,
            schedule_time=None,
    ) -> "Task":
        """
        Create a new Task for a given website.
        """

        # -------------------------------------------------
        # Create task
        # -------------------------------------------------
        task = cls.objects.create(
            action=action,
            uid=str(uuid.uuid4()),
            user=user,
            snapshot=snapshot,
            status=TaskStatus.SCHEDULED,
            taskParameters=Task.build_taskParameters(snapshot),
            priority=priority,
            schedule=schedule,
            scheduleTime=schedule_time,

            updateTime=timezone.now(),
        )

        return task

    def build_browsertrix_yaml_config(self, snapshot_id: int) -> Path:
        """
        Write Browsertrix YAML config compatible with `browsertrix-crawler crawl --config`.
        """

        yaml_config = {}
        task_params = self.taskParameters or {}

        # -------------------------------------------------
        # Resolve crawlScope from TaskParameters
        # -------------------------------------------------
        try:
            crawl_config = task_params["crawlConfig"]
            engine_config = crawl_config.get("engineConfig")
            if isinstance(engine_config,str):
                engine_config = dict()
            scope = engine_config.get("crawlScope",dict())
        except KeyError as exc:
            raise ValueError(
                f"Invalid TaskParameters structure, missing {exc}"
            ) from exc

        # -----------------
        # Seeds / scope
        # -----------------

        yaml_config["seeds"] = scope.get("startUrls", [self.snapshot.website.url])
        if not yaml_config["seeds"]:
            yaml_config["seeds"] = [self.snapshot.website.url]
        yaml_config["scopeType"] = scope.get("type", "page")

        if "maxDepth" in scope:
            yaml_config["depth"] = scope["maxDepth"]

        if scope.get("includeLinked"):
            yaml_config["includeLinked"] = True

        if scope.get("urlPrefixes"):
            yaml_config["scopeIncludeRx"] = scope["urlPrefixes"]

        # -----------------
        # Additional / exclude pages
        # -----------------
        if scope.get("additionalPages"):
            yaml_config["extraPages"] = scope["additionalPages"]

        exclude = scope.get("excludePages", {})
        if exclude.get("textMatches"):
            yaml_config.setdefault("exclude", []).extend(
                [t for t in exclude["textMatches"]]
            )
        if exclude.get("regexMatches"):
            yaml_config.setdefault("exclude", []).extend(
                [r for r in exclude["regexMatches"]]
            )

        # -----------------
        # Crawl limits
        # -----------------
        limits = engine_config.get("crawlLimits", {})
        if limits.get("maxPages"):
            yaml_config["pageLimit"] = limits["maxPages"]

        if limits.get("timeLimitSeconds"):
            yaml_config["timeLimit"] = limits["timeLimitSeconds"]

        if limits.get("sizeLimitGB"):
            yaml_config["sizeLimit"] = int(limits["sizeLimitGB"] * 1024 * 1024 * 1024)

        # -----------------
        # Page behavior
        # -----------------
        behavior = engine_config.get("pageBehavior", {})
        if behavior.get("autoScroll"):
            yaml_config.setdefault("behaviors", []).append("autoscroll")
        if behavior.get("autoClick"):
            yaml_config.setdefault("behaviors", []).append("autoclick")

        if behavior.get("pageLoadLimitMs"):
            yaml_config["pageLoadTimeout"] = behavior["pageLoadLimitMs"] // 1000

        if behavior.get("delayAfterPageLoadMs"):
            yaml_config["postLoadDelay"] = behavior["delayAfterPageLoadMs"] // 1000

        if behavior.get("delayBeforeNextPageMs"):
            yaml_config["pageExtraDelay"] = behavior["delayBeforeNextPageMs"] // 1000

        # -----------------
        # Browser settings
        # -----------------
        browser = engine_config.get("browserSettings", {})
        if browser.get("userAgent"):
            yaml_config["userAgent"] = browser["userAgent"]
        if browser.get("language"):
            yaml_config["lang"] = browser["language"]
        if browser.get("proxyServer"):
            yaml_config["proxyServer"] = browser["proxyServer"]

        # -----------------
        # Output & indexing
        # -----------------
        yaml_config["generateCDX"] = True
        yaml_config["collection"] = str(snapshot_id)
        yaml_config["cwd"] = "/crawls"

        # -----------------
        # Write file
        # -----------------
        config_dir = Path(settings.BROWSERTIX_VOLUME) / "configs"
        config_dir.mkdir(parents=True, exist_ok=True)

        path = config_dir / f"browsertrix_{snapshot_id}.yaml"
        path.write_text(yaml.dump(yaml_config, sort_keys=False))
        print(yaml.dump(yaml_config, sort_keys=False))
        return path

    def build_task_response(self) -> dict:
        """
        Build TaskResponse JSON according to Swagger schema.
        Snapshot is optional.
        """

        snapshot = self.snapshot

        response = {
            # ---- identity ----
            "task_id": self.uid,
            "snapshot_id": snapshot.id if snapshot else None,
            "status": self.status,
            "updated_at": timezone.now().isoformat(),

            # ---- uuid ----
            "uuid": str(uuid.uuid4()),

            # warcs
            "warcs": [
                warc.build_json_response()
                for warc in snapshot.warcs.all()
            ],

            # ---- crawl stats ----
            "crawl_stats": snapshot.crawl_stats if snapshot else {},

            # ---- container / process stats ----
            "container_stats": snapshot.process_stats if snapshot else {},
        }

        return response

    def update_task_params(self, params: dict) -> None:
        """
        Merge params into taskParameters JSON field and persist changes.
        """
        if not isinstance(params, dict):
            raise TypeError("params must be a dict")

        current = self.taskParameters or {}
        current.update(params)

        self.taskParameters = current
        self.save(update_fields=["taskParameters"])

    def _build_task_api_payload(self) -> dict:
        """
        Build a payload matching the Task schema defined in OpenAPI YAML.
        """
        return {
            "action": self.action,
            "uid": self.uid,
            "user": self.user,
            "scheduleTime": self.scheduleTime.isoformat() if self.scheduleTime else None,
            "startTime": self.startTime.isoformat() if self.startTime else None,
            "updateTime": self.updateTime.isoformat() if self.updateTime else None,
            "updateMessage": self.updateMessage,
            "finishTime": self.finishTime.isoformat() if self.finishTime else None,
            "status": self.status,
            "result": self.result,
            "resultDescription": self.resultDescription,
            "runData": self.runData,
            "priority": self.priority,
            "schedule": self.schedule,
            "taskParameters": self.taskParameters,
            "taskResponse": self.taskResponse,
        }

    def send_task_response(self) -> "TaskResponseDelivery":
        """
        Send Task.taskResponse JSON to TASK_RESPONSE_URL via PUT.
        Stores every delivery attempt.
        """

        if not self.taskResponse:
            raise ValueError("Task.taskResponse is empty – nothing to send")

        payload = [self._build_task_api_payload()]
        delivery = TaskResponseDelivery.objects.create(
            task=self,
            payload=payload,
            target_url=settings.TASK_RESPONSE_URL,
            http_method="PUT",
        )

        token = get_keycloak_access_token()

        try:
            response = requests.put(
                settings.TASK_RESPONSE_URL,
                json=payload,
                timeout=getattr(settings, "TASK_RESPONSE_TIMEOUT", 10),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    # Idempotency / traceability
                    # "X-Delivery-UUID": str(delivery.id),
                },
            )

            delivery.http_status = response.status_code
            delivery.response_body = response.text
            delivery.success = response.ok

        except Exception as exc:
            delivery.success = False
            delivery.error_message = str(exc)

        delivery.save(update_fields=[
            "http_status",
            "response_body",
            "success",
            "error_message",
        ])
        return delivery


    def update_task_response(self, save=True) -> dict:
        """
        Build and persist TaskResponse JSON into task.taskResponse.
        """

        response = self.build_task_response()

        self.taskResponse = response
        self.updateTime = timezone.now()

        if save:
            self.save(update_fields=["taskResponse", "updateTime"])

        return response

class Warc(models.Model):
    snapshot = models.ForeignKey(
        "Snapshot",
        on_delete=models.CASCADE,
        related_name="warcs",
    )
    filename = models.CharField(max_length=512)
    path = models.TextField()  # full production path
    size_bytes = models.BigIntegerField()
    sha256 = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("snapshot", "filename")

    def __str__(self):
        return f"{self.filename} ({self.size_bytes} bytes)"

    def build_json_response(self) -> dict:
        return {
            "filename": self.filename,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256 or None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "path": self.path,
        }

class GlobalConfig(models.Model):
    ENTRY_CLASS_CHOICES = [
        ("global", "Global"),
        ("restricted", "Restricted"),
        ("system", "System"),
    ]

    key = models.CharField(max_length=255, unique=True)
    value = models.TextField(blank=True, null=True)
    entry_class = models.CharField(
        max_length=20,
        choices=ENTRY_CLASS_CHOICES,
        default="global"
    )

    def __str__(self):
        return f"{self.key} = {self.value}"


class TaskResponseDelivery(models.Model):
    """
    Persistent audit log of TaskResponse deliveries.
    Each row = one PUT attempt.
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique delivery UUID (idempotency key)",
    )

    task = models.ForeignKey(
        "Task",
        on_delete=models.CASCADE,
        related_name="response_deliveries",
    )

    # ---- payload ----
    payload = models.JSONField(
        help_text="Exact Task.taskResponse JSON sent",
    )

    # ---- target ----
    target_url = models.URLField()
    http_method = models.CharField(max_length=8, default="PUT")

    # ---- result ----
    http_status = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="HTTP status returned by receiver",
    )

    response_body = models.TextField(
        blank=True,
        help_text="Raw response body (if any)",
    )

    success = models.BooleanField(default=False)

    error_message = models.TextField(
        blank=True,
        help_text="Network / exception error (if any)",
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["task"]),
            models.Index(fields=["success"]),
        ]

    def __str__(self):
        return f"TaskResponseDelivery {self.id} task={self.task_id}"

