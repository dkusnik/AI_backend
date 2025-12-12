import uuid

from django.contrib.auth.models import AbstractUser, User
from django.contrib.postgres.fields import ArrayField, JSONField
from django.db import models
from django.utils import timezone


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

# TODO: dla OpenID
#class User(AbstractUser):
#    # users are authenticated externally; we still persist a record linking to institution
#    institution = models.ForeignKey(Organization, null=True, blank=True, on_delete=models.SET_NULL, related_name='users')
#    external_id = models.CharField(max_length=512, blank=True, null=True, help_text="ID from external identity provider")

CRAWL_SCOPE_CHOICES = [
    ("prefix", "Prefix (URL prefix)"),
    ("domain", "Domain"),
    ("host", "Host"),
]


class WebsiteCrawlParameters(models.Model):
    """Global default parameters for Browsertrix crawler."""

    scope_type = models.CharField(
        max_length=16,
        choices=CRAWL_SCOPE_CHOICES,
        default="domain"
    )
    generate_cdx = models.BooleanField(default=True)
    workers = models.PositiveIntegerField(default=32)
    page_load_timeout = models.PositiveIntegerField(default=25)
    disk_utilization = models.PositiveIntegerField(default=0)
    time_limit = models.PositiveIntegerField(default=86400)

    def __str__(self):
        return "Global Browsertrix Default Parameters"


class Website(models.Model):
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name='websites')
    name = models.CharField(max_length=255)
    url = models.URLField()
    isDeleted = models.BooleanField(default=False)
    displayName = models.CharField(max_length=255, blank=True, null=True)
    shortDescription = models.TextField(blank=True, null=True)
    longDescription = models.TextField(blank=True, null=True)
    doCrawl = models.BooleanField(default=True)
    suspendCrawlUntilTimestamp = models.DateTimeField(null=True, blank=True)
    tags = models.ManyToManyField(Tag, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
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
        return f"{self.url} ({self.institution})"

    def get_final_params(self):
        """Return final parameters: override → fallback defaults."""
        defaults = WebsiteCrawlParameters.objects.first()
        return {
            "scope_type": self.scope_type or defaults.scope_type,
            "generate_cdx": self.generate_cdx if self.generate_cdx is not None else defaults.generate_cdx,
            "workers": self.workers or defaults.workers,
            "page_load_timeout": self.page_load_timeout or defaults.page_load_timeout,
            "disk_utilization": self.disk_utilization or defaults.disk_utilization,
            "time_limit": self.time_limit or defaults.time_limit,
        }


class WebsiteGroup(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    websites = models.ManyToManyField(Website)


class Snapshot(models.Model):
    STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('running', 'Running'),
        ('finished', 'Finished'),
        ('failed', 'Failed'),
        ('accepted', 'Accepted'),  # accepted into production queue
    ]

    STATUS_PUBLICATION = [
        ('INTERNAL', "internal"),
        ('PUBLIC', "public")
        ]

    # PENDING = "pending"
    # CRAWLING = "crawling"
    # COMPLETED = "completed"
    # FAILED = "failed"
    # DELETED = "deleted"

    website = models.ForeignKey(Website, on_delete=models.CASCADE, related_name='crawl_jobs')
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='queued')
    progress = models.FloatField(default=0.0)  # 0.0 - 100.0
    stats = models.JSONField(default=dict, blank=True)  # e.g. {'fetched': 123, 'bytes': 45678}
    process_id = models.IntegerField(null=True, blank=True)
    machine = models.CharField(max_length=255, null=True, blank=True)   # 👈 NEW
    error = models.TextField(null=True, blank=True)
    accepted = models.BooleanField(default=False)
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
        return f"CrawlJob {self.id} for {self.website.name}"

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
#  COMMENTS
# --------------------------------------------------------------------

class CommentEntryClass(models.TextChoices):
    PUBLIC = "public"
    PRIVATE = "private"
    HISTORY = "history"
    HISTORY_HIDDEN = "history_hidden"
    SYSTEM = "system"
    LOG = "log"


class CommentEntryLevel(models.TextChoices):
    NONE = "none"
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    ALERT = "alert"


class CommentThread(models.Model):
    accessMode = models.CharField(max_length=20)
    entityType = models.CharField(max_length=32)
    entityId = models.IntegerField()
    messages = models.IntegerField(default=0)
    lastTimestamp = models.DateTimeField(null=True, blank=True)
    lastAuthor = models.IntegerField(null=True, blank=True)
    lastMessage = models.TextField(blank=True, null=True)
    lastType = models.CharField(max_length=32, blank=True, null=True)


class Comment(models.Model):
    threadId = models.IntegerField()
    createdAt = models.DateTimeField(default=timezone.now)
    userId = models.IntegerField()
    body = models.TextField()
    entryClass = models.CharField(max_length=20, choices=CommentEntryClass.choices)
    entryLevel = models.CharField(max_length=20, choices=CommentEntryLevel.choices)

# --------------------------------------------------------------------
#  METADATA KEYS
# --------------------------------------------------------------------

class MetadataEntryString(models.Model):
    key = models.CharField(max_length=255)
    value = models.TextField()


class MetadataTechnicalOrganisation(models.Model):
    extra = models.ManyToManyField(MetadataEntryString, blank=True)


class MetadataTechnicalWebsite(models.Model):
    extra = models.ManyToManyField(MetadataEntryString, blank=True)


class MetadataTechnicalSnapshot(models.Model):
    crawler = models.CharField(max_length=255, blank=True, null=True)
    crawlerConfiguration = models.CharField(max_length=255, blank=True, null=True)
    crawlerOutput = models.TextField(blank=True, null=True)
    crawlWarcSize = models.BigIntegerField(null=True, blank=True)
    warehousePointer = models.CharField(max_length=255, blank=True, null=True)
    extra = models.ManyToManyField(MetadataEntryString, blank=True)


class MetadataArchivalOrganisation(models.Model):
    extra = models.ManyToManyField(MetadataEntryString, blank=True)


class MetadataArchivalWebsite(models.Model):
    extra = models.ManyToManyField(MetadataEntryString, blank=True)


class MetadataArchivalSnapshot(models.Model):
    extra = models.ManyToManyField(MetadataEntryString, blank=True)


# --------------------------------------------------------------------
#  TASKS
# --------------------------------------------------------------------

class TaskStatus(models.TextChoices):
    SCHEDULED = "scheduled"
    CANCELLED = "cancelled"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class Task(models.Model):
    action = models.CharField(max_length=64)
    uid = models.CharField(max_length=128)
    user = models.CharField(max_length=128)
    scheduleTime = models.DateTimeField(null=True, blank=True)
    startTime = models.DateTimeField(null=True, blank=True)
    updateTime = models.DateTimeField(null=True, blank=True)
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


class Category(models.Model):
    """
    Category model (from YAML).
    """

    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class Questionnaire(models.Model):
    """
    YAML: components.schemas.Questionnaire
    """

    title = models.CharField(max_length=500)
    header = models.TextField(blank=True, null=True)
    footer = models.TextField(blank=True, null=True)

    category = models.ForeignKey(
        Category,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="questionnaires"
    )

    is_deleted = models.BooleanField(default=False)
    is_published = models.BooleanField(default=False)

    def __str__(self):
        return f"Questionnaire {self.id}: {self.title}"


class Question(models.Model):
    """
    YAML: components.schemas.Question
    """

    INPUT_TYPES = [
        ("text", "Text"),
        ("textarea", "Textarea"),
        ("radio", "Radio"),
        ("checkbox", "Checkbox"),
        ("select", "Select"),
        ("date", "Date"),
        ("number", "Number"),
    ]

    questionnaire = models.ForeignKey(
        Questionnaire,
        on_delete=models.CASCADE,
        related_name="questions"
    )

    title = models.CharField(max_length=500)
    body = models.TextField(blank=True, null=True)

    input = models.CharField(max_length=20, choices=INPUT_TYPES)

    # newline-separated values for radio/select/checkbox
    values = models.TextField(
        blank=True,
        null=True,
        help_text="Possible values (newline-separated)",
    )

    def get_values_list(self):
        if not self.values:
            return []
        return [v.strip() for v in self.values.split("\n") if v.strip()]

    def __str__(self):
        return f"Q{self.id} ({self.input})"


class QuestionnaireResponse(models.Model):
    """
    YAML: components.schemas.QuestionnaireResponse

    - organisationId
    - questionnaireId
    - response: [string]
    """

    organisation_id = models.BigIntegerField()
    questionnaire = models.ForeignKey(
        Questionnaire,
        on_delete=models.CASCADE,
        related_name="responses",
    )

    # YAML: array of strings
    response = ArrayField(
        base_field=models.TextField(),
        default=list,
        blank=True,
    )

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("organisation_id", "questionnaire")

    def __str__(self):
        return f"Response org={self.organisation_id} q={self.questionnaire_id}"


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