from django.contrib import messages
from django.utils.translation import gettext_lazy as _

from archiver.services.crawl_manager import start_crawl


def admin_start_crawl(modeladmin, request, queryset):
    """
    Admin action: enqueue crawl tasks for selected websites
    """
    started = 0
    skipped = 0

    for website in queryset:
        if not website.enabled:
            skipped += 1
            continue
        job_id = start_crawl(website.id)
        started += 1

    messages.success(
        request,
        _(f"Started {started} crawl(s). Skipped {skipped} disabled website(s).")
    )


admin_start_crawl.short_description = "Start crawl for selected websites"
