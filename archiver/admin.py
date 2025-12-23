from django.contrib import admin, messages

from archiver.services.crawl_manager import (queue_crawl, replay_publish,
                                             replay_unpublish)

from .models import (Snapshot, SnapshotResponseDelivery, Task,
                     TaskResponseDelivery, Warc, Website,
                     WebsiteCrawlParameters, WebsiteGroup)


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class DeleteReadOnlyAdmin(ReadOnlyAdmin):
    def has_delete_permission(self, request, obj=None):
        return True


admin.site.register(WebsiteCrawlParameters)
admin.site.register(WebsiteGroup)


@admin.register(Website)
class WebsiteAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "organisation")
    actions = ["queue_crawl_action", ]

    @admin.action(description="Queue crawl for selected websites")
    def queue_crawl_action(self, request, queryset):
        """
        Admin action: enqueue crawl jobs for selected websites.
        """
        success = 0
        failures = 0

        for website in queryset:
            try:
                queue_crawl(website.id)
                success += 1
            except Exception as exc:
                failures += 1
                self.message_user(
                    request,
                    f"Failed to queue crawl for {website}: {exc}",
                    level=messages.ERROR,
                )

        if success:
            self.message_user(
                request,
                f"Successfully queued {success} crawl job(s).",
                level=messages.SUCCESS,
            )

        if failures:
            self.message_user(
                request,
                f"{failures} crawl job(s) failed.",
                level=messages.WARNING,
            )


@admin.register(Snapshot)
class SnapshotAdmin(ReadOnlyAdmin):
    actions = ["publish_action", "unpublish_action"]
    @admin.action(description="Publish snapshot to production")
    def publish_action(self, request, queryset):
        """
        Admin action: enqueue crawl jobs for selected websites.
        """
        success = 0
        failures = 0

        for snapshot in queryset:
            try:
                job_id = replay_publish(snapshot.uid)
                success += 1
            except Exception as exc:
                failures += 1
                self.message_user(
                    request,
                    f"Failed to queue snapshot {snapshot} to production: {exc}",
                    level=messages.ERROR,
                )

        if success:
            self.message_user(
                request,
                f"Successfully queued {success} snapshot to production job(s).",
                level=messages.SUCCESS,
            )

        if failures:
            self.message_user(
                request,
                f"{failures} snapshot to production job(s) failed.",
                level=messages.WARNING,
            )

    @admin.action(description="Publish snapshot to production")
    def publish_action(self, request, queryset):
        """
        Admin action: enqueue crawl jobs for selected websites.
        """
        success = 0
        failures = 0

        for snapshot in queryset:
            try:
                job_id = replay_unpublish(snapshot.uid)
                success += 1
            except Exception as exc:
                failures += 1
                self.message_user(
                    request,
                    f"Failed to queue snapshot {snapshot} to production: {exc}",
                    level=messages.ERROR,
                )

        if success:
            self.message_user(
                request,
                f"Successfully queued {success} snapshot to production job(s).",
                level=messages.SUCCESS,
            )

        if failures:
            self.message_user(
                request,
                f"{failures} snapshot to production job(s) failed.",
                level=messages.WARNING,
            )


@admin.register(Task)
class TaskAdmin(DeleteReadOnlyAdmin):
    ordering = ("-created_at",)
    pass


@admin.register(Warc)
class WarcAdmin(ReadOnlyAdmin):
    pass


@admin.register(TaskResponseDelivery)
class TaskResponseDeliveryAdmin(DeleteReadOnlyAdmin):
    ordering = ("-created_at",)


@admin.register(SnapshotResponseDelivery)
class SnapshotResponseDeliveryAdmin(DeleteReadOnlyAdmin):
    ordering = ("-created_at",)
