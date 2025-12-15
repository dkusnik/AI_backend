from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

from .admin_actions import admin_start_crawl
from .models import (Organisation,
                     Snapshot, Task, User, Website,
                     WebsiteCrawlParameters, WebsiteGroup)

#admin.site.register(User, UserAdmin)
admin.site.register(Organisation)
admin.site.register(Website)
admin.site.register(WebsiteGroup)
admin.site.register(Snapshot)
admin.site.register(Task)

# @admin.register(Website)
# class WebsiteAdmin(admin.ModelAdmin):
#     list_display = ("name", "url", "enabled")
#     list_filter = ("enabled", "scope_type")
#     search_fields = ("name", "url")
#     fieldsets = (
#         ("Basic info", {"fields": ("name", "url", "institution", "enabled")}),
#         ("Crawler Parameters (optional overrides)", {
#             "fields": (
#                 "scope_type",
#                 "generate_cdx",
#                 "workers",
#                 "page_load_timeout",
#                 "disk_utilization",
#                 "time_limit",
#             )
#         }),
#     )
#     actions = [admin_start_crawl]
#
#
# @admin.register(WebsiteCrawlParameters)
# class WebsiteCrawlParametersAdmin(admin.ModelAdmin):
#     fieldsets = (
#         ("Default Browsertrix Parameters", {
#             "fields": (
#                 "scope_type",
#                 "generate_cdx",
#                 "workers",
#                 "page_load_timeout",
#                 "disk_utilization",
#                 "time_limit",
#             )
#         }),
#     )
