from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

from .admin_actions import admin_start_crawl
from .models import (Category, Organisation, Question, Questionnaire,
                     QuestionnaireResponse, Snapshot, Task, User, Website,
                     WebsiteCrawlParameters, WebsiteGroup)

#admin.site.register(User, UserAdmin)
admin.site.register(Organisation)
admin.site.register(Website)
admin.site.register(WebsiteGroup)
admin.site.register(Snapshot)
admin.site.register(Task)

class QuestionInline(admin.TabularInline):
    model = Question
    extra = 0
    fields = ("title", "input", "values", "body")
    readonly_fields = ()

@admin.register(Questionnaire)
class QuestionnaireAdmin(admin.ModelAdmin):
    list_display = ("id", "title", "is_published", "is_deleted")
    list_filter = ("is_published", "is_deleted")
    search_fields = ("title",)
    inlines = [QuestionInline]

@admin.register(Question)
class QuestionAdmin(admin.ModelAdmin):
    list_display = ("id", "questionnaire", "title", "input")
    list_filter = ("input",)
    search_fields = ("title",)

@admin.register(QuestionnaireResponse)
class QuestionnaireResponseAdmin(admin.ModelAdmin):
    list_display = ("id", "organisation_id", "questionnaire", "updated_at")
    search_fields = ("organisation_id",)
    readonly_fields = ("updated_at",)

@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ("id", "name")

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
