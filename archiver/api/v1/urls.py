from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import (
                    CrawlConfigViewSet, GlobalConfigViewSet,
                    OrganisationViewSet,
                    ScheduleConfigViewSet, SnapshotViewSet,
                    TaskCancelView, TaskCreateView, TaskDetailView,
                    WebsiteGroupViewSet, WebsiteViewSet)

router = DefaultRouter()
router.register(r"organisation", OrganisationViewSet, basename="organisation")
router.register(r"website", WebsiteViewSet, basename="website")
router.register(r"website-group", WebsiteGroupViewSet, basename="websitegroup")
router.register(r"snapshot", SnapshotViewSet, basename="snapshot")
router.register(r"crawl", CrawlConfigViewSet, basename="crawl")
router.register(r"schedule", ScheduleConfigViewSet, basename="schedule")
router.register(r"global", GlobalConfigViewSet, basename="global")
#router.register(r"statistic", StatisticViewSet, basename="statistic")

urlpatterns = [
    path("", include(router.urls)),
    #path("user/", UserOwnView.as_view(), name="user-own"),
    path("task/", TaskCreateView.as_view(), name="task-create"),
    path("task/<str:uid>/", TaskDetailView.as_view(), name="task-detail"),
    path("task/<str:uid>/cancel/", TaskCancelView.as_view(), name="task-cancel"),
]
