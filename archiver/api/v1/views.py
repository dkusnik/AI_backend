# archiver/views.py
from django.shortcuts import get_object_or_404
from rest_framework import filters, status, viewsets
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response

from archiver.models import (CrawlConfig,
                             GlobalConfig, Organisation,
                             ScheduleConfig, Snapshot, Task,
                             Website, WebsiteGroup)

from archiver.permissions import IsAdmin, IsAPIClient, IsArchivist, IsModerator
from .serializers import (CrawlConfigSerializer,
                          GlobalConfigSerializer, OrganisationSerializer,
                          ScheduleConfigSerializer, SnapshotSerializer,
                          TaskSerializer,
                          WebsiteGroupSerializer, WebsiteSerializer)
# For tasks
from archiver.services.crawl_manager import (resume_crawl, queue_crawl, stop_crawl,
                                             suspend_crawl)


# --------------------------
# Organisation
# --------------------------
class OrganisationViewSet(viewsets.ModelViewSet):
    queryset = Organisation.objects.all().order_by("id")
    serializer_class = OrganisationSerializer
    permission_classes = [IsAuthenticated]  # further check with scopes as needed


# --------------------------
# Website
# --------------------------
class WebsiteViewSet(viewsets.ModelViewSet):
    queryset = Website.objects.all().order_by("id")
    serializer_class = WebsiteSerializer
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=["post"], permission_classes=[IsArchivist])
    def start_crawl(self, request, pk=None):
        site = self.get_object()
        job_id = queue_crawl(site.id)
        return Response({"job_id": job_id}, status=status.HTTP_202_ACCEPTED)


# --------------------------
# WebsiteGroup
# --------------------------
class WebsiteGroupViewSet(viewsets.ModelViewSet):
    queryset = WebsiteGroup.objects.all().order_by("id")
    serializer_class = WebsiteGroupSerializer
    permission_classes = [IsAuthenticated]


# --------------------------
# Snapshot
# --------------------------
class SnapshotViewSet(viewsets.ModelViewSet):
    queryset = Snapshot.objects.all().order_by("-crawlStartTimestamp")
    serializer_class = SnapshotSerializer
    permission_classes = [IsAuthenticated]


# --------------------------
# CrawlConfig
# --------------------------
class CrawlConfigViewSet(viewsets.ModelViewSet):
    queryset = CrawlConfig.objects.all().order_by("id")
    serializer_class = CrawlConfigSerializer
    permission_classes = [IsArchivist]


# --------------------------
# ScheduleConfig
# --------------------------
class ScheduleConfigViewSet(viewsets.ModelViewSet):
    queryset = ScheduleConfig.objects.all().order_by("id")
    serializer_class = ScheduleConfigSerializer
    permission_classes = [IsArchivist]

# --------------------------
# GlobalConfig
# --------------------------
class GlobalConfigViewSet(viewsets.ModelViewSet):
    queryset = GlobalConfig.objects.all().order_by("key")
    serializer_class = GlobalConfigSerializer
    permission_classes = [IsArchivist]


# --------------------------
# Statistic
# --------------------------
# class StatisticViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = Statistic.objects.all().order_by("key")
#     serializer_class = StatisticSerializer
#     permission_classes = [AllowAny]


# --------------------------
# Task endpoints - central
# --------------------------
from rest_framework.views import APIView


class TaskCreateView(APIView):
    permission_classes = [IsArchivist]

    def post(self, request):
        serializer = TaskSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        action = data.get("action")
        params = data.get("taskParameters") or {}

        # Map crawl actions explicitly
        if action == "crawl_run":
            website_id = params.get("websiteId")
            if not website_id:
                return Response({"error": "websiteId required"}, status=400)
            job_uuid = queue_crawl(website_id)
            task_obj = Task.objects.create(action=action, uid=job_uuid, user=request.user.username, status="scheduled", taskParameters=params)
            headers = {"X-Task-Operation-Status": "created"}
            return Response(TaskSerializer(task_obj).data, status=200, headers=headers)

        if action in ("crawl_stop", "crawl_suspend", "crawl_resume"):
            identifier = params.get("websiteId") or data.get("uid")
            if not identifier:
                return Response({"error": "websiteId or uid required"}, status=400)
            if action == "crawl_stop":
                ok = stop_crawl(str(identifier))
                status_text = "cancelled" if ok else "failed"
            elif action == "crawl_suspend":
                ok = suspend_crawl(str(identifier))
                status_text = "suspended" if ok else "failed"
            else:
                ok = resume_crawl(str(identifier))
                status_text = "running" if ok else "failed"
            t = Task.objects.create(action=action, uid=str(identifier) or "", user=request.user.username, status="running" if ok else "failed", taskParameters=params)
            headers = {"X-Task-Operation-Status": "updated" if ok else "conflicting-removed"}
            return Response(TaskSerializer(t).data, status=200 if ok else 409, headers=headers)

        # For other actions just create Task record (scheduler/cluster will pick)
        t = Task.objects.create(action=action, uid=data.get("uid") or "", user=request.user.username, status="scheduled", taskParameters=params)
        headers = {"X-Task-Operation-Status": "created"}
        return Response(TaskSerializer(t).data, status=200, headers=headers)


class TaskDetailView(APIView):
    permission_classes = [IsArchivist]
    def get(self, request, uid):
        t = get_object_or_404(Task, uid=uid)
        headers = {"X-Task-Status": t.status}
        return Response(TaskSerializer(t).data, headers=headers)

    def put(self, request, uid):
        t = get_object_or_404(Task, uid=uid)
        serializer = TaskSerializer(t, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        headers = {"X-Task-Operation-Status": "updated"}
        return Response(serializer.data, headers=headers)

class TaskCancelView(APIView):
    permission_classes = [IsModerator]
    def post(self, request, uid):
        t = get_object_or_404(Task, uid=uid)
        if t.status in ("running", "scheduled"):
            t.status = "cancelled"
            t.save(update_fields=["status"])
            return Response(TaskSerializer(t).data)
        return Response({"detail":"Cannot cancel"}, status=409)

