import json
import logging

from django.http import HttpResponseNotAllowed, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.csrf import csrf_exempt

from archiver.models import Snapshot, Website


def snapshot_list(request):
    snapshots = Snapshot.objects.order_by("-created_at")

    return render(
        request,
        "snapshots/list.html",
        {"snapshots": snapshots}
    )


def snapshot_detail(request, snapshot_id: int):
    snapshot = get_object_or_404(Snapshot, id=snapshot_id)

    return render(
        request,
        "snapshots/detail.html",
        {
            "snapshot": snapshot,
            "crawl": snapshot.crawl_stats or {},
            "process": snapshot.process_stats or {},
        }
    )


def snapshot_stats_partial(request, snapshot_id: int):
    snapshot = get_object_or_404(Snapshot, id=snapshot_id)

    return render(
        request,
        "snapshots/_stats.html",
        {
            "snapshot": snapshot,
            "crawl": snapshot.crawl_stats or {},
            "process": snapshot.process_stats or {},
        }
    )


#  FOR DEBUG ONLY


@csrf_exempt
def dummy_put_collector(request):
    """
    Dummy endpoint to collect PUT requests and log payload.
    """
    logger = logging.getLogger("dummy.put.collector")
    if request.method == "GET":
        return JsonResponse({}, status=200)

    if request.method != "PUT":
        return HttpResponseNotAllowed(["PUT"])

    try:
        body = json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning("PUT_collector received invalid JSON")
        return JsonResponse(
            {"error": "Invalid JSON"},
            status=400,
        )

    logger.info(
        f"PUT_collector received payload - {body}",
        extra={
            "path": request.path,
            "headers": dict(request.headers),
            "remote_addr": request.META.get("REMOTE_ADDR"),
            "body": body,
        },
    )

    return JsonResponse(
        {
            "status": "ok",
            "received": True,
        },
        status=200,
    )

def seed_list(request):
    tag = request.GET.get('tag')
    if tag:
        websites = Website.objects.filter(snapshots__published=True).filter(
            tags__name__icontains=tag).prefetch_related('tags').distinct()
    else:
        websites = Website.objects.filter(snapshots__published=True).prefetch_related('tags').distinct()
    data = [
        {
            "title": website.name,
            "url": website.url,
            "tags": ','.join([tag.name for tag in website.tags.all()])
        }
        for website in websites
    ]
    return JsonResponse(data, safe=False)