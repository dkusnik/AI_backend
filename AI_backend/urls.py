"""
URL configuration for AI_backend project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView
from archiver.views import snapshot_list, snapshot_detail, snapshot_stats_partial, dummy_put_collector

urlpatterns = [
    path("", snapshot_list, name="snapshot_list"),
    path("snapshots/<int:snapshot_id>/", snapshot_detail, name="snapshot_detail"),
    path("snapshots/<int:snapshot_id>/stats/", snapshot_stats_partial, name="snapshot_stats_partial"),
    path('admin/', admin.site.urls),
    path('django-rq/', include('django_rq.urls')),  # worker monitoring UI (optional)
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path("api/docs/", SpectacularSwaggerView.as_view(url_name="schema")),
    path("api/docs/redoc/", SpectacularRedocView.as_view(url_name="schema"), name="redoc"),

    path("dummy/PUT_collector", dummy_put_collector, name="dummy_put_collector"),
    path("task/api/", dummy_put_collector, name="dummy_put_collector"),
    path("snapshot/", dummy_put_collector, name="dummy_put_collector"),
    path("snapshot/<snapshot_id>/", dummy_put_collector, name="dummy_put_collector"),
]
