from rest_framework import serializers
from django.contrib.auth import get_user_model
from archiver.models import (
    Organisation,
    Website, WebsiteGroup, Snapshot,
    CrawlConfig, ScheduleConfig,
    Task, GlobalConfig
)

User = get_user_model()


# -------------------------
# User
# -------------------------
class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True, required=False)

    class Meta:
        model = User
        fields = [
            "id", "groupId", "organisationId", "username",
            "first_name", "last_name", "email", "phone",
            "emailActivated", "password", "status"
        ]
        read_only_fields = ("id",)

    def update(self, instance, validated_data):
        pwd = validated_data.pop("password", None)
        for k, v in validated_data.items():
            # map firstName/lastName if provided
            if k == "firstName":
                instance.first_name = v
            elif k == "lastName":
                instance.last_name = v
            else:
                setattr(instance, k, v)
        if pwd:
            instance.set_password(pwd)
        instance.save()
        return instance


# -------------------------
# Organisation
# -------------------------
class OrganisationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Organisation
        fields = ["id", "parentId", "name"]

# -------------------------
# Website & WebsiteGroup
# -------------------------
class WebsiteSerializer(serializers.ModelSerializer):

    class Meta:
        model = Website
        fields = [
            "id", "organisation_id", "url", "isDeleted", "name",
            "displayName", "shortDescription", "longDescription",
            "doCrawl", "suspendCrawlUntilTimestamp",
        ]


class WebsiteGroupSerializer(serializers.ModelSerializer):
    websites = serializers.PrimaryKeyRelatedField(many=True, queryset=Website.objects.all())

    class Meta:
        model = WebsiteGroup
        fields = ["id", "name", "description", "websites"]


# -------------------------
# Snapshot
# -------------------------
class SnapshotSerializer(serializers.ModelSerializer):
    class Meta:
        model = Snapshot
        fields = [
            "id", "website_id", "url", "isDeleted",
            "status", "publicationStatus", "crawlStartTimestamp",
            "crawlStopTimestamp", "size", "itemCount", "warcPath",
            "replayCollectionId",
        ]


# -------------------------
# Task (accepts TaskParameters)
# -------------------------
class TaskParametersSerializer(serializers.Serializer):
    website = serializers.IntegerField(required=False)
    groupId = serializers.IntegerField(required=False)
    snapshotId = serializers.IntegerField(required=False)
    scheduleId = serializers.IntegerField(required=False)
    crawlConfigId = serializers.IntegerField(required=False)


class TaskSerializer(serializers.ModelSerializer):
    taskParameters = TaskParametersSerializer(required=False)

    class Meta:
        model = Task
        fields = [
            "action", "uid", "user", "scheduleTime", "startTime", "updateTime",
            "updateMessage", "finishTime", "status", "result",
            "resultDescription", "runData", "priority", "schedule",
            "taskParameters", "taskResponse"
        ]
        read_only_fields = ("status", "result", "runData", "taskResponse")


class CrawlConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = CrawlConfig
        fields = ["id", "parentId", "name", "description", "status", "crawlEngine", "yamlConfig"]


class ScheduleConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ScheduleConfig
        fields = ["id", "parentId", "name", "description", "status", "yamlConfig"]


class GlobalConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = GlobalConfig
        fields = ["key", "value"]

