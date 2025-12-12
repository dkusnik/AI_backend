from rest_framework import serializers
from django.contrib.auth import get_user_model
from archiver.models import (
    Category, Questionnaire, Question, QuestionnaireResponse,
    Organisation, Questionnaire, Question,
    Website, WebsiteGroup, Snapshot,
    CrawlConfig, ScheduleConfig, CommentThread, Comment,
    Task, GlobalConfig, Tag
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
# Questionnaire + Question
# -------------------------
class QuestionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Question
        fields = ["id", "questionnaireId", "title", "body", "input", "values"]


class QuestionnaireSerializer(serializers.ModelSerializer):
    questions = QuestionSerializer(many=True, required=False)

    class Meta:
        model = Questionnaire
        fields = [
            "id", "isDeleted", "isPublished", "title",
            "header", "footer", "category", "questions"
        ]


# -------------------------
# Tag
# -------------------------
class TagSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tag
        fields = ["id", "name"]


# -------------------------
# Website & WebsiteGroup
# -------------------------
class WebsiteSerializer(serializers.ModelSerializer):
    tags = TagSerializer(many=True, required=False)

    class Meta:
        model = Website
        fields = [
            "id", "organisation_id", "url", "isDeleted", "name",
            "displayName", "shortDescription", "longDescription",
            "doCrawl", "suspendCrawlUntilTimestamp", "tags"
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


class CategorySerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)

    class Meta:
        model = Category
        fields = ["id", "name"]


class QuestionSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    questionnaireId = serializers.PrimaryKeyRelatedField(
        source="questionnaire", queryset=Questionnaire.objects.all(), write_only=True, required=False
    )
    questionnaire_id = serializers.IntegerField(read_only=True)
    input = serializers.ChoiceField(
        choices=[c[0] for c in Question.INPUT_TYPES]
    )
    values = serializers.CharField(allow_blank=True, allow_null=True, required=False)

    class Meta:
        model = Question
        fields = [
            "id",
            "questionnaireId",  # write only alias for creating via nested payload if desired
            "questionnaire_id",  # read-only
            "title",
            "body",
            "input",
            "values",
        ]

    def validate(self, attrs):
        # when creating/updating ensure values provided for select/radio/checkbox
        input_type = attrs.get("input", getattr(self.instance, "input", None))
        values = attrs.get("values", getattr(self.instance, "values", None))
        if input_type in ("radio", "checkbox", "select"):
            if not values:
                raise serializers.ValidationError({
                    "values": "This field is required for input types radio, checkbox and select."
                })
            # ensure at least 1 option
            opts = [v.strip() for v in values.split("\n") if v.strip()]
            if not opts:
                raise serializers.ValidationError({
                    "values": "At least one option must be provided (newline-separated)."
                })
        return attrs


class QuestionnaireSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    title = serializers.CharField(required=True)
    header = serializers.CharField(allow_blank=True, allow_null=True, required=False)
    footer = serializers.CharField(allow_blank=True, allow_null=True, required=False)
    category = CategorySerializer(required=False, allow_null=True)
    isDeleted = serializers.BooleanField(source="is_deleted", default=False)
    isPublished = serializers.BooleanField(source="is_published", default=False)
    questions = QuestionSerializer(many=True, read_only=True)

    class Meta:
        model = Questionnaire
        fields = [
            "id", "isDeleted", "isPublished", "title",
            "header", "footer", "category", "questions"
        ]

    def create(self, validated_data):
        # category may be nested or not provided
        cat_data = validated_data.pop("category", None)
        if cat_data:
            cat_name = cat_data.get("name")
            category, _ = Category.objects.get_or_create(name=cat_name)
            validated_data["category"] = category
        q = Questionnaire.objects.create(**validated_data)
        return q

    def update(self, instance, validated_data):
        cat_data = validated_data.pop("category", None)
        if cat_data:
            cat_name = cat_data.get("name")
            category, _ = Category.objects.get_or_create(name=cat_name)
            instance.category = category
        # map is_deleted/is_published
        instance.title = validated_data.get("title", instance.title)
        instance.header = validated_data.get("header", instance.header)
        instance.footer = validated_data.get("footer", instance.footer)
        instance.is_deleted = validated_data.get("is_deleted", instance.is_deleted)
        instance.is_published = validated_data.get("is_published", instance.is_published)
        instance.save()
        return instance


class QuestionnaireResponseSerializer(serializers.ModelSerializer):
    questionnaireId = serializers.PrimaryKeyRelatedField(
        source="questionnaire", queryset=Questionnaire.objects.all()
    )
    organisationId = serializers.IntegerField(source="organisation_id")
    response = serializers.ListField(child=serializers.CharField(), allow_empty=True)

    class Meta:
        model = QuestionnaireResponse
        fields = ["organisationId", "questionnaireId", "response", "updated_at"]

    def validate(self, attrs):
        questionnaire = attrs["questionnaire"]
        response_list = attrs.get("response", [])
        # get number of questions expected
        expected = questionnaire.questions.count()
        # It's acceptable that response list length <= expected for partial answers,
        # but if you require full answers uncomment the strict check below.
        if len(response_list) > expected:
            raise serializers.ValidationError({
                "response": f"Response length ({len(response_list)}) exceeds number of questions ({expected})."
            })
        return attrs

    def create(self, validated_data):
        # Upsert: unique (organisation_id, questionnaire)
        organisation_id = validated_data["organisation_id"]
        questionnaire = validated_data["questionnaire"]
        response = validated_data.get("response", [])
        obj, created = QuestionnaireResponse.objects.update_or_create(
            organisation_id=organisation_id,
            questionnaire=questionnaire,
            defaults={"response": response}
        )
        return obj

class CrawlConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = CrawlConfig
        fields = ["id", "parentId", "name", "description", "status", "crawlEngine", "yamlConfig"]


class ScheduleConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ScheduleConfig
        fields = ["id", "parentId", "name", "description", "status", "yamlConfig"]


class CommentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Comment
        fields = ["id", "threadId", "createdAt", "userId", "body", "entryClass", "entryLevel"]


class CommentThreadSerializer(serializers.ModelSerializer):
    class Meta:
        model = CommentThread
        fields = ["id", "accessMode", "entityType", "entityId", "messages", "lastTimestamp", "lastAuthor", "lastMessage", "lastType"]


class GlobalConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = GlobalConfig
        fields = ["key", "value"]

