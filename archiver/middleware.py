import jwt
from django.utils.deprecation import MiddlewareMixin
from django.contrib.auth.models import User, Group

class OAuth2RolesMiddleware(MiddlewareMixin):
    def process_request(self, request):
        auth = request.META.get("HTTP_AUTHORIZATION")
        if not auth or not auth.startswith("Bearer "):
            return

        token = auth.split(" ", 1)[1]
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
            roles = payload.get("realm_access", {}).get("roles", [])
            username = payload.get("preferred_username", None)
        except Exception:
            return

        # auto-create service users
        if username:
            user, _ = User.objects.get_or_create(username=username, defaults={"is_active": True})
            # assign groups dynamically but not persist
            user._tmp_roles = roles
            request.user = user
