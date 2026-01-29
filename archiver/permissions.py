# archiver/permissions.py
from rest_framework import permissions


# Helper: check if any required scope present inside token claims or user.is_staff
def _has_scope(request, scope_name):
    # Customize this to your auth backend. Typical places:
    # - request.auth.get("scope")  (a space-separated string)
    # - request.auth.get("scope", "").split()
    # - request.user.is_staff as fallback for admin
    auth = getattr(request, "auth", None)
    if auth:
        try:
            scopes = auth.get("scope", "") or auth.get("scp", "")
            if isinstance(scopes, str):
                scopes_list = scopes.split()
            else:
                scopes_list = list(scopes)
            return scope_name in scopes_list
        except Exception:
            pass
    # fallback
    if scope_name == "admin" and request.user and request.user.is_superuser:
        return True
    if scope_name == "archivist" and request.user and request.user.is_staff:
        return True
    return False


class HasScope(permissions.BasePermission):
    """
    Usage: permission_classes = [HasScope('archivist')]
    """
    def __init__(self, scope):
        self.scope = scope

    def has_permission(self, request, view):
        return _has_scope(request, self.scope)


# Prebuilt permission classes
class IsArchivist(HasScope):
    def __init__(self):
        super().__init__("archivist")


class IsModerator(HasScope):
    def __init__(self):
        super().__init__("moderator")


class IsAdmin(HasScope):
    def __init__(self):
        super().__init__("admin")


class IsClient(HasScope):
    def __init__(self):
        super().__init__("client")


class IsAPIClient(HasScope):
    def __init__(self):
        super().__init__("api")
