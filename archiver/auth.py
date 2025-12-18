import requests
import time
import requests
from django.conf import settings
from django.core.cache import cache


TOKEN_CACHE_KEY = "keycloak:cluster-service:access-token"
TOKEN_EXP_CACHE_KEY = "keycloak:cluster-service:access-token-exp"


def get_keycloak_access_token() -> str:
    """
    Get cached OAuth token for cluster-service.
    Refresh automatically if expired or missing.
    """

    token = cache.get(TOKEN_CACHE_KEY)
    expires_at = cache.get(TOKEN_EXP_CACHE_KEY)

    now = int(time.time())

    # ----------------------------
    # Return cached token if valid
    # ----------------------------
    if token and expires_at and now < expires_at:
        return token

    # ----------------------------
    # Fetch new token from Keycloak
    # ----------------------------
    response = requests.post(
        settings.KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.KEYCLOAK_CLIENT_ID,
            "client_secret": settings.KEYCLOAK_CLIENT_SECRET,
        },
        timeout=10,
    )

    response.raise_for_status()

    data = response.json()

    token = data["access_token"]
    expires_in = data.get("expires_in", settings.KEYCLOAK_TOKEN_EXPIRES)

    # Refresh token a bit early (safety margin)
    expires_at = now + expires_in - 30

    cache.set(TOKEN_CACHE_KEY, token, timeout=expires_in)
    cache.set(TOKEN_EXP_CACHE_KEY, expires_at, timeout=expires_in)

    return token