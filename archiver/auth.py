# archiver/auth.py
from mozilla_django_oidc.auth import OIDCAuthenticationBackend
from django.contrib.auth.models import User
from archiver.models import UserProfile, Organisation


class OIDCKeycloakBackend(OIDCAuthenticationBackend):

    def create_user(self, claims):
        """Tworzy nowego użytkownika Django na podstawie claimów z Keycloak."""

        user = User.objects.create_user(
            username=claims.get("preferred_username"),
            email=claims.get("email", ""),
            first_name=claims.get("given_name", ""),
            last_name=claims.get("family_name", ""),
        )
        user.save()

        # user profile + organisation mapping
        self.update_user_profile(user, claims)

        return user

    def update_user(self, user, claims):
        """Aktualizuje użytkownika Django po każdym logowaniu."""
        changed = False

        # aktualizacja standardowych pól Django
        mappings = {
            "email": "email",
            "first_name": "given_name",
            "last_name": "family_name",
        }
        for django_field, claim_field in mappings.items():
            new_value = claims.get(claim_field, "")
            if new_value and getattr(user, django_field) != new_value:
                setattr(user, django_field, new_value)
                changed = True

        if changed:
            user.save()

        # aktualizacja profilu/organizacji
        self.update_user_profile(user, claims)

        return user

    def update_user_profile(self, user, claims):
        """Mapowanie dodatkowych danych z Keycloak do UserProfile."""
        profile, _ = UserProfile.objects.get_or_create(user=user)

        organisation_id = claims.get("organisationId")
        if organisation_id:
            try:
                org = Organisation.objects.get(id=organisation_id)
                profile.organisation = org
            except Organisation.DoesNotExist:
                pass  # ignorujemy brak zgodności

        profile.save()

    #
    # Kluczowe: identyfikator użytkownika z Keycloak
    #
    def get_username(self, claims):
        """Używamy preferred_username jako username Django."""
        return claims.get("preferred_username")
