"""Production startup must fail closed on unsafe configuration."""

import pytest
from cryptography.fernet import Fernet

from config import (
    ProductionConfigurationError,
    Settings,
    validate_production_settings,
)

pytestmark = pytest.mark.unit


def _settings(**overrides) -> Settings:
    values = {
        "mongodb_url": "mongodb://database.internal:27017",
        "apify_api_key": "apify-secret",
        "jwt_secret_key": "j" * 48,
        "app_env": "production",
        "app_role": "web",
        "debug": False,
        "discovery_mock_mode": False,
        "openrouter_api_key": "openrouter-secret",
        "growthtoolkit_api_key": "growth-secret",
        "unipile_token": "unipile-secret",
        "unipile_webhook_secret": "webhook-secret",
        "google_client_id": "google-client",
        "google_client_secret": "google-secret",
        "google_redirect_uri": "https://app.outflo.example/api/auth/google/callback",
        "encryption_key": Fernet.generate_key().decode("utf-8"),
        "frontend_url": "https://app.outflo.example",
        "backend_base_url": "https://api.outflo.example",
        "api_base_url": "https://api.outflo.example",
        "cors_origins": "https://app.outflo.example",
    }
    values.update(overrides)
    return Settings.model_construct(**values)


def test_valid_production_configuration_passes():
    validate_production_settings(_settings())


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"jwt_secret_key": "short"}, "JWT_SECRET_KEY"),
        ({"encryption_key": "not-fernet"}, "ENCRYPTION_KEY"),
        ({"unipile_webhook_secret": ""}, "UNIPILE_WEBHOOK_SECRET"),
        ({"debug": True}, "DEBUG"),
        ({"app_role": "all"}, "APP_ROLE"),
        ({"cors_origins": "*"}, "CORS_ORIGINS"),
        ({"google_redirect_uri": "http://app.test/callback"}, "GOOGLE_REDIRECT_URI"),
        ({"enrichment_startup_sweep_enabled": True}, "ENRICHMENT_STARTUP_SWEEP_ENABLED"),
    ],
)
def test_unsafe_production_configuration_is_rejected(overrides, expected):
    with pytest.raises(ProductionConfigurationError, match=expected):
        validate_production_settings(_settings(**overrides))


def test_development_configuration_is_not_forced_to_have_provider_secrets():
    validate_production_settings(
        Settings.model_construct(
            app_env="development",
            mongodb_url="mongodb://127.0.0.1:27017",
            apify_api_key="",
            jwt_secret_key="dev",
        )
    )
