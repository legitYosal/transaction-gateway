# services/gateway/settings.py

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GATEWAY_",
        extra="ignore",
    )

    database_url: str
    ledger_service_url: str


@lru_cache
def get_settings() -> GatewaySettings:
    return GatewaySettings()
