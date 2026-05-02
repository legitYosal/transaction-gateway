from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class LedgerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LEDGER_",
        extra="ignore",
    )

    database_url: str
    logging_level: str
    enable_docs: bool


@lru_cache
def get_settings() -> LedgerSettings:
    return LedgerSettings()
