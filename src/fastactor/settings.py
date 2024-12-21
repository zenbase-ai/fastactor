from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FASTACTOR_")

    mailbox_size: int = 1024


settings = Settings()
