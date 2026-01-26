


import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Alert thresholds (any discount triggers alert)
    min_discount_percent: int = 0

    # Scheduler
    check_interval_hours: int = 1

    # Database
    database_url: str = "sqlite:///catfood.db"

    # Scraper settings
    request_delay_min: int = 2
    request_delay_max: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
