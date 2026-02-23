from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from dotenv import load_dotenv


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


@dataclass(frozen=True)
class DBSettings:
    host: str
    port: int
    user: str
    password: str
    dbname: str
    sslmode: str = "prefer"

    def dsn_kwargs(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.dbname,
            "sslmode": self.sslmode,
        }


@dataclass(frozen=True)
class ScraperSettings:
    base_url: str = "https://www.work.ua/"
    category_city_path: str = "resumes-kyiv-auto-transport/"
    request_timeout: int = 10
    max_retries: int = 3
    retry_backoff_sec: int = 2
    sleep_between_pages: float = 2.0
    sleep_between_resumes: float = 1.0
    user_agent: str = "workua-resume-toolkit/0.1"

    @property
    def category_url(self) -> str:
        return self.base_url.rstrip('/') + '/' + self.category_city_path.lstrip('/')

    def validate(self) -> None:
        if not self.base_url.startswith("http"):
            raise ValueError("ScraperSettings.base_url must start with http/https")
        if not self.category_city_path:
            raise ValueError("ScraperSettings.category_city_path must be non-empty")


@dataclass(frozen=True)
class Settings:
    db: DBSettings
    scraper: ScraperSettings


def load_default_scraper_config() -> Dict[str, Any]:
    here = Path(__file__).resolve().parent
    cfg_path = here / "scraper_config.json"
    if cfg_path.exists():
        return json.loads(cfg_path.read_text(encoding="utf-8"))
    return {}


def load_settings() -> Settings:

    load_dotenv(override=False)  # no error if .env is missing

    # DB
    db = DBSettings(
        host=_env("DB_HOST", "localhost") or "localhost",
        port=int(_env("DB_PORT", "5432") or "5432"),
        user=_env("DB_USER", "postgres") or "postgres",
        password=_env("DB_PASSWORD", "") or "",
        dbname=_env("DB_NAME", "resume_search_mvp") or "resume_search_mvp",
        sslmode=_env("DB_SSLMODE", "prefer") or "prefer",
    )

    # Scraper: defaults + JSON example overrides + ENV overrides
    cfg = load_default_scraper_config()

    scraper = ScraperSettings(
        base_url=str(cfg.get("base_url") or "https://www.work.ua/"),
        category_city_path=str(cfg.get("category_city_path") or "resumes-kyiv-auto-transport/"),
        request_timeout=int(cfg.get("request_timeout") or 10),
        max_retries=int(cfg.get("max_retries") or 3),
        retry_backoff_sec=int(cfg.get("retry_backoff_sec") or 2),
        sleep_between_pages=float(cfg.get("sleep_between_pages") or 2.0),
        sleep_between_resumes=float(cfg.get("sleep_between_resumes") or 1.0),
        user_agent=_env("WORKUA_USER_AGENT", str(cfg.get("user_agent") or "workua-resume-toolkit/0.1")) or "workua-resume-toolkit/0.1",
    )

    # ENV overrides
    env_cat = _env("WORKUA_CATEGORY_CITY_PATH", None)
    if env_cat:
        scraper = ScraperSettings(**{**scraper.__dict__, "category_city_path": env_cat})

    scraper.validate()
    return Settings(db=db, scraper=scraper)
