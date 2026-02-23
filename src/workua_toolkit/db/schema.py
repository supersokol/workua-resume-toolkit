from __future__ import annotations

import logging
logger = logging.getLogger(__name__)


from typing import Any, Dict
import psycopg2


DDL = """
CREATE TABLE IF NOT EXISTS resumes (
  id BIGSERIAL PRIMARY KEY,
  source_url TEXT NOT NULL UNIQUE,
  resume_date DATE NULL,
  raw_html TEXT NULL,
  cleaned_text TEXT NULL,
  parse_mode TEXT NULL,
  warnings JSONB NULL,
  parsed_json JSONB NULL,
  processed_json JSONB NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_resumes_resume_date ON resumes(resume_date DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_resumes_updated_at ON resumes(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_resumes_parsed_gin ON resumes USING GIN (parsed_json);
CREATE INDEX IF NOT EXISTS idx_resumes_processed_gin ON resumes USING GIN (processed_json);
"""


def migrate(conn_kwargs: Dict[str, Any]) -> None:
    logger.info("DB migrate: start (host=%s db=%s)", conn_kwargs.get("host"), conn_kwargs.get("dbname"))
    with psycopg2.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    logger.info("DB migrate: done")
