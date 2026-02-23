from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import json
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras


class PostgresRepo:
    def __init__(self, conn_kwargs: Dict[str, Any]):
        self.conn_kwargs = conn_kwargs

    def _conn(self):
        return psycopg2.connect(**self.conn_kwargs)

    def drop_tables(self) -> None:
        logger.warning("DB drop_tables: dropping resumes table")
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS resumes CASCADE;")
            conn.commit()
        logger.info("DB drop_tables: done")

    def upsert_payload(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[int]]:
        """Insert or update a resume row by source_url.

        Returns (inserted, id).
        """
        source_url = payload.get("source_url")
        if not source_url:
            raise ValueError("payload.source_url is required")

        logger.info(
            "DB upsert: start (url=%s has_cleaned=%s has_parsed=%s)",
            source_url,
            bool(payload.get("cleaned_text")),
            isinstance(payload.get("parsed"), dict),
        )

        parsed = payload.get("parsed")
        meta = payload.get("meta") or {}
        warnings = meta.get("warnings")
        parse_mode = meta.get("parse_mode")

        # allow top-level override for raw/cleaned names
        raw_html = payload.get("raw_html")
        cleaned_text = payload.get("cleaned_text")

        resume_date = None
        if isinstance(parsed, dict):
            resume_date = parsed.get("resume_date")

        sql = """
        INSERT INTO resumes(source_url, resume_date, raw_html, cleaned_text, parse_mode, warnings, parsed_json, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
        ON CONFLICT (source_url) DO UPDATE SET
          resume_date = EXCLUDED.resume_date,
          raw_html = EXCLUDED.raw_html,
          cleaned_text = EXCLUDED.cleaned_text,
          parse_mode = EXCLUDED.parse_mode,
          warnings = EXCLUDED.warnings,
          parsed_json = EXCLUDED.parsed_json,
          updated_at = NOW()
        RETURNING id, (xmax = 0) AS inserted;
        """

        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        source_url,
                        resume_date,
                        raw_html,
                        cleaned_text,
                        parse_mode,
                        json.dumps(warnings) if warnings is not None else None,
                        json.dumps(parsed) if parsed is not None else None,
                    ),
                )
                row = cur.fetchone()
            conn.commit()

        if not row:
            logger.warning("DB upsert: no row returned (url=%s)", source_url)
            return False, None
        rid, inserted = int(row[0]), bool(row[1])
        logger.debug("DB upsert: done (url=%s id=%s inserted=%s)", source_url, rid, inserted)
        return inserted, rid

    def set_processed_json(self, source_url: str, processed: Dict[str, Any]) -> None:
        logger.info("DB set_processed_json: start (url=%s)", source_url)
        sql = """
        UPDATE resumes
        SET processed_json = %s::jsonb,
            updated_at = NOW()
        WHERE source_url = %s
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (json.dumps(processed), source_url))
            conn.commit()
            
        logger.debug("DB set_processed_json: done (url=%s, keys=%s)", source_url, list(processed.keys())[:20])
    def get_stats(self) -> Dict[str, Any]:
        sql = """
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE processed_json IS NOT NULL) AS processed,
          COUNT(*) FILTER (WHERE parsed_json IS NOT NULL) AS parsed,
          MIN(created_at) AS min_created,
          MAX(updated_at) AS max_updated
        FROM resumes
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                row = cur.fetchone() or {}
        return dict(row)

    def list_resumes(self, page: int = 1, page_size: int = 25) -> List[Dict[str, Any]]:
        page = max(1, int(page))
        page_size = max(1, min(200, int(page_size)))
        offset = (page - 1) * page_size

        sql = """
        SELECT id, source_url, resume_date, parse_mode, created_at, updated_at,
               parsed_json, processed_json
        FROM resumes
        ORDER BY updated_at DESC
        OFFSET %s LIMIT %s
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (offset, page_size))
                rows = cur.fetchall() or []
        return [dict(r) for r in rows]

    def get_resume(self, rid: int) -> Optional[Dict[str, Any]]:
        sql = """
        SELECT *
        FROM resumes
        WHERE id = %s
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (int(rid),))
                row = cur.fetchone()
        return dict(row) if row else None
