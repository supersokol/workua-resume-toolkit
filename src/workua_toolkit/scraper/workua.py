from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import datetime as dt
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from workua_toolkit.config.settings import ScraperSettings
from .parsing import (
    extract_resume_id_from_url,
    parse_is_veteran_from_raw_html,
    html_to_text_keep_breaks,
    normalize_ws,
    parse_name_position_from_cleaned,
    parse_resume_date_from_cleaned,
    parse_employment_from_cleaned,
    parse_salary_from_cleaned,
    strip_salary_tail_from_position,
    extract_section_text_by_title,
    parse_bullets,
    clean_person_name,
    parse_city_from_cleaned,
    parse_ready_to_work_from_cleaned,
    parse_considered_positions_from_cleaned,
    parse_disability_from_cleaned,
)




class PayloadMode(str, Enum):
    RAW = "raw"
    RAW_CLEANED = "raw_cleaned"
    RAW_CLEANED_PARSED = "raw_cleaned_parsed"


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@dataclass(frozen=True)
class PayloadMeta:
    schema_version: str = "resume_payload_v1"


class WorkUAScraper:
    """Work.ua resume scraper that yields JSON payloads.

    Design goals:
    - No DB logic
    - Generator-based API (stream payloads)
    - Best-effort extraction with explicit payload modes
    """

    def __init__(self, settings: ScraperSettings):
        self.settings = settings
        self.base_url = settings.base_url.rstrip("/") + "/"
        self.category_city_path = settings.category_city_path.lstrip("/")
        self.request_timeout = settings.request_timeout

        self.stats = {
            "pages_visited": 0,
            "urls_found": 0,
            "payloads_yielded": 0,
            "payloads_failed": 0,
            "missing_main_block": 0,
        }

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

        retry = Retry(
            total=settings.max_retries,
            connect=settings.max_retries,
            read=settings.max_retries,
            backoff_factor=settings.retry_backoff_sec,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        logger.info(
            "WorkUAScraper initialized (base_url=%s, category_city_path=%s, timeout=%ss, retries=%s, sleep_pages=%ss, sleep_resumes=%ss)",
            self.base_url,
            self.category_city_path,
            self.request_timeout,
            settings.max_retries,
            settings.sleep_between_pages,
            settings.sleep_between_resumes,
        )

    # -------------------------
    # Public streaming APIs
    # -------------------------
    def iter_resumes_by_pages(
        self,
        page_from: int,
        page_to: int,
        *,
        payload_mode: PayloadMode = PayloadMode.RAW_CLEANED_PARSED,
        skip_first: int = 0,
        limit: Optional[int] = None,
        dedupe: bool = True,
        sleep_between_pages: Optional[float] = None,
        sleep_between_resumes: Optional[float] = None,
    ) -> Iterator[Dict[str, Any]]:
        urls = self._iter_resume_urls_from_pages(
            page_from,
            page_to,
            sleep_between_pages=sleep_between_pages,
        )
        logger.info(
            "Scrape pages range start (page_from=%s, page_to=%s, mode=%s, skip_first=%s, limit=%s, dedupe=%s)",
            page_from, page_to, payload_mode, skip_first, limit, dedupe
        )
        yielded0 = self.stats.get("payloads_yielded", 0)
        yield from self._iter_payloads_from_urls(
            urls,
            payload_mode=payload_mode,
            skip_first=skip_first,
            limit=limit,
            dedupe=dedupe,
            sleep_between_resumes=sleep_between_resumes,
        )
        yielded = self.stats.get("payloads_yielded", 0) - yielded0
      
        logger.info(
            "Scrape pages range done (yielded=%s, stats=%s)",
            yielded, self.stats
        )

    def iter_resumes_until(
        self,
        *,
        target_n: int,
        start_page: int = 1,
        skip_first: int = 0,
        max_pages: Optional[int] = None,
        payload_mode: PayloadMode = PayloadMode.RAW_CLEANED_PARSED,
        dedupe: bool = True,
        sleep_between_pages: Optional[float] = None,
        sleep_between_resumes: Optional[float] = None,
    ) -> Iterator[Dict[str, Any]]:
        if target_n <= 0:
            return

        logger.info(
            "Scrape until start (target_n=%s, start_page=%s, skip_first=%s, max_pages=%s, mode=%s, dedupe=%s)",
            target_n, start_page, skip_first, max_pages, payload_mode, dedupe
        )

        page = max(1, start_page)
        yielded = 0
        seen: Set[str] = set()
        skipped = 0

        while True:
            if max_pages is not None and (page - start_page) >= max_pages:
                logger.warning("Scrape until stopped by max_pages=%s (page=%s, yielded=%s)", max_pages, page, yielded)
                break

            page_urls = list(self._iter_resume_urls_from_pages(page, page, sleep_between_pages=sleep_between_pages))
            if not page_urls:
                logger.info("Scrape until stopped: no more URLs (page=%s, yielded=%s)", page, yielded)
                break

            logger.debug("Page %s: found %s resume URLs", page, len(page_urls))

            for u in page_urls:
                u = self._normalize_resume_url(u)

                if dedupe and u in seen:
                    continue
                if dedupe:
                    seen.add(u)

                if skipped < max(0, skip_first):
                    skipped += 1
                    continue

                payload = self._safe_extract_payload(u, payload_mode=payload_mode)
                if payload is None:
                    self.stats["payloads_failed"] += 1
                    logger.debug("Payload extraction returned None (skipped or failed) url=%s", u)
                    continue

                yield payload
                self.stats["payloads_yielded"] += 1
                yielded += 1

                if sleep_between_resumes is None:
                    time.sleep(self.settings.sleep_between_resumes)
                elif sleep_between_resumes > 0:
                    time.sleep(sleep_between_resumes)

                if yielded >= target_n:
                    logger.info("Scrape until reached target (target_n=%s, page=%s)", target_n, page)
                    return

            page += 1

        logger.info("Scrape until done (yielded=%s, stats=%s)", yielded, self.stats)

    def iter_resumes_by_urls(
        self,
        urls: Iterable[str],
        *,
        payload_mode: PayloadMode = PayloadMode.RAW_CLEANED_PARSED,
        skip_first: int = 0,
        limit: Optional[int] = None,
        dedupe: bool = True,
        sleep_between_resumes: Optional[float] = None,
    ) -> Iterator[Dict[str, Any]]:
        yield from self._iter_payloads_from_urls(
            urls,
            payload_mode=payload_mode,
            skip_first=skip_first,
            limit=limit,
            dedupe=dedupe,
            sleep_between_resumes=sleep_between_resumes,
        )


    # -------------------------
    # Collect into a list
    # -------------------------
    def scrape_resumes_by_pages(
        self,
        page_from: int,
        page_to: int,
        *,
        payload_mode: PayloadMode = PayloadMode.RAW_CLEANED_PARSED,
        skip_first: int = 0,
        limit: Optional[int] = None,
        dedupe: bool = True,
        sleep_between_pages: Optional[float] = None,
        sleep_between_resumes: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        return list(self.iter_resumes_by_pages(
            page_from, page_to,
            payload_mode=payload_mode,
            skip_first=skip_first,
            limit=limit,
            dedupe=dedupe,
            sleep_between_pages=sleep_between_pages,
            sleep_between_resumes=sleep_between_resumes,
        ))

    def scrape_resumes_until(
        self,
        *,
        target_n: int,
        start_page: int = 1,
        skip_first: int = 0,
        max_pages: Optional[int] = None,
        payload_mode: PayloadMode = PayloadMode.RAW_CLEANED_PARSED,
        dedupe: bool = True,
        sleep_between_pages: Optional[float] = None,
        sleep_between_resumes: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        return list(self.iter_resumes_until(
            target_n=target_n,
            start_page=start_page,
            skip_first=skip_first,
            max_pages=max_pages,
            payload_mode=payload_mode,
            dedupe=dedupe,
            sleep_between_pages=sleep_between_pages,
            sleep_between_resumes=sleep_between_resumes,
        ))

    def scrape_resumes_by_urls(
        self,
        urls: Iterable[str],
        *,
        payload_mode: PayloadMode = PayloadMode.RAW_CLEANED_PARSED,
        skip_first: int = 0,
        limit: Optional[int] = None,
        dedupe: bool = True,
        sleep_between_resumes: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        return list(self.iter_resumes_by_urls(
            urls,
            payload_mode=payload_mode,
            skip_first=skip_first,
            limit=limit,
            dedupe=dedupe,
            sleep_between_resumes=sleep_between_resumes,
        ))

    # -------------------------
    # URL discovery
    # -------------------------
    def _build_list_page_url(self, page: int) -> str:
        return f"{self.base_url}{self.category_city_path}?page={page}"

    def _iter_resume_urls_from_pages(
        self,
        page_from: int,
        page_to: int,
        *,
        sleep_between_pages: Optional[float] = None,
    ) -> Iterator[str]:
        for page in range(page_from, page_to + 1):
            self.stats["pages_visited"] += 1
            url = self._build_list_page_url(page)
            logger.info("List page %s: GET %s", page, url)
            logger.debug("List page GET: page=%s url=%s", page, url)
            html = self._get_html(url)
            if not html:
                logger.warning("List page empty/failed (page=%s url=%s)", page, url)
                continue
            urls = self._extract_resume_urls_from_list_html(html)
            if not urls:
                logger.debug("No resume URLs found on page=%s", page)
                continue
            logger.debug("Extracted %s resume URLs on page=%s", len(urls), page)
            for u in urls:
                self.stats["urls_found"] += 1
                yield u

            if sleep_between_pages is None:
                time.sleep(self.settings.sleep_between_pages)
            elif sleep_between_pages > 0:
                time.sleep(sleep_between_pages)

    def _extract_resume_urls_from_list_html(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        blocks = soup.select("div.resume-link a[href]")
        urls: List[str] = []
        for a in blocks:
            href = a.get("href", "")
            if not href:
                continue
            abs_url = href if href.startswith("http") else (self.base_url.rstrip("/") + "/" + href.lstrip("/"))
            if "/resumes/" in abs_url:
                urls.append(abs_url)
        return list(dict.fromkeys(urls))

    # -------------------------
    # Payload extraction
    # -------------------------
    def _iter_payloads_from_urls(
        self,
        urls: Iterable[str],
        *,
        payload_mode: PayloadMode,
        skip_first: int = 0,
        limit: Optional[int] = None,
        dedupe: bool = True,
        sleep_between_resumes: Optional[float] = None,
    ) -> Iterator[Dict[str, Any]]:
        seen: Set[str] = set()
        skipped = 0
        yielded = 0

        for u in urls:
            u = self._normalize_resume_url(u)
            if dedupe and u in seen:
                logger.debug("Deduped URL: %s", u)
                continue
            if dedupe:
                seen.add(u)

            if skipped < max(0, skip_first):
                skipped += 1
                if skipped <= 3 or skipped % 50 == 0:
                    logger.debug("Skipping resume #%s due to skip_first (url=%s)", skipped, u)
                continue

            payload = self._safe_extract_payload(u, payload_mode=payload_mode)
            if payload is None:
                self.stats["payloads_failed"] += 1
                logger.debug("Skipped/failed payload (url=%s)", u)
                continue

            yield payload
            self.stats["payloads_yielded"] += 1
            yielded += 1

            if sleep_between_resumes is None:
                time.sleep(self.settings.sleep_between_resumes)
            elif sleep_between_resumes > 0:
                time.sleep(sleep_between_resumes)

            if limit is not None and yielded >= limit:
                logger.info("Iterator stopped by limit=%s (yielded=%s)", limit, yielded)
                return

        logger.info("Iterator finished (yielded=%s, skipped=%s, dedupe=%s)", yielded, skipped, dedupe)

    def _safe_extract_payload(self, url: str, *, payload_mode: PayloadMode) -> Optional[Dict[str, Any]]:
        try:
            payload = self.extract_payload(url, payload_mode=payload_mode)
            return payload
        except Exception as e:
            logger.exception("Payload extraction failed: url=%s mode=%s error=%s", url, payload_mode, e)
            return None

    def _normalize_resume_url(self, url: str) -> str:
        if url.startswith("/"):
            return self.base_url.rstrip("/") + url
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return self.base_url + url.lstrip("/")

    def _get_html(self, url: str) -> Optional[str]:
        r = self.session.get(url, timeout=self.request_timeout)
        if r.status_code != 200:
            logger.warning("GET failed: %s status=%s", url, r.status_code)
            return None
        return r.text

    def extract_payload(self, resume_url: str, *, payload_mode: PayloadMode) -> Optional[Dict[str, Any]]:
        """Fetch and extract one resume page.

        Returns a JSON-serializable payload or None if the resume should be skipped.
        """
        logger.debug("Extract payload start (url=%s, mode=%s)", resume_url, payload_mode)

        rid = extract_resume_id_from_url(resume_url)
        r = self.session.get(resume_url, timeout=self.request_timeout)
        if r.status_code != 200:
            logger.warning("Resume GET failed: %s status=%s", resume_url, r.status_code)
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        container = None
        chosen = "none"
        warnings: List[str] = []

        if rid:
            container = soup.find("div", id=f"resume_{rid}")
            if container:
                chosen = "resume_id_block"
            else:
                chosen = "missing_resume_id_block"
                self.stats["missing_main_block"] += 1
                warnings.append("missing_main_resume_block")
                logger.debug("Main resume block missing (rid=%s url=%s), fallback will be used", rid, resume_url)

        if container is None:
            # best-effort fallback: use the whole page
            chosen = "whole_page_fallback"
            self.stats["missing_main_block"] += 1
            warnings.append("used_whole_page_fallback")
            raw_html = r.text
        else:
            raw_html = str(container)

        # optionally skip "file-only" resumes (based on cleaned text heuristic)
        # cleaning here is cheap and helps decide skipping, even in RAW mode
        cleaned_for_skip = html_to_text_keep_breaks(raw_html)
        import re as _re
        if _re.search(r"(файл резюме|завантажити файл|прикріплен(ий|і)\s+файл)", cleaned_for_skip, _re.IGNORECASE):
            logger.info("Resume skipped (file-uploaded resume) url=%s", resume_url)
            return None
        if _re.search(r"(• Візитка)", cleaned_for_skip, _re.IGNORECASE):
            logger.info("Resume skipped (business card) url=%s", resume_url)
            return None

        cleaned_text: Optional[str] = None
        parsed: Optional[Dict[str, Any]] = None

        if payload_mode in (PayloadMode.RAW_CLEANED, PayloadMode.RAW_CLEANED_PARSED):
            cleaned_text = cleaned_for_skip

        if payload_mode == PayloadMode.RAW_CLEANED_PARSED:
            parsed = self._parse_best_effort(container, cleaned_for_skip)
            if parsed.get("person_name") == "unknown":
                warnings.append("unknown_person_name")
                logger.debug("Best-effort parse: unknown person_name (url=%s)", resume_url)

        payload = {
            "schema_version": "resume_payload_v1",
            "source_url": resume_url,
            "raw_html": raw_html,
            "cleaned_text": cleaned_text,
            "parsed": parsed,
            "meta": {
                "parse_mode": chosen,
                "warnings": warnings,
                "scraped_at": _utc_now_iso(),
            },
        }

        logger.debug("Extract payload done (url=%s parse_mode=%s warnings=%s)", resume_url, chosen, warnings)
        return payload

    def _parse_best_effort(self, container, cleaned: str) -> Dict[str, Any]:
        """Extract best-effort parsed fields from container/cleaned text."""
        person_name = "unknown"
        position = None
        salary = None
        resume_date = None
        birthday = None
        full_time = False
        part_time = False
        from_home = False

        work_experience = None
        education = None
        additional_education = None
        recommendations = None
        additional_info = None
        skills = None
        languages = None

        city = None
        ready_to_work = None          # Optional[List[str]]
        considered_positions = None   # Optional[List[str]]
        disability = None

        if container is not None:
            h1 = container.find("h1")
            if h1:
                nm = normalize_ws(h1.get_text(" ", strip=True))
                nm = clean_person_name(nm)
                if nm:
                    person_name = nm

            pos_h2 = container.select_one("h2.title-print") or container.find("h2")
            if pos_h2:
                pos_txt = normalize_ws(pos_h2.get_text(" ", strip=True))
                if pos_txt:
                    position = pos_txt

        t_name, t_pos = parse_name_position_from_cleaned(cleaned)
        is_veteran = parse_is_veteran_from_raw_html(str(container))
        t_date = parse_resume_date_from_cleaned(cleaned)
        t_emp = parse_employment_from_cleaned(cleaned)
        t_salary = parse_salary_from_cleaned(cleaned)

        city = parse_city_from_cleaned(cleaned)
        ready_to_work = parse_ready_to_work_from_cleaned(cleaned)
        considered_positions = parse_considered_positions_from_cleaned(cleaned)
        disability = parse_disability_from_cleaned(cleaned)

        if not person_name or person_name == "unknown":
            person_name = t_name or person_name
        if not position:
            position = t_pos or position

        position = strip_salary_tail_from_position(position)

        salary = t_salary
        resume_date = t_date
        if not (full_time or part_time or from_home):
            full_time, part_time, from_home = t_emp

        work_experience = extract_section_text_by_title(cleaned, ["Досвід роботи"])
        education = extract_section_text_by_title(cleaned, ["Освіта"])
        additional_education = extract_section_text_by_title(cleaned, ["Додаткова освіта"])
        recommendations = extract_section_text_by_title(cleaned, ["Рекомендації"])
        additional_info = extract_section_text_by_title(cleaned, ["Додаткова інформація"])

        skills_sec = extract_section_text_by_title(cleaned, ["Знання і навички"])
        skills_list = parse_bullets(skills_sec)
        skills = skills_list if skills_list else None

        lang_sec = extract_section_text_by_title(cleaned, ["Знання мов"])
        languages_list = parse_bullets(lang_sec)
        languages = languages_list if languages_list else None

        return {
            "person_name": person_name or "unknown",
            "resume_date": resume_date,  # str or None
            "position": position,
            "salary": salary,
            "full_time": bool(full_time),
            "part_time": bool(part_time),
            "from_home": bool(from_home),
            "city": city,
            "ready_to_work": ready_to_work,                 # <-- list[str] or None
            "considered_positions": considered_positions,   # <-- list[str] or None
            "disability": disability,
            "veteran": bool(is_veteran),
            "birthday": birthday,
            "work_experience": work_experience,
            "education": education,
            "additional_education": additional_education,
            "skills": skills,
            "languages": languages,
            "recommendations": recommendations,
            "additional_info": additional_info,
        }
