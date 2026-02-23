from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .regex_extractor import parse_work_experience_section, parse_education_section, parse_language_item



@dataclass
class RegexExtractResult:
    position: Optional[str] = None
    salary: Optional[int] = None
    skills: List[str] = field(default_factory=list)
    languages: Optional[List[Dict[str, Any]]] = None
    work_items: List[Any] = field(default_factory=list)
    edu_items: List[Any] = field(default_factory=list)


def _split_csv_like(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v)
    return [x.strip() for x in re.split(r"[,;•\u2022]+", s) if x.strip()]


def regex_extract_from_payload(payload: Dict[str, Any]) -> RegexExtractResult:
    """Extract structured WorkItem/EduItem lists using regex-based section parsers."""
    src = payload.get("source_url")
    logger.debug("regex_extract_from_payload: start url=%s", src)

    parsed = payload.get("parsed") or {}
    cleaned = payload.get("cleaned_text") or ""
    
    if not cleaned:
        logger.warning("regex_extract_from_payload: missing cleaned_text (url=%s). Regex parsing will be degraded.", src)
    if not isinstance(parsed, dict):
        logger.warning("regex_extract_from_payload: parsed is not a dict (url=%s)", src)

    languages = _split_csv_like(parsed.get("languages")) if isinstance(parsed, dict) else []     
    languages = list(dict.fromkeys([x for x in languages if x]))
    
    languages_structured: List[dict] = []
    try:
        seen = set()
        for x in languages:
            parsed = parse_language_item(x)
            if not parsed:
                continue
            key = (parsed["language"], parsed.get("level"))
            if key in seen:
                continue
            seen.add(key)
            languages_structured.append(parsed)
    except Exception:
        languages_structured = []


    work_text = (parsed.get("work_experience") if isinstance(parsed, dict) else None) or ""
    edu_text = (parsed.get("education") if isinstance(parsed, dict) else None) or ""

    work_items = parse_work_experience_section(work_text)
    edu_items = parse_education_section(edu_text)

    res = RegexExtractResult(
        position=(parsed.get("position") if isinstance(parsed, dict) else None),
        salary=(parsed.get("salary") if isinstance(parsed, dict) else None),
        skills=_split_csv_like(parsed.get("skills")) if isinstance(parsed, dict) else [],
        languages=languages_structured,
        work_items=work_items,
        edu_items=edu_items,
    )

    logger.debug(
        "regex_extract_from_payload: done url=%s work_items=%s edu_items=%s skills=%s",
        src,
        len(res.work_items or []),
        len(res.edu_items or []),
        len(res.skills or []),
#        len(res.warnings or []),
    )

    return res
