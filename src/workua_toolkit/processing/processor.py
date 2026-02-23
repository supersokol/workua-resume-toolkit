from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from .regex_extractor import fmt_years_1dp, extract_driving_categories, driving_cats_from_skill_months
from .regex_resume import regex_extract_from_payload
from .types import ProcessedResume, SimilarityMatcher


def norm_skill(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_title(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"[\(\)\[\],.;:]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def fill_missing_work_titles(work_items: List[Any], work_text: str, position: Optional[str]) -> List[Any]:
    pos = (position or "").strip()
    if not pos:
        m = re.search(r"(Должность|Посада|Position)\s*[:\-]\s*([^\n]+)", work_text or "", re.IGNORECASE)
        if m:
            pos = m.group(2).strip()
    for it in work_items or []:
        title = (getattr(it, "title", "") or "").strip()
        if not title and pos:
            try:
                setattr(it, "title", pos)
            except Exception:
                pass
    return work_items

def aggregate_months_by_title(
    work_items: List[Any],
    matcher: Optional[SimilarityMatcher] = None,
    thr: float = 0.82,
) -> List[Dict[str, Any]]:
    """
    Aggregate experience months by title.

    - If matcher is None: bucket by normalized title (fast, deterministic).
    - If matcher is provided: cluster titles by semantic similarity to a stable representative.
    """
    clusters: List[Dict[str, Any]] = []
    # cluster schema:
    # {
    #   "rep_raw": str,      # stable human-readable representative
    #   "rep_norm": str,     # normalized key of rep_raw
    #   "months": int,
    #   "titles": set[str],  # raw titles encountered
    # }

    for it in work_items or []:
        raw_title = (getattr(it, "title", "") or "").strip()
        months = int(getattr(it, "months", 0) or 0)
        if not raw_title or months <= 0:
            continue

        title_n = norm_title(raw_title)
        if not title_n:
            continue

        # -------------------------
        # Lightweight mode: exact buckets by normalized title
        # -------------------------
        if matcher is None:
            for c in clusters:
                if title_n == c["rep_norm"]:
                    c["months"] += months
                    c["titles"].add(raw_title)
                    break
            else:
                clusters.append(
                    {
                        "rep_raw": raw_title,
                        "rep_norm": title_n,
                        "months": months,
                        "titles": {raw_title},
                    }
                )
            continue

        # -------------------------
        # Semantic mode: attach to best cluster representative
        # -------------------------
        best_idx: Optional[int] = None
        best_sim: float = 0.0

        for i, c in enumerate(clusters):
            # Compare to stable representative, not an arbitrary element of the set
            sim = matcher.similarity(raw_title, c["rep_raw"])
            if sim > best_sim:
                best_sim, best_idx = sim, i

        if best_idx is not None and best_sim >= thr:
            c = clusters[best_idx]
            c["months"] += months
            c["titles"].add(raw_title)

            # Optional: update representative if this title is "better"
            # Simple heuristic: prefer shorter/more canonical title
            # (keeps output nicer; safe to remove if you want strict determinism)
            if len(raw_title) < len(c["rep_raw"]):
                c["rep_raw"] = raw_title
                c["rep_norm"] = norm_title(raw_title) or c["rep_norm"]
        else:
            clusters.append(
                {
                    "rep_raw": raw_title,
                    "rep_norm": title_n,
                    "months": months,
                    "titles": {raw_title},
                }
            )

    # -------------------------
    # Finalize output
    # -------------------------
    out: List[Dict[str, Any]] = []
    for c in clusters:
        m = int(c["months"])
        out.append(
            {
                # keep human-readable position and normalized key too
                "position": c["rep_norm"],
                "display_position": c["rep_raw"],
                "months": m,
                "years": fmt_years_1dp(m) if m else None,
                "titles": sorted(list(c["titles"]))[:20],
            }
        )

    out.sort(key=lambda x: x["months"], reverse=True)
    logger.debug(
        "aggregate_months_by_title: items=%s clusters=%s matcher=%s",
        len(work_items or []),
        len(out),
        "on" if matcher is not None else "off",
    )
    return out

def build_skill_months_from_work_items(
    work_items: List[Any],
    known_raw_skills: List[str],
    matcher: Optional[SimilarityMatcher] = None,
    thr: float = 0.78,
) -> Dict[str, int]:
    """ 
    normalized_skill -> months. Adds full 'months' of a WorkItem if a known skill is detected in duties.
    If matcher is provided, do semantic match duty -> best known skill.
    Otherwise only exact normalized matching.
    """
    from collections import defaultdict

    skill_months = defaultdict(int)

    known = []
    for s in known_raw_skills or []:
        ns = norm_skill(s)
        if ns:
            known.append((ns, s))

    if not known:
        logger.debug("build_skill_months_from_work_items: no known skills provided")
        return {}

    for wi in work_items or []:
        months = int(getattr(wi, "months", 0) or 0)
        if months <= 0:
            continue

        duties_list = getattr(wi, "duties", None) or []
        if isinstance(duties_list, str):
            duties_list = [duties_list]

        for d in duties_list:
            d = (d or "").strip()
            if not d:
                continue

            # 1) exact match via normalization
            matched = False
            nd = norm_skill(d)
            if nd:
                for ns, _orig in known:
                    if nd == ns:
                        skill_months[ns] += months
                        matched = True
                        break
            if matched:
                continue

            # 2) semantic match only if matcher is available
            if matcher is None:
                continue

            best_ns = None
            best_sim = 0.0
            for ns, orig in known:
                try:
                    sim = matcher.similarity(d, orig)
                except Exception as e:
                    logger.exception("Matcher similarity failed (duty=%r, skill=%r, exception=%r)", d, orig, e)
                    continue
                if sim > best_sim:
                    best_sim, best_ns = sim, ns

            if best_ns and best_sim >= thr:
                skill_months[best_ns] += months
    out = dict(skill_months)
    logger.debug(
        "build_skill_months_from_work_items: work_items=%s known_skills=%s matched=%s matcher=%s",
        len(work_items or []),
        len(known),
        len(out),
        "on" if matcher is not None else "off",
    )
    return out

class UniversalResumeProcessor:
    """Regex-first processor that produces a compact processed JSON."""

    def __init__(self, matcher: Optional[SimilarityMatcher] = None):
        self.matcher = matcher

    def process_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = payload.get("source_url")
        logger.info(
            "Process payload: url=%s matcher=%s",
            url,
            "on" if self.matcher else "off",
        )
        try:
            rx = regex_extract_from_payload(payload)
            parsed = payload.get("parsed") or {}

            logger.debug(
                "Regex extract summary (url=%s work_items=%s edu_items=%s skills=%s)",
                url,
                len(rx.work_items or []),
                len(rx.edu_items or []),
                len(rx.skills or []),
                #len(rx.warnings or []),
            )

            position = parsed.get("position") if isinstance(parsed, dict) else None
            work_text = parsed.get("work_experience") if isinstance(parsed, dict) else ""

            work_items = fill_missing_work_titles(rx.work_items, work_text or "", position=position)

            total_work_months = sum(int(getattr(it, "months", 0) or 0) for it in work_items or [])
            total_work_years = fmt_years_1dp(total_work_months) if total_work_months > 0 else None

            logger.info(
                "Processed: work_items=%s edu_items=%s total_work_years=%s",
                len(rx.work_items or []),
                len(rx.edu_items or []),
                total_work_years,
            )
            months_by_position = aggregate_months_by_title(work_items, matcher=self.matcher)

            total_edu_months = sum(int(getattr(it, "months", 0) or 0) for it in rx.edu_items or [])
            total_edu_years = fmt_years_1dp(total_edu_months) if total_edu_months > 0 else None

            normalized_skills = []
            for s in rx.skills or []:
                ns = norm_skill(s)
                if ns and ns not in normalized_skills:
                    normalized_skills.append(ns)

            skill_months = build_skill_months_from_work_items(
                work_items=work_items,
                known_raw_skills=rx.skills,
                matcher=self.matcher,
            )

            driving_categories: List[str] = []

            for c in extract_driving_categories(", ".join(rx.skills or [])):
                if c not in driving_categories:
                    driving_categories.append(c)

            for c in driving_cats_from_skill_months(skill_months):
                if c not in driving_categories:
                    driving_categories.append(c)
            
            # dedupe
            driving_categories = list(set(driving_categories))
            
            logger.info(
                "Processing payload done (url=%s total_work_years=%s total_edu_years=%s positions=%s skills_norm=%s skill_months=%s driving_categories=%s)",
                url,
                total_work_years,
                total_edu_years,
                len(months_by_position or []),
                len(normalized_skills),
                len(skill_months or {}),
                len(driving_categories or {}),
            )

            out = ProcessedResume(
                driving_categories=driving_categories,
                normalized_skills=normalized_skills,
                skill_months=skill_months or None,
                work_experience_items=[getattr(x, "__dict__", {}) for x in work_items] if work_items else None,
                education_items=[getattr(x, "__dict__", {}) for x in rx.edu_items] if rx.edu_items else None,
                total_work_years=total_work_years,
                total_edu_years=total_edu_years,
                months_by_position=months_by_position or None,
                extractor_warnings=[],
            )
            return asdict(out)
        except Exception as e:
            
            logger.exception("Processing failed: url=%s error=%s", url, e)
            processed.extractor_warnings.append("processing_failed")