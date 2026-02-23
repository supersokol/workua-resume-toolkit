from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, TypedDict


class SimilarityMatcher(Protocol):
    def similarity(self, a: str, b: str) -> float: ...


class SkillItem(TypedDict, total=False):
    name: str
    experience_years: Optional[float]


@dataclass
class ProcessedResume:
    driving_categories: List[str] = field(default_factory=list)

    normalized_skills: List[str] = field(default_factory=list)
    skill_months: Optional[Dict[str, int]] = None

    work_experience_items: Optional[List[Dict[str, Any]]] = None
    education_items: Optional[List[Dict[str, Any]]] = None

    total_work_years: Optional[float] = None
    total_edu_years: Optional[float] = None
    months_by_position: Optional[List[Dict[str, Any]]] = None

    extractor_warnings: List[str] = field(default_factory=list)
