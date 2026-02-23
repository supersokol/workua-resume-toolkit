from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import re
from datetime import date
import datetime as dt
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple

# -----------------------------
# Dates helpers (for fallback)
# -----------------------------

# -----------------------------
# Work-experience fallback helpers
# -----------------------------
_IGNORE_HEADERS_RE = re.compile(
    r"^\s*(ОСОБИСТІ\s+ЯКОСТІ|ОБОВ['’]?\s*ЯЗКИ|ОБОВЯЗКИ|ОБЯЗАННОСТИ)\s*:?\s*$",
    re.IGNORECASE
)

_BULLET_PREFIX_RE = re.compile(r"^\s*[•\-\*\u2022]+\s*(?:[•\-\*\u2022]+\s*)*")  # "•", "• •", "- -", etc.

# Варианты дат в одной строке:
# "2020 – 2025", "2020-2025", "2020 — 2025", "2020 – нині", "2020 – now"
_INLINE_YEARS_RE = re.compile(
    r"(?P<start>\d{4})\s*[–—\-]\s*(?P<end>\d{4}|нині|тепер|present|now)\b",
    re.IGNORECASE
)

# Любая "дата-подобная" подпись чтобы детектить начало нового блока,
# если твой основной формат не подходит
_ANY_DATES_HINT_RE = re.compile(
    r"(\b\d{2}\.\d{4}\b|\b\d{4}\b\s*[–—\-]\s*(\b\d{4}\b|нині|тепер|present|now))",
    re.IGNORECASE
)

_MONTHS = {
    # UA
    "січ": 1, "лют": 2, "бер": 3, "квіт": 4, "трав": 5, "черв": 6,
    "лип": 7, "серп": 8, "вер": 9, "жовт": 10, "лист": 11, "груд": 12,
    # RU
    "янв": 1, "фев": 2, "мар": 3, "апр": 4, "май": 5, "июн": 6,
    "июл": 7, "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
}

DEGREE_MAP = [
    ("незакінчена вища", ["незакінчена", "незакінчен", "неповна вища", "incomplete higher"]),
    ("вища", ["вища", "высшее", "higher"]),
    ("середня спеціальна", ["середня спеціальна", "среднее специальное", "vocational", "college"]),
    ("середня", ["середня", "среднее", "secondary"]),
]

_RE_DATE_LINE = re.compile(
    r"(?i)^\s*(?:з|с)\s*\d{2}\.\d{4}\s*(?:по|до)\s*(?:\d{2}\.\d{4}|нині|тепер|сьогодні|present)\s*(?:\([^)]*\))?\s*$"
)

_RE_MMYYYY = re.compile(r"(\d{2})\.(\d{4})")

OPF_TOKENS = {
    "тов", "тзов", "ооо", "пп", "дп", "прat", "пат", "ат", "прат",
    "фоп", "флп", "іп", "упсп", "тоv"  # можно дополнять
}

HEADER_PREFIX_RE = re.compile(
    r"^\s*(обов[’'`]?язки|обязанности|обовязки|osobysti yakosti|особисті якості)\s*:?\s*",
    re.IGNORECASE
)

BULLET_RE = re.compile(r"^\s*([•\-\*]+|\d+[\.\)\:])\s+")

DATE_PAIR_LINE_RE = re.compile(
    r"^\s*(з|із|from)\s+(.+?)\s+(по|to)\s+(.+?)\s*[\-–—]\s*(.+)$",
    re.IGNORECASE
)

DUTIES_PREFIX_RE = re.compile(
    r"^\s*(обов[’']?язки|обов'язки|обязанности|duties)\s*:\s*",
    re.IGNORECASE
)

ROLE_PREFIX_RE = re.compile(r"\s*(?:-|–|—|:|\()\s*", re.UNICODE)

# -----------------------------
# Driving categories
# -----------------------------
_DRIVING_RE = re.compile(r"\bкат\.?\s*([A-ZА-Я]{1,2})\b", re.IGNORECASE)

def extract_driving_categories(text: str) -> List[str]:
    cats = set()
    for m in _DRIVING_RE.finditer(text or ""):
        cats.add(m.group(1).upper())
    for c in re.findall(r"\b(A|B|C|D|BE|CE|DE)\b", text or ""):
        cats.add(c.upper())
    order = ["A", "B", "BE", "C", "CE", "D", "DE"]
    return [c for c in order if c in cats]

def driving_cats_from_skill_months(skill_months: Optional[Dict[str, int]]) -> List[str]:
    """
    Extract driving categories from normalized_skill keys of skill_months mapping.
    Works even if your pipeline doesn't build skills_structured.
    """
    cats: List[str] = []
    for k in (skill_months or {}).keys():
        for c in extract_driving_categories(k):
            if c not in cats:
                cats.append(c)
    return cats


# -----------------------------
# Language parsing
# -----------------------------
_LEVEL_ALIASES = {
    "початковий": ["початковий", "beginner", "elementary", "a1", "a2"],
    "середній": ["середній", "intermediate", "b1"],
    "вище середнього": ["вище середнього", "upper intermediate", "upper-intermediate", "b2"],
    "просунутий": ["просунутий", "advanced", "c1"],
    "вільно": ["вільно", "fluent", "native", "c2"],
}

def _norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"\s+", " ", s)
    return s

def _detect_level(s: str) -> Optional[str]:
    t = _norm_text(s)
    for lvl, aliases in _LEVEL_ALIASES.items():
        for a in aliases:
            a2 = _norm_text(a)
            if re.search(rf"(^|[^a-z0-9]){re.escape(a2)}([^a-z0-9]|$)", t):
                return lvl
    return None

def parse_language_item(item: Any) -> Optional[Dict[str, Any]]:
    """
    Parses items like:
      - "Англійська — середній"
      - "Deutsch B2"
      - "Polski: вільно"
    """
    if item is None:
        return None

    s = str(item).strip()
    if not s:
        return None

    parts = re.split(r"[-–—:]", s, maxsplit=1)

    if len(parts) == 2:
        lang = parts[0].strip()
        rest = parts[1].strip()
    else:
        tokens = s.split()
        if not tokens:
            return None
        lang = tokens[0]
        rest = s[len(lang):].strip()

    level = _detect_level(rest)
    if not lang:
        return None

    return {"language": lang, "level": level}

def _split_title_into_role_candidates(title: str) -> List[str]:
    """
    Пробуем разложить заголовок должности по дефисам, / и т.п.
    Возвращаем список ролей-кандидатов (нормализованных, без мусора).
    """
    t = (title or "").strip()
    if not t:
        return []
    # ключевой сплит: по '-' (и длинным тире) + дополнительно по '/'
    parts = re.split(r"\s*(?:-|–|—|/)\s*", t)
    parts = [p.strip() for p in parts if p and p.strip()]
    # защитимся от мусора: слишком короткие/слишком длинные
    parts = [p for p in parts if 2 <= len(p) <= 80]
    # дедуп
    out = []
    seen = set()
    for p in parts:
        k = p.lower()
        if k not in seen:
            seen.add(k)
            out.append(p)
    return out

def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _find_role_prefix_positions(duties_text: str, role: str) -> List[int]:
    """
    Ищем в duties_text вхождения role, после которых сразу идёт один из префиксов: -, :, (
    Возвращаем позиции start-индексов этих вхождений (в исходной строке).
    """
    if not duties_text or not role:
        return []
    dt = duties_text
    # ищем role как слово/фразу, не обязательно whole-word (но с границами по буквам лучше)
    pattern = re.compile(rf"(?i)(?<!\w){re.escape(role)}(?!\w)\s*(?:-|–|—|:|\()", re.UNICODE)
    return [m.start() for m in pattern.finditer(dt)]

def _split_duties_by_role_prefixes(title: str, duties_text: str) -> Optional[List[Tuple[str, str]]]:
    roles = _split_title_into_role_candidates(title)
    if len(roles) < 2:
        return None

    dt = _normalize_ws(duties_text)
    if not dt:
        return None

    hits = []
    for r in roles:
        pos_list = _find_role_prefix_positions(dt, r)
        if not pos_list:
            return None
        hits.append((pos_list[0], r))  # берём первое вхождение роли

    # сортируем по позиции в тексте — это и есть реальный порядок сегментов
    hits.sort(key=lambda x: x[0])

    # 🔥 НОВАЯ ЛОГИКА: проверяем, что все роли уникальны и все из title действительно найдены
    roles_lower_set = {r.lower() for r in roles}
    hits_roles_lower = [r.lower() for _, r in hits]

    if len(hits_roles_lower) != len(roles_lower_set):
        return None  # дубликаты или потерянные роли

    if set(hits_roles_lower) != roles_lower_set:
        return None  # нашли не те роли

    segments: List[Tuple[str, str]] = []
    for idx, (start_pos, role) in enumerate(hits):
        m = re.search(
            rf"(?i)(?<!\w){re.escape(role)}(?!\w)\s*(?:-|–|—|:|\()",
            dt[start_pos:]
        )
        if not m:
            return None

        seg_start = start_pos + m.end()
        seg_end = len(dt) if idx + 1 == len(hits) else hits[idx + 1][0]
        seg_text = dt[seg_start:seg_end].strip()

        if not seg_text:
            return None

        segments.append((role.strip(), seg_text))

    return segments

def split_duties_strict_dot_semi(text: str) -> List[str]:
    t = _normalize_ws(text)
    if not t:
        return []
    # если есть функция split_outside_parens, лучше ей:
    try:
        parts = split_outside_parens(t, ".;")
    except Exception:
        parts = re.split(r"[.;]+", t)
    out = [p.strip(" -–—\t,") for p in parts if p.strip(" -–—\t,")]
    return out

def has_duties_prefix(s: str) -> bool:
    return bool(DUTIES_PREFIX_RE.match(s or ""))

def normalize_date_token(tok: str) -> Optional[str]:
    t = (tok or "").strip().lower().replace("р.", "").replace("роки", "").replace("р", "").strip()
    # поддержка dd.mm.yyyy / dd.mm.yy
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", t)
    if m:
        dd, mm, yy = m.group(1), m.group(2), m.group(3)
        if len(yy) == 2:
            yy = "19" + yy  # грубо; можно улучшить
        return f"{yy.zfill(4)}-{mm.zfill(2)}"
    # поддержка mm.yyyy
    m = re.search(r"(\d{1,2})\.(\d{4})", t)
    if m:
        mm, yy = m.group(1), m.group(2)
        return f"{yy}-{mm.zfill(2)}"
    # если "нині/тепер" => present
    if "нині" in t or "тепер" in t or "present" in t or "дотепер" in t:
        return "present"
    return None

def is_bullet_line(s: str) -> bool:
    return bool(BULLET_RE.match(s or ""))

def clean_bullet_prefix(s: str) -> str:
    return BULLET_RE.sub("", (s or "").strip())

def strip_headers(s: str) -> str:
    return HEADER_PREFIX_RE.sub("", (s or "").strip())

def is_ignorable_header_line(line: str) -> bool:
    return bool(_IGNORE_HEADERS_RE.match((line or "").strip()))

def strip_bullets(line: str) -> str:
    return _BULLET_PREFIX_RE.sub("", (line or "").strip()).strip()

def is_bullet_line(line: str) -> bool:
    return bool(_BULLET_PREFIX_RE.match((line or "").strip()))

def parse_inline_title_dates_meta(line: str) -> Optional[Tuple[str, str, str, int, Optional[str], Optional[str]]]:
    """
    Parse: "Кур’єр / водій (2020 – 2025) Some Company (Industry)"
    Returns: (title, start_ym, end_norm, months, company, industry)
      end_norm: "YYYY-12" or "present"
    """
    s = (line or "").strip()
    if not s:
        return None

    # title + (dates) required
    m = re.search(r"^(?P<title>.+?)\s*\((?P<dates>[^)]{3,50})\)\s*(?P<rest>.*)$", s)
    if not m:
        return None

    title = m.group("title").strip()
    dates_part = m.group("dates").strip()
    rest = (m.group("rest") or "").strip()

    # parse years range from dates_part
    m2 = _INLINE_YEARS_RE.search(dates_part)
    if not m2:
        return None

    y1 = int(m2.group("start"))
    end_raw = m2.group("end").lower()

    start_ym = f"{y1:04d}-01"

    if end_raw.isdigit():
        y2 = int(end_raw)
        end_norm = f"{y2:04d}-12"
        months = calc_months(start_ym, end_norm)
    else:
        end_norm = "present"
        months = calc_months(start_ym, _now_ym())

    # company (optional) + (industry optional)
    company = None
    industry = None
    if rest:
        # if ends with "(industry)"
        m3 = re.search(r"^(?P<company>.*?)(?:\s*\((?P<industry>[^)]{2,120})\)\s*)?$", rest)
        if m3:
            company = (m3.group("company") or "").strip() or None
            industry = (m3.group("industry") or "").strip() or None
        else:
            company = rest.strip() or None

    return title, start_ym, end_norm, int(months), company, industry

def duties_from_lines(lines: List[str]) -> Tuple[List[str], str]:
    """
    Превращает сырой список строк в duties list + duties_text
    Правила:
      - игнорируем заголовки ("ОСОБИСТІ ЯКОСТІ", "ОБОВ'ЯЗКИ" и т.п.)
      - bullet line -> 1 duty целиком
      - non-bullet -> дробим по , . ;
    """
    acc: List[str] = []
    for ln in lines:
        ln = (ln or "").strip()
        if not ln:
            continue
        if is_ignorable_header_line(ln):
            continue

        if is_bullet_line(ln):
            d = strip_bullets(ln)
            if d:
                acc.append(d)
        else:
            # обычный текст: разбиваем
            # (не используем bullets splitting)
            parts = re.split(r"[;,\.\u2022•]+", ln)
            for p in parts:
                p = p.strip()
                if p and not is_ignorable_header_line(p):
                    acc.append(p)

    # dedup preserving order
    out = []
    seen = set()
    for x in acc:
        k = re.sub(r"\s+", " ", x.lower()).strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(x)

    duties_text = " ".join(out).strip()
    return out, duties_text

#------------------------

def _ym_to_int(ym: str) -> int:
    # "YYYY-MM" -> YYYY*12 + MM
    y, m = ym.split("-")
    return int(y) * 12 + int(m)

def _now_ym() -> str:
    today = date.today()
    return f"{today.year:04d}-{today.month:02d}"

def calc_months(start_ym: str, end_ym: str) -> int:
    """
    Возвращает количество месяцев между start_ym и end_ym включительно.
    Пример: 2018-03..2018-03 => 1
            2018-03..2019-06 => 16
    """
    if not start_ym or not end_ym:
        return 0
    a = _ym_to_int(start_ym)
    b = _ym_to_int(end_ym)
    if b < a:
        return 0
    return (b - a) + 1

def _month_from_token(tok: str) -> Optional[int]:
    t = tok.strip().lower()
    t3 = t[:3]
    return _MONTHS.get(t3)

def parse_ym(token: str) -> Optional[dt.date]:
    """
    Поддержка:
      04.2017
      2017-04
      квітень 2017 / апрель 2017 (берём первые 3 буквы)
    """
    s = token.strip()

    m = re.match(r"^(\d{2})\.(\d{4})$", s)
    if m:
        mm, yy = int(m.group(1)), int(m.group(2))
        return dt.date(yy, mm, 1)

    m = re.match(r"^(\d{4})-(\d{2})$", s)
    if m:
        yy, mm = int(m.group(1)), int(m.group(2))
        return dt.date(yy, mm, 1)

    m = re.match(r"^([A-Za-zА-Яа-яІіЇїЄєҐґ]+)\s+(\d{4})$", s)
    if m:
        mon = _month_from_token(m.group(1))
        yy = int(m.group(2))
        if mon:
            return dt.date(yy, mon, 1)

    return None

def months_between(a: dt.date, b: dt.date) -> int:
    if b < a:
        return 0
    return (b.year - a.year) * 12 + (b.month - a.month) + 1

def fmt_years_1dp(months: int) -> Optional[float]:
    if months <= 0:
        return None
    years = months / 12.0
    return float(f"{years:.1f}")

def is_dates_meta_line(s: str) -> bool:
    """
    Пример:
    'з 10.2019 по 02.2022 (2 роки 5 місяців) Ostriv, Киев (Розничная торговля)'
    или
    'з 01.2004 по нині (22 роки) Власне авто (Приватні особи)'
    """
    t = (s or "").strip()
    if not t:
        return False
    # обязательно "з/с" и "по/до"
    if not re.search(r"(?i)\b(з|с)\b", t):
        return False
    if not re.search(r"(?i)\b(по|до)\b", t):
        return False
    # хотя бы одна дата MM.YYYY
    if not _RE_MMYYYY.search(t):
        return False
    return True

def _to_yyyy_mm(mm: str, yyyy: str) -> str:
    return f"{yyyy}-{mm}"

def looks_like_title(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    if is_dates_meta_line(t):
        return False
    if len(t) > 80:
        return False
    return True

def looks_like_city(token: str) -> bool:
    t = (token or "").strip()
    if not t:
        return False

    low = t.lower().strip(' "\'“”«»()')
    if low in OPF_TOKENS:
        return False

    # если есть кавычки/аббревиатуры/цифры — скорее не город
    if re.search(r'[\"“”«»0-9]', t):
        return False

    # если 3+ слов — скорее не город (типа "ЛК Юкрейн Групп")
    words = [w for w in re.split(r"\s+", low) if w]
    if len(words) >= 3:
        return False

    # если это "обл." / "район" / "львівська обл." — это регион, можно считать city/region
    if "обл" in low or "район" in low or "область" in low:
        return True

    # 1–2 слова — обычно город
    return len(words) <= 2

def clean_duties_text(s: str) -> str:
    s = (s or "").strip()

    # удаляем ведущие маркеры обязанностей (разные варианты)
    s = re.sub(r"^\s*(ОБОВ['’]ЯЗКИ|ОБОВЯЗКИ|ОБЯЗАННОСТИ|DUTIES)\s*:?\s*", "", s, flags=re.IGNORECASE)

    # иногда строка состоит только из маркера
    if re.fullmatch(r"(ОБОВ['’]ЯЗКИ|ОБОВЯЗКИ|ОБЯЗАННОСТИ)\s*:?\s*", s, flags=re.IGNORECASE):
        return ""

    return s.strip()

def split_duties(text: str) -> List[str]:
    raw = (text or "").strip()
    if not raw:
        return []

    # 1) фиксируем: был ли префикс "ОБОВ'ЯЗКИ:" в НАЧАЛЕ исходного текста
    prefixed = has_duties_prefix(raw)

    # 2) чистим заголовки/мусор (но prefixed уже запомнили)
    s = strip_headers(raw)

    # если это просто заголовок — пусто
    if not s or s.lower() in {"обов'язки", "обовязки", "обязанности", "особисті якості"}:
        return []

    # 3) построчная очистка (чтобы "ОБОВ'ЯЗКИ:" отдельной строкой не ломала эвристики)
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    lines = [strip_headers(ln) for ln in lines]
    lines = [ln for ln in lines if ln and ln.lower() not in {"обов'язки", "обовязки", "обязанности", "особисті якості"}]

    if not lines:
        return []

    # 4) если есть буллеты — каждая строка = одна обязанность (как и было)
    bullet_lines = [ln for ln in lines if is_bullet_line(ln)]
    if bullet_lines:
        out = []
        for ln in bullet_lines:
            ln = clean_bullet_prefix(ln)
            ln = strip_headers(ln)
            if ln:
                out.append(ln)
        return out

    # 5) собираем в один текст
    s1 = " ".join(lines).strip()
    if not s1:
        return []

    # 6) КЛЮЧЕВОЕ: если был префикс "ОБОВ'ЯЗКИ:" — НЕ режем по запятым вообще
    if prefixed:
        parts = split_outside_parens(s1, ".;")
        return [p.strip(" -–—\t") for p in parts if p.strip(" -–—\t")]

    # 7) иначе — твоя эвристика (описание vs перечень)
    words = len(re.findall(r"\w+", s1))
    commas = s1.count(",")
    if words >= 10 and commas <= max(1, words // 12):
        parts = split_outside_parens(s1, ".;")
        return [p.strip(" -–—\t") for p in parts if p.strip(" -–—\t")]

    parts = split_outside_parens(s1, ",.;")
    return [p.strip(" -–—\t") for p in parts if p.strip(" -–—\t")]

def split_tail_parentheses(meta: str):
    """
    Возвращает:
      base_text (без хвостовых скобок),
      parens: список содержимого скобок в конце строки (по порядку)
    """
    s = (meta or "").strip()
    parens = []

    # снимаем цепочку " (...) (...) (...) " с конца
    while True:
        m = re.search(r"\s*\(([^()]*)\)\s*$", s)
        if not m:
            break
        parens.insert(0, m.group(1).strip())
        s = s[:m.start()].rstrip()

    return s, parens

def split_outside_parens(text: str, seps: str):
    out = []
    buf = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)

        if depth == 0 and ch in seps:
            piece = "".join(buf).strip()
            if piece:
                out.append(piece)
            buf = []
        else:
            buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out

def parse_one_line_date_entries(lines: List[str]) -> List["WorkItem"]:
    items = []
    for ln in lines:
        m = DATE_PAIR_LINE_RE.match(ln)
        if not m:
            continue

        start_raw = m.group(2)
        end_raw = m.group(4)
        rest = m.group(5).strip()

        start = normalize_date_token(start_raw)
        end = normalize_date_token(end_raw)

        # rest: "title, title2 ... COMPANY"
        # split по запятым в кандидаты ролей
        candidates = [x.strip() for x in rest.split(",") if x.strip()]

        # эвристика: последняя часть обычно содержит компанию (там кавычки, ТОВ/ДП/ООО и т.п.)
        company = None
        title = rest

        if len(candidates) >= 2:
            last = candidates[-1]
            low = last.lower()
            if any(tok in low for tok in OPF_TOKENS) or re.search(r"[«»\"“”]", last):
                company = last
                title = ", ".join(candidates[:-1])

        # если company все ещё None — попробуй вытащить “ТОВ …” из конца строки
        if company is None:
            m2 = re.search(r"((?:ТОВ|ООО|ДП|ПП|АТ|ПрАТ|ФОП)\s+.+)$", rest, re.IGNORECASE)
            if m2:
                company = m2.group(1).strip()
                title = rest[:m2.start()].strip(" -–—,")
        
        items.append(
            WorkItem(
                title=title,
                company=company,
                city=None,
                industry=None,
                start=start,
                end=end,
                months=0,  # месяцы можно посчитать отдельно, если хочешь
                duties=[],
                duties_text="",
                block_text=ln.strip(),
            )
        )
    return items

def parse_dates_meta_line(line: str) -> Tuple[str, str, int, str, str, str, str]:
    """
    Returns:
      start, end, months, company, city, industry, duties_hint_text
    duties_hint_text — хвост, который не удалось уверенно распарсить как company/city/industry (может пригодиться).
    """
    t = (line or "").strip()

    # 1) start/end
    dates = list(_RE_MMYYYY.finditer(t))
    start = end = ""
    if dates:
        start = _to_yyyy_mm(dates[0].group(1), dates[0].group(2))
    if len(dates) >= 2:
        end = _to_yyyy_mm(dates[1].group(1), dates[1].group(2))
    else:
        if re.search(r"(?i)\b(нині|тепер|сьогодні|present)\b", t):
            end = "present"

    end_norm = end  # то что ты распарсил

    # нормализация "по нині"/"present"
    if end_norm in {"present", "now"}:
        end_ym = _now_ym()
    else:
        # если у тебя end_norm уже "YYYY-MM" — оставь
        end_ym = end_norm

    months = calc_months(start, end_ym)

    # 2) отрезаем левую часть до закрывающей скобки длительности (если есть)
    #    обычно: "... (2 роки 5 місяців) <meta...>"
    meta_part = t
    m = re.search(r"\([^)]*\)\s*(.*)$", t)
    if m:
        meta_part = (m.group(1) or "").strip()
    else:
        # если скобок нет — пробуем отрезать после "по <...>"
        m2 = re.search(r"(?i)\b(по|до)\b\s*(?:\d{2}\.\d{4}|нині|тепер|сьогодні|present)\s*(.*)$", t)
        if m2:
            meta_part = (m2.group(2) or "").strip()

    meta_base, parens = split_tail_parentheses(meta_part)

    company = city = industry = ""
    duties_hint = ""

    # industry = последняя скобка
    industry = parens[-1].strip() if parens else None

    # city/region = первая скобка, если скобок >= 2
    region = parens[0].strip() if len(parens) >= 2 else None

    # теперь разбор meta_base как "company, city" или просто "company"
    parts = [p.strip() for p in meta_base.split(",") if p.strip()]

    company = None
    city = None

    if not parts:
        company = None
        city = None
    else:
        # пытаемся выделить city как последний сегмент, но только если он действительно city
        if len(parts) >= 2 and looks_like_city(parts[-1]):
            city = parts[-1]
            company = ", ".join(parts[:-1]).strip()
        else:
            company = ", ".join(parts).strip()

    # если region найден — это сильнее, чем city из запятой
    if region:
        city = region

    return start, end_norm, months, company, city, industry, duties_hint

@dataclass
class WorkItem:
    title: str = ""
    company: str = ""
    city: str = ""
    industry: str = ""      # <-- новое
    start: Optional[str] = None    # "YYYY-MM"
    end: Optional[str] = None      # "YYYY-MM" or "present"
    months: Optional[int] = None
    duties: List[str] = field(default_factory=list)   # <-- новое
    duties_text: str = ""          # не режем

    block_text: str = ""  # весь текст блока одной должности

@dataclass
class EduItem:
    place: str = ""
    degree: str = ""               # "бакалавр/магістр/..." если найдём
    specialty: str = ""
    start: Optional[str] = None
    end: Optional[str] = None
    months: Optional[int] = None
    extra: str = ""

def parse_work_experience_section(text: str) -> List["WorkItem"]:
    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln]

    items: List[WorkItem] = []

    # -----------------------------
    # 1) Primary parser: your current "title line" + "dates/meta line"
    # -----------------------------
    i = 0
    while i < len(lines):
        if not looks_like_title(lines[i]):
            i += 1
            continue

        title = lines[i].strip()

        if i + 1 >= len(lines) or not is_dates_meta_line(lines[i + 1]):
            i += 1
            continue

        line2 = lines[i + 1]
        start, end, months, company, city, industry, _ = parse_dates_meta_line(line2)

        j = i + 2
        duties_acc = []
        while j < len(lines):
            cur = lines[j].strip()
            nxt = lines[j + 1].strip() if j + 1 < len(lines) else ""

            # новый блок: title + datesmeta
            if looks_like_title(cur) and nxt and is_dates_meta_line(nxt):
                break

            # если неожиданно встретили datesmeta без title — стоп
            if is_dates_meta_line(cur):
                break

            # игнорируем заголовки обязанностей/качеств
            if is_ignorable_header_line(cur):
                j += 1
                continue

            duties_acc.append(cur)
            j += 1

        duties_text = " ".join(duties_acc).strip()

        # 1) ПЫТАЕМСЯ РАЗБИТЬ ОДИН БЛОК НА НЕСКОЛЬКО РОЛЕЙ
        role_segments = _split_duties_by_role_prefixes(title, duties_text)

        if role_segments:
            # создаём несколько WorkItem с одинаковыми датами/компанией/городом/индустрией
            for role_title, role_text in role_segments:
                print(role_text)
                role_duties = split_duties_strict_dot_semi(role_text)  # только . и ;
                items.append(
                    WorkItem(
                        title=role_title,
                        company=company,
                        city=city,
                        industry=industry,
                        start=start,
                        end=end,
                        months=int(months) if isinstance(months, int) else 0,
                        duties=role_duties,
                        duties_text=role_text,      # важно: именно роль-кусок
                        block_text=role_text,       # если ты добавил поле "весь текст блока" — сюда же
                    )
                )
        else:
            # 2) ОСТАЛЬНОЕ — КАК РАНЬШЕ
            duties = split_duties(duties_text)
            #duties, duties_text = duties_from_lines(duties_acc)

            block_lines = [title, line2] + duties_acc
            block_text = "\n".join(block_lines).strip()

            items.append(
                WorkItem(
                    title=title,
                    company=company,
                    city=city,
                    industry=industry,
                    start=start,
                    end=end,
                    months=int(months) if isinstance(months, int) else 0,
                    duties=duties,
                    duties_text=duties_text,
                    block_text=block_text,
                )
            )
        i = j

    # -----------------------------
    # 2) Fallback parser: "Title (2020–2025) Company (Industry)" format
    #    Only if primary found nothing
    # -----------------------------
    if not items:
        items2: List[WorkItem] = []
        k = 0
        while k < len(lines):
            ln = lines[k].strip()
            if not ln or is_ignorable_header_line(ln):
                k += 1
                continue

            parsed = parse_inline_title_dates_meta(ln)
            if not parsed:
                k += 1
                continue

            title, start, end, months, company, industry = parsed

            # collect duties until a new inline block or until a "date hint" line that likely starts a new block
            dlines = []
            k += 1
            while k < len(lines):
                cur = lines[k].strip()
                if not cur:
                    k += 1
                    continue

                # stop on ignorable header but do not include
                if is_ignorable_header_line(cur):
                    k += 1
                    continue

                # if new inline pattern starts -> new block
                if parse_inline_title_dates_meta(cur):
                    break

                # if we meet "dates-like" line, treat as new block marker as you requested
                # (this is a heuristic; helps on mixed formats)
                if _ANY_DATES_HINT_RE.search(cur) and looks_like_title(cur):
                    break

                dlines.append(cur)
                k += 1

            duties, duties_text = duties_from_lines(dlines)
            block_lines = [title, line2] + duties_acc
            block_text = "\n".join(block_lines).strip()
            
            items2.append(
                WorkItem(
                    title=title,
                    company=company,
                    city=None,
                    industry=industry,
                    start=start,
                    end=end,
                    months=int(months) if isinstance(months, int) else 0,
                    duties=duties,
                    duties_text=duties_text,
                    block_text=block_text,
                )
            )
        if not items2:
            items2 = parse_one_line_date_entries(lines)
        items = items2
    
    if not items:
        logger.debug("parse_work_experience_section: non-empty text but 0 items (len=%s)", len(text))

    return items

def parse_education_section(text: str, now: Optional[dt.date] = None) -> List[EduItem]:
    if not text:
        return []
    now = now or dt.date.today()

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    items: List[EduItem] = []
    cur: Optional[EduItem] = None

    # диапазон лет: "з 1983 по 1990"
    yr_re = re.compile(r"(?:з|с)\s+(\d{4})\s+(?:по|до)\s+(\d{4})", re.IGNORECASE)

    def flush():
        nonlocal cur
        if cur:
            items.append(cur)
            cur = None

    for ln in lines:
        m = yr_re.search(ln)
        if m:
            if not cur:
                cur = EduItem()
            y1, y2 = int(m.group(1)), int(m.group(2))
            start_dt = dt.date(y1, 1, 1)
            end_dt = dt.date(y2, 12, 1)
            cur.start = f"{y1:04d}-01"
            cur.end = f"{y2:04d}-12"
            cur.months = months_between(start_dt, end_dt)
            cur.degree = detect_degree(ln)
            cur.place, cur.specialty = parse_edu_place_specialty(ln)
            tail = ln[m.end():].strip(" ,—-")
            if tail:
                cur.extra = (cur.extra + " " + tail).strip()
            continue

        # эвристика: если строка похожа на “учреждение” — начнём новый элемент
        if len(ln) > 6 and (("університет" in ln.lower()) or ("інститут" in ln.lower()) or ("university" in ln.lower()) or ("academy" in ln.lower())):
            flush()
            cur = EduItem(place=ln)
            continue

        if cur:
            # specialty/degree обычно тут
            # не режем — сохраним в specialty/extra
            if not cur.specialty:
                cur.specialty = ln
            else:
                cur.extra = (cur.extra + " " + ln).strip()

    flush()
    return items

def detect_degree(text: str) -> str:
    t = (text or "").lower()
    for norm, keys in DEGREE_MAP:
        for k in keys:
            if k in t:
                return norm
    return ""

def parse_edu_place_specialty(line: str) -> tuple[str, str]:
    """
    Очень простой, но работающий для твоих примеров:
    - place: первое слово/аббревиатура до пробела (или до первой запятой)
    - specialty: после точки до запятой (если есть)
    """
    s = (line or "").strip()
    if not s:
        return "", ""

    # отрежем дату и скобки
    s = re.sub(r"\(\s*\d+.*?\)", "", s).strip()

    # place: до первой запятой или до "Киев/Київ" и т.п.
    head = s.split(",")[0].strip()

    # place часто первое слово (кгифк/кнту/нтуу...)
    place = head.split()[0].strip() if head else ""

    # specialty: кусок после точки до запятой
    specialty = ""
    if "." in head:
        after_dot = head.split(".", 1)[1].strip()
        specialty = after_dot

    return place, specialty