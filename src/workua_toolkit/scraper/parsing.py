from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import datetime as dt
import re
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

# Helper parsing utilities extracted from the original prototype.
# ---------------------------
# Helpers
# ---------------------------

_MONTH_UA = {
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6,
    "липня": 7, "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
}

_MONTH_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def _split_csv(v: Optional[str]) -> Optional[List[str]]:
    if not v:
        return None
    parts = [x.strip() for x in re.split(r"[,;•\n]+", v) if x.strip()]
    return parts or None

def normalize_ws(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def split_nonempty_lines(text: str) -> List[str]:
    return [normalize_ws(ln) for ln in (text or "").splitlines() if normalize_ws(ln)]

def strip_bullet(line: str) -> str:
    line = normalize_ws(line)
    return re.sub(r"^[•\-\*\u2022]\s*", "", line).strip()

def parse_is_veteran_from_raw_html(raw_html: str) -> bool:
    """
    Detects 'Ветеран' badge near the name.
    Best-effort: checks badge-like elements and text near h1.
    """
    if not raw_html:
        return False

    try:
        soup = BeautifulSoup(raw_html, "html.parser")
    except Exception:
        # fallback: plain text check
        return bool(re.search(r"\bветеран\b", raw_html, flags=re.IGNORECASE))

    # 1) check inside h1 block (often badge is within/near name block)
    h1 = soup.find("h1")
    if h1:
        txt = normalize_ws(h1.get_text(" ", strip=True))
        if re.search(r"\bветеран\b", txt, flags=re.IGNORECASE):
            return True
        # sometimes badge is a sibling/child span
        # check nearby (parent container)
        parent = h1.parent
        if parent:
            near_txt = normalize_ws(parent.get_text(" ", strip=True))
            if re.search(r"\bветеран\b", near_txt, flags=re.IGNORECASE):
                return True

    # 2) generic badge search (robust to markup changes)
    # look for elements that contain exactly/mostly "Ветеран"
    for el in soup.find_all(["span", "div", "a"]):
        t = normalize_ws(el.get_text(" ", strip=True))
        if not t:
            continue
        if re.fullmatch(r"ветеран", t, flags=re.IGNORECASE):
            return True

    return False


def html_to_text_keep_breaks(html: str) -> str:
    """
    Convert HTML to text preserving meaningful structure for downstream regex parsing:
    - h1/h2/h3/h4 as section boundaries
    - dt/dd -> "Key: Value"
    - li -> "• item"
    - normalize whitespace / collapse empty lines
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # 1) remove irrelevant tags
    for t in soup(["script", "style", "noscript"]):
        t.decompose()

    # 2) optionally drop clearly hidden UI blocks that produce noise
    #    (you can comment these out if you prefer to keep print-only variants)
    for t in soup.select(".hidden-print"):
        t.decompose()

    # 3) <br> -> newline
    for br in soup.find_all("br"):
        br.replace_with("\n")

    # 4) normalize dt/dd pairs into "Key: Value"
    #    This dramatically improves regex parsing later.
    for dt_tag in soup.find_all("dt"):
        key = dt_tag.get_text(" ", strip=True).rstrip(":")
        dd_tag = dt_tag.find_next_sibling("dd")
        val = dd_tag.get_text(" ", strip=True) if dd_tag else ""
        dt_tag.replace_with(f"\n{key}: {val}\n")
        if dd_tag:
            dd_tag.decompose()

    # 5) make headers act like section separators
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        txt = h.get_text(" ", strip=True)
        if txt:
            h.replace_with(f"\n\n{txt}\n")

    # 6) paragraphs as lines
    for p in soup.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt:
            p.replace_with(f"\n{txt}\n")

    # 7) list items as bullets
    for li in soup.find_all("li"):
        txt = li.get_text(" ", strip=True)
        if txt:
            li.replace_with(f"\n• {txt}\n")
        else:
            li.decompose()

    text = soup.get_text(separator="\n")

    # --- whitespace normalization ---
    text = text.replace("\xa0", " ")         # NBSP -> space
    text = text.replace("\u200b", "")        # zero-width space if present

    # trim spaces around newlines
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)

    # collapse multiple spaces
    text = re.sub(r"[ \t]{2,}", " ", text)

    # collapse excessive empty lines (keep at most one blank line)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # drop lines that are just bullets with nothing
    text = re.sub(r"\n•\s*\n", "\n", text)

    lines = [ln for ln in text.splitlines() if ln.strip() not in {"OK"}]
    text = "\n".join(lines)

    return text.strip()

def detect_cleaned_format(cleaned: str) -> str:
    t = (cleaned or "").lower()
    # structured – секционные заголовки
    if any(x in t for x in [
        "досвід роботи", "опыт работы",
        "освіта", "образование",
        "знання і навички", "навыки",
        "знання мов", "языки",
        "додаткова інформація", "дополнительная информация",
    ]):
        return "structured"
    # br-only эвристика: много двоеточий и мало заголовков
    colon_lines = sum(1 for ln in cleaned.splitlines() if ":" in ln)
    if colon_lines >= 5:
        return "br_only"
    return "unknown"

def extract_resume_id_from_url(url: str) -> Optional[str]:
    """
    Work.ua resume URL usually: https://www.work.ua/resumes/8592563/
    """
    m = re.search(r"/resumes/(\d+)/", url)
    return m.group(1) if m else None


def parse_time_datetime_attribute(soup: BeautifulSoup) -> Optional[dt.date]:
    """
    Primary: <time datetime="2026-01-26 03:50:33">...</time>
    """
    t = soup.select_one("time[datetime]")
    if not t:
        return None
    val = t.get("datetime", "").strip()
    if not val:
        return None
    # accept "YYYY-MM-DD ..." or "YYYY-MM-DD"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", val)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return dt.date(y, mo, d)
    except ValueError:
        return None

def parse_ua_ru_date_text(text: str) -> Optional[dt.date]:
    """
    Fallback: from visible text like:
      "Резюме від 26 січня 2026"
      "Резюме от 12 января 2026"
    """
    text = normalize_ws(text)
    # UA/RU pattern
    m = re.search(r"(\d{1,2})\s+([A-Za-zА-Яа-яІіЇїЄєҐґ]+)\s+(\d{4})", text)
    if not m:
        return None
    day = int(m.group(1))
    mon_name = m.group(2).lower()
    year = int(m.group(3))
    mon = _MONTH_UA.get(mon_name) or _MONTH_RU.get(mon_name)
    if not mon:
        return None
    try:
        return dt.date(year, mon, day)
    except ValueError:
        return None

def parse_resume_date_from_cleaned(cleaned: str) -> Optional[dt.date]:
    for ln in split_nonempty_lines(cleaned)[:20]:
        d = parse_ua_ru_date_text(ln)
        if d:
            return d
    return None

SECTION_STOP = {
        "досвід роботи",
        "освіта", 
        "додаткова освіта", 
        "знання і навички", 
        "знання мов",
        "рекомендації",
        "додаткова інформація",
        "контактна інформація", 
        "інвалідність"
}

def looks_like_section_title(s: str) -> bool:
    t = normalize_ws(s).lower()
    return any(x == t or x in t for x in SECTION_STOP)

def clean_person_name(name: str) -> str:
    s = normalize_ws(name)
    # remove "Ветеран" badge word if it got merged into h1/text
    s = re.sub(r"\bветеран\b", "", s, flags=re.IGNORECASE)
    s = normalize_ws(s)
    return s or "unknown"

def parse_name_position_from_cleaned(cleaned: str) -> Tuple[str, Optional[str]]:
    lines = split_nonempty_lines(cleaned)

    idx = 0
    if lines and ("резюме" in lines[0].lower()):
        idx = 1

    name = "unknown"
    pos = None

    for j in range(idx, min(idx + 12, len(lines))):
        ln = strip_bullet(lines[j])
        if not ln or looks_like_section_title(ln):
            continue

        # имя: 1-3 слова, только буквы/дефисы, без двоеточий
        if ":" in ln:
            continue
        if not re.search(r"[A-Za-zА-Яа-яІіЇїЄєҐґ]", ln):
            continue

        words = ln.split()
        if 1 <= len(words) <= 3 and len(ln) <= 80:
            name = ln

            # позиция: следующая строка, не секция, не метка, без двоеточия
            for k in range(j + 1, min(j + 6, len(lines))):
                cand = strip_bullet(lines[k])
                if not cand or looks_like_section_title(cand):
                    continue
                if ":" in cand:
                    continue
                pos = cand[:120]
                break
            break

    name = clean_person_name(ln)

    return name, pos

def find_label_value_2line(cleaned: str, label_variants: List[str]) -> Optional[str]:
    lines = split_nonempty_lines(cleaned)
    labels = [normalize_ws(v).lower().rstrip(":") for v in label_variants]

    for i in range(len(lines) - 1):
        a = strip_bullet(lines[i]).lower().rstrip(":")
        if any(lbl == a or lbl in a for lbl in labels):
            val = strip_bullet(lines[i + 1])
            return val or None
    return None

def find_label_value(cleaned: str, label_variants: List[str]) -> Optional[str]:
    """
    Supports both:
      - "Label: Value" on the same line
      - "Label" on one line and value on the next line
    """
    lines = split_nonempty_lines(cleaned)
    labels = [normalize_ws(v).lower().rstrip(":") for v in label_variants]

    for i, ln in enumerate(lines):
        s = strip_bullet(ln)
        sl = s.lower()

        # same-line: "Label: Value"
        if ":" in s:
            left, right = s.split(":", 1)
            left = left.strip().lower().rstrip(":")
            if any(lbl == left or lbl in left for lbl in labels):
                v = right.strip()
                return v or None

        # two-line fallback
        a = sl.rstrip(":")
        if any(lbl == a or lbl in a for lbl in labels):
            if i + 1 < len(lines):
                v = strip_bullet(lines[i + 1])
                return v or None

    return None

def parse_considered_positions_from_cleaned(cleaned: str) -> Optional[List[str]]:
    val = find_label_value(cleaned, ["Розглядає посади"])
    return _split_csv(val)

def parse_employment_from_cleaned(cleaned: str) -> Tuple[bool, bool, bool]:
    emp = find_label_value_2line(cleaned, ["Вид зайнятості", "Зайнятість"])
    if not emp:
        return False, False, False
    return parse_employment_flags_from_text(emp)

def parse_city_from_cleaned(cleaned: str) -> Optional[str]:
    return find_label_value_2line(cleaned, ["Місто проживання", "Місто"])

def parse_ready_to_work_from_cleaned(cleaned: str) -> Optional[List[str]]:
    val = find_label_value(cleaned, ["Готовий працювати"])
    return _split_csv(val)

def parse_age_from_cleaned(cleaned: str) -> Optional[int]:
    val = find_label_value_2line(cleaned, ["Вік"])
    if not val:
        return None
    m = re.search(r"(\d{1,2})", val)
    return int(m.group(1)) if m else None

def extract_section_text_by_title(cleaned: str, title_variants: List[str]) -> Optional[str]:
    lines = split_nonempty_lines(cleaned)
    titles = [normalize_ws(t).lower() for t in title_variants]

    start = None
    for i, ln in enumerate(lines):
        lnl = strip_bullet(ln).lower()
        if any(t in lnl for t in titles):
            start = i + 1
            break
    if start is None:
        return None

    out = []
    for j in range(start, len(lines)):
        l = strip_bullet(lines[j]).lower()
        if looks_like_section_title(l):
            break
        out.append(lines[j])

    txt = "\n".join(out).strip()
    return txt or None

def parse_salary_from_cleaned(cleaned: str) -> Optional[int]:
    """
    Tries to find salary in cleaned text.
    Handles:
      "... 100 000 грн"
      "... 45 000 грн"
      "... 100000 грн"
    Strategy:
      - search in first ~40 lines (top of resume)
      - pick first plausible match
    """
    lines = split_nonempty_lines(cleaned)[:40]

    for ln in lines:
        t = normalize_ws(ln.replace("\xa0", " "))

        # optional: skip section titles
        if looks_like_section_title(t):
            continue

        m = re.search(r"(\d[\d\s]{2,})\s*грн\b", t, flags=re.IGNORECASE)
        if not m:
            continue

        num = re.sub(r"\s+", "", m.group(1))
        try:
            val = int(num)
        except ValueError:
            continue

        # very loose sanity bounds (can adjust or remove)
        if 1_000 <= val <= 1_000_000:
            return val

    return None

def strip_salary_tail_from_position(pos: Optional[str]) -> Optional[str]:
    if not pos:
        return pos
    t = normalize_ws(pos.replace("\xa0", " "))

    # remove ", 100 000 грн" / "— 100 000 грн" / " 100 000 грн" tail
    t2 = re.sub(r"[\s,–—-]*\d[\d\s]{2,}\s*грн\b.*$", "", t, flags=re.IGNORECASE).strip()
    return t2 or None

def parse_salary_from_h2(h2_text: str) -> Tuple[str, Optional[int]]:
    """
    Example: "Водій (виїзний), 90 000 грн"
    or "... 90 000 грн" with NBSP.
    Returns (position_text, salary_int|None)
    """
    if not h2_text:
        return "", None
    txt = h2_text.replace("\xa0", " ")
    txt = normalize_ws(txt)

    # salary often after comma
    # find number + грн
    m = re.search(r"(\d[\d\s]*)\s*грн", txt, flags=re.IGNORECASE)
    salary = None
    if m:
        num = re.sub(r"\s+", "", m.group(1))
        try:
            salary = int(num)
        except ValueError:
            salary = None

    # position = text before salary fragment if present
    # try split by comma if typical format
    if "," in txt:
        pos = txt.split(",")[0].strip()
    else:
        # also sometimes salary in <span class="text-muted-print">
        pos = re.sub(r"\s*\d[\d\s]*\s*грн.*$", "", txt, flags=re.IGNORECASE).strip()
    return pos, salary

def parse_employment_flags_from_text(text: str) -> Tuple[bool, bool, bool]:
    """
    "Вид зайнятості: повна, неповна"
    Also on RU pages: "полная занятость", "неполная", "удаленная"
    """
    t = (text or "").lower()
    full_time = ("повна" in t) 
    part_time = ("неповна" in t) 
    from_home = ("віддал" in t) 
    return full_time, part_time, from_home

def parse_disability_from_cleaned(cleaned: str) -> Optional[str]:
    sec = extract_section_text_by_title(cleaned, ["Інвалідність"])
    if not sec:
        return None
    lines = split_nonempty_lines(sec)
    return lines[0] if lines else None

def parse_bullets(section_text: Optional[str]) -> List[str]:
    if not section_text:
        return []
    items = []
    for ln in split_nonempty_lines(section_text):
        ln2 = strip_bullet(ln)
        if ln2:
            items.append(ln2)
    # dedup preserve order
    return list(dict.fromkeys(items))

# ---------------------------
# Scraper
# ---------------------------

