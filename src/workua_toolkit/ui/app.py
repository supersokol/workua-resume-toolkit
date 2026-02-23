from __future__ import annotations

from workua_toolkit.logging.setup import setup_logging
setup_logging()

import json
import streamlit as st
import datetime as dt
from decimal import Decimal
from typing import Any

from workua_toolkit.config.settings import load_settings
from workua_toolkit.db.schema import migrate
from workua_toolkit.db.repo import PostgresRepo
from workua_toolkit.scraper.workua import WorkUAScraper, PayloadMode
from workua_toolkit.processing.processor import UniversalResumeProcessor

st.set_page_config(page_title="Work.ua Resume Toolkit", layout="wide")

def to_jsonable(x: Any) -> Any:
    if x is None:
        return None

    # dates
    if isinstance(x, (dt.date, dt.datetime)):
        return x.isoformat()

    # decimals (часто прилетают из БД/парсинга)
    if isinstance(x, Decimal):
        # можно float(x), но float может терять точность; для зарплат чаще ок
        return float(x)

    # containers
    if isinstance(x, dict):
        return {str(k): to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]
    if isinstance(x, set):
        return [to_jsonable(v) for v in x]

    return x

def _json_download(name: str, obj):
    s = json.dumps(obj, ensure_ascii=False, indent=2, default=str)
    st.download_button(f"Download {name}", data=s.encode("utf-8"), file_name=name, mime="application/json")


def main():
    settings = load_settings()
    repo = PostgresRepo(settings.db.dsn_kwargs())

    st.title("Work.ua Resume Toolkit")

    with st.sidebar:
        st.header("DB")
        if st.button("Init / migrate schema"):
            migrate(settings.db.dsn_kwargs())
            st.success("Schema ready.")

        if st.button("Drop table resumes"):
            repo.drop_tables()
            st.warning("Dropped.")

        st.divider()
        st.header("Scraper settings")
        st.caption("Defaults loaded from config/scraper_config.json and overridden by .env")
        st.code(json.dumps(settings.scraper.__dict__, ensure_ascii=False, indent=2), language="json")

    tabs = st.tabs(["Scrape", "Browse DB"])

    # ---------------- Scrape
    with tabs[0]:
        col1, col2 = st.columns([1, 1], gap="large")

        with col1:
            st.subheader("Scrape input")
            mode = st.selectbox("Payload mode", [m.value for m in PayloadMode], index=2)
            save_to_db = st.checkbox("Save to DB", value=True)
            process_now = st.checkbox("Process now (regex)", value=False)

            scrape_kind = st.radio("Scrape mode", ["Pages range", "Until N", "By URLs"])

            scraper = WorkUAScraper(settings.scraper)

            payloads = []
            if scrape_kind == "Pages range":
                page_from = st.number_input("page_from", min_value=1, value=1, step=1)
                page_to = st.number_input("page_to", min_value=1, value=2, step=1)
                skip_first = st.number_input("skip_first", min_value=0, value=0, step=1)
                limit = st.number_input("limit (0 = no limit)", min_value=0, value=0, step=1)
                if st.button("Run scrape"):
                    it = scraper.iter_resumes_by_pages(
                        int(page_from),
                        int(page_to),
                        payload_mode=PayloadMode(mode),
                        skip_first=int(skip_first),
                        limit=(None if int(limit) == 0 else int(limit)),
                    )
                    for p in it:
                        payloads.append(p)

            elif scrape_kind == "Until N":
                target_n = st.number_input("target_n", min_value=1, value=25, step=1)
                start_page = st.number_input("start_page", min_value=1, value=1, step=1)
                skip_first = st.number_input("skip_first", min_value=0, value=0, step=1)
                max_pages = st.number_input("max_pages (0 = until end)", min_value=0, value=0, step=1)
                if st.button("Run scrape"):
                    it = scraper.iter_resumes_until(
                        target_n=int(target_n),
                        start_page=int(start_page),
                        skip_first=int(skip_first),
                        max_pages=(None if int(max_pages) == 0 else int(max_pages)),
                        payload_mode=PayloadMode(mode),
                    )
                    for p in it:
                        payloads.append(p)

            else:
                urls_text = st.text_area("Resume URLs (one per line)", height=200)
                skip_first = st.number_input("skip_first", min_value=0, value=0, step=1)
                limit = st.number_input("limit (0 = no limit)", min_value=0, value=0, step=1)
                if st.button("Run scrape"):
                    urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
                    it = scraper.iter_resumes_by_urls(
                        urls,
                        payload_mode=PayloadMode(mode),
                        skip_first=int(skip_first),
                        limit=(None if int(limit) == 0 else int(limit)),
                    )
                    for p in it:
                        payloads.append(p)

            st.caption(f"Collected payloads: {len(payloads)}")

            if payloads:
                _json_download("payloads.json", payloads)

        with col2:
            st.subheader("Results")

            if payloads:
                processor = UniversalResumeProcessor(matcher=None)

                def fmt_date(x):
                    import datetime as dt
                    if not x:
                        return "—"
                    if isinstance(x, (dt.date, dt.datetime)):
                        return x.isoformat()
                    return str(x)

                def badge(v):
                    if v is True:
                        return "✅"
                    if v is False:
                        return "—"
                    return "?"

                for idx, p in enumerate(payloads[:50], start=1):
                    parsed = p.get("parsed") or {}
                    meta = p.get("meta") or {}

                    title = f"{idx}. {parsed.get('person_name','unknown')} — {parsed.get('position') or 'n/a'}"

                    with st.expander(title, expanded=False):

                        # ---------- header ----------
                        if p.get("source_url"):
                            st.markdown(f"[🔗 Open resume]({p['source_url']})")

                        st.caption(f"parse_mode: `{meta.get('parse_mode','—')}`")

                        # ---------- metrics ----------
                        resume_date = fmt_date(parsed.get("resume_date"))
                        salary = parsed.get("salary")
                        salary_txt = "—" if not salary else str(salary)

                        c1, c2, c3 = st.columns(3)

                        c1.metric("Resume date", resume_date)
                        c2.metric("Salary", salary_txt)

                        c3.markdown(
                            f"""
        **Employment**
        - Full-time: {badge(parsed.get("full_time"))}
        - Part-time: {badge(parsed.get("part_time"))}
        - Remote: {badge(parsed.get("from_home"))}
        """
                        )

                        # ---------- download ----------
                        st.markdown("**Download**")
                        _json_download(f"resume_{idx}_payload.json", p)

                        # ---------- tabs ----------
                        tab_raw, tab_cleaned, tab_parsed, tab_processed = st.tabs(
                            ["raw_html", "cleaned_text", "parsed", "processed"]
                        )

                        with tab_raw:
                            st.text_area("raw_html", value=p.get("raw_html") or "", height=200)

                        with tab_cleaned:
                            st.text_area("cleaned_text", value=p.get("cleaned_text") or "", height=200)


                        with tab_parsed:
                            st.json(to_jsonable(parsed))

                        with tab_processed:
                            if process_now:
                                processed = processor.process_payload(p)
                                st.json(to_jsonable(processed))
                            else:
                                st.info("Enable 'Process now' to compute processed_json.")

                        # ---------- save to DB ----------
                        if save_to_db:
                            try:
                                pp = to_jsonable(p)
                                inserted, rid = repo.upsert_payload(pp)

                                if process_now:
                                    processed = processor.process_payload(pp)
                                    repo.set_processed_json(p["source_url"], to_jsonable(processed))

                                st.success(f"Saved (id={rid}, inserted={inserted})")

                            except Exception as e:
                                st.error(f"DB error: {e}")

    # ---------------- Browse DB
    with tabs[1]:
        st.subheader("DB browser")
        stats = {}
        try:
            stats = repo.get_stats()
        except Exception as e:
            st.error(f"DB error: {e}")

        st.write(stats)

        page = st.number_input("page", min_value=1, value=1, step=1)
        page_size = st.number_input("page_size", min_value=1, max_value=200, value=25, step=1)

        try:
            rows = repo.list_resumes(page=int(page), page_size=int(page_size))
        except Exception as e:
            st.error(f"DB error: {e}")
            rows = []

        st.caption(f"Rows: {len(rows)}")

        for r in rows:
            parsed = r.get("parsed_json") or {}
            title = f"#{r.get('id')} {parsed.get('person_name','unknown')} — {parsed.get('position') or 'n/a'}"
            with st.expander(title, expanded=False):
                st.write(r.get("source_url"))
                st.write({"resume_date": r.get("resume_date"), "updated_at": r.get("updated_at")})
                tab_parsed, tab_processed = st.tabs(["parsed_json", "processed_json"])
                with tab_parsed:
                    st.json(parsed)
                with tab_processed:
                    st.json(r.get("processed_json") or {})
                _json_download(f"db_resume_{r.get('id')}.json", r)


if __name__ == "__main__":
    main()
