from __future__ import annotations

from workua_toolkit.logging.setup import setup_logging
setup_logging()

import argparse
import json
import sys
from typing import Optional

from workua_toolkit.config.settings import load_settings
from workua_toolkit.scraper.workua import WorkUAScraper, PayloadMode


def cmd_scrape(args: argparse.Namespace) -> int:
    setup_logging()
    s = load_settings()
    scraper = WorkUAScraper(s.scraper)

    payload_mode = PayloadMode[args.payload_mode]

    if args.urls:
        it = scraper.iter_resumes_by_urls(
            args.urls,
            payload_mode=payload_mode,
            limit=args.limit,
            skip_first=args.skip_first,
        )
    elif args.pages:
        page_from, page_to = args.pages
        it = scraper.iter_resumes_by_pages(
            page_from=page_from,
            page_to=page_to,
            payload_mode=payload_mode,
            limit=args.limit,
            skip_first=args.skip_first,
        )
    else:
        it = scraper.iter_resumes_until(
            target_n=args.target_n,
            start_page=args.start_page,
            skip_first=args.skip_first,
            max_pages=args.max_pages,
            payload_mode=payload_mode,
        )

    out_f = open(args.out, "w", encoding="utf-8") if args.out else None
    try:
        for p in it:
            line = json.dumps(p, ensure_ascii=False)
            if out_f:
                out_f.write(line + "\n")
            else:
                sys.stdout.write(line + "\n")
    finally:
        if out_f:
            out_f.close()

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="workua-toolkit",
        description="Work.ua Resume Toolkit CLI",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("scrape", help="Scrape resumes and output NDJSON")
    src = sp.add_mutually_exclusive_group(required=False)
    src.add_argument("--urls", nargs="+", help="Explicit resume URLs")
    src.add_argument("--pages", nargs=2, type=int, metavar=("FROM", "TO"), help="Inclusive pages range")

    sp.add_argument("--target-n", type=int, default=50, help="Scrape until N resumes (default: 50)")
    sp.add_argument("--start-page", type=int, default=1)
    sp.add_argument("--skip-first", type=int, default=0)
    sp.add_argument("--max-pages", type=int, default=None)

    sp.add_argument("--limit", type=int, default=None, help="Hard limit for pages/urls iterators")
    sp.add_argument(
        "--payload-mode",
        default="RAW_CLEANED_PARSED",
        choices=["RAW", "RAW_CLEANED", "RAW_CLEANED_PARSED"],
    )
    sp.add_argument("--out", default=None, help="Output file (NDJSON). If omitted -> stdout")
    sp.set_defaults(func=cmd_scrape)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
