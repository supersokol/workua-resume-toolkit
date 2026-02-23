from __future__ import annotations

from workua_toolkit.config.settings import load_settings
from workua_toolkit.db.repo import PostgresRepo


def main():
    s = load_settings()
    repo = PostgresRepo(s.db.dsn_kwargs())
    repo.drop_tables()
    print("OK: dropped resumes")


if __name__ == "__main__":
    main()
