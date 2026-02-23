from __future__ import annotations

from workua_toolkit.config.settings import load_settings
from workua_toolkit.db.schema import migrate


def main():
    s = load_settings()
    migrate(s.db.dsn_kwargs())
    print("OK: schema migrated")


if __name__ == "__main__":
    main()
