import os

# Async URL used at request time via dependencies.engine. Override via env to
# point the example at any SQLAlchemy-compatible database (Postgres, MySQL,
# Memory SQLite, etc.). The sync variant is only used at startup for
# `metadata.create_all` since SQLAlchemy's `create_all` requires a sync engine.
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///test_db_example.sqlite")
DATABASE_URL_SYNC = os.environ.get("DATABASE_URL_SYNC", "sqlite:///test_db_example.sqlite")
