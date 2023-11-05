from typing import cast

from pydantic import BaseModel, Field, PostgresDsn as _PostgresDsn

from furiousapi.core.config import BaseConnectionSettings


class PostgresDsn(_PostgresDsn):
    user_required = False


class PostgreSQLConnectionOptions(BaseModel):
    pass


class PostgreSQLConnectionSettings(BaseConnectionSettings[PostgresDsn, PostgreSQLConnectionOptions]):
    connection_string: PostgresDsn = cast(PostgresDsn, "postgresql+asyncpg://user@localhost:5432")
    options: PostgreSQLConnectionOptions = Field(default_factory=PostgreSQLConnectionOptions)
