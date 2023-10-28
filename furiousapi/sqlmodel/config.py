from typing import cast

from furiousapi.core.config import BaseConnectionSettings
from pydantic import BaseModel, Field, PostgresDsn


class PostgreSQLConnectionOptions(BaseModel):
    pass


class PostgreSQLConnectionSettings(BaseConnectionSettings[PostgresDsn, PostgreSQLConnectionOptions]):
    connection_string: PostgresDsn = cast(PostgresDsn, "postgresql+asyncpg://user@localhost:5432")
    options: PostgreSQLConnectionOptions = Field(default_factory=PostgreSQLConnectionOptions)
