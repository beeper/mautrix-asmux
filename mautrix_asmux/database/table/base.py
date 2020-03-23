from typing import Optional, List

from pypika import Table, Query, Field, Parameter
import asyncpg
import attr

from mautrix.util.async_db import Database


class Base:
    table_name: str
    _pypika_table: Table
    _columns: List[str]
    db: Database

    @classmethod
    def init(cls, db: Database) -> None:
        cls.db = db
        cls._pypika_table = Table(cls.table_name)
        fields_dict = attr.fields_dict(cls)
        cls._columns = [key for key in fields_dict.keys() if not key.endswith("_")]

    async def insert(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        parameters = [Parameter(f"${n}") for n in range(1, len(self._columns) + 1)]
        values = [getattr(self, field) for field in self._columns]
        await conn.execute(Query.into(self._pypika_table)
                           .columns(self._columns)
                           .insert(parameters),
                           *values)
