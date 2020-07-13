# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional, Dict, ClassVar
import random
import string

from attr import dataclass
import asyncpg

from .base import Base


@dataclass
class User(Base):
    id: str
    api_token: str
    login_token: str
    manager_url: Optional[str]

    cache_by_id: ClassVar[Dict[str, 'User']] = {}
    cache_by_api_token: ClassVar[Dict[str, 'User']] = {}

    def __attrs_post_init__(self) -> None:
        self.cache_by_id[self.id] = self
        self.cache_by_api_token[self.api_token] = self

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]
        del self.cache_by_api_token[self.api_token]

    @classmethod
    async def get(cls, id: str, *, conn: Optional[asyncpg.Connection] = None
                  ) -> Optional['User']:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT id, api_token, login_token, manager_url '
                                  'FROM "user" WHERE id=$1', id)
        return User(**row) if row else None

    @classmethod
    async def find_by_api_token(cls, api_token: str, *, conn: Optional[asyncpg.Connection] = None
                                ) -> Optional['User']:
        try:
            return cls.cache_by_api_token[api_token]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT id, api_token, login_token, manager_url FROM "user" '
                                  'WHERE api_token=$1', api_token)
        return User(**row) if row else None

    @staticmethod
    def _random(length: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    async def get_or_create(cls, id: str) -> 'User':
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        async with cls.db.acquire() as conn, conn.transaction():
            user = await cls.get(id, conn=conn)
            if not user:
                user = User(id=id, api_token=cls._random(64), login_token=cls._random(64),
                            manager_url=None)
                await user.insert(conn=conn)
            return user

    async def insert(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        await conn.execute('INSERT INTO "user" (id, api_token, login_token, manager_url) '
                           'VALUES ($1, $2, $3, $4)', self.id, self.api_token, self.login_token,
                           self.manager_url)

    async def edit(self, manager_url: str, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        await conn.execute('UPDATE "user" SET manager_url=$1 WHERE id=$2', manager_url, self.id)
        self.manager_url = manager_url

    async def delete(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        self._delete_from_cache()
        await conn.execute('DELETE FROM "user" WHERE id=$1', self.id)
