# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional, List, Iterable, Dict, Tuple, ClassVar
from uuid import UUID, uuid4
import random
import string

from attr import dataclass
import asyncpg

from .base import Base
from .user import User


@dataclass
class AppService(Base):
    id: UUID
    owner: str
    prefix: str

    bot: str
    address: str
    hs_token: str
    as_token: str

    login_token: Optional[str] = None
    created_: bool = False

    cache_by_id: ClassVar[Dict[UUID, 'AppService']] = {}
    cache_by_owner: ClassVar[Dict[Tuple[str, str], 'AppService']] = {}

    def __attrs_post_init__(self) -> None:
        self.cache_by_id[self.id] = self
        self.cache_by_owner[(self.owner, self.prefix)] = self

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]
        del self.cache_by_owner[(self.owner, self.prefix)]

    @classmethod
    async def get(cls, id: UUID, *, conn: Optional[asyncpg.Connection] = None
                  ) -> Optional['AppService']:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT appservice.id, owner, prefix, bot, address, '
                                  '       hs_token, as_token, "user".login_token '
                                  'FROM appservice JOIN "user" ON "user".id=appservice.owner '
                                  'WHERE appservice.id=$1::uuid', id)
        return AppService(**row) if row else None

    @classmethod
    async def find(cls, owner: str, prefix: str, *, conn: Optional[asyncpg.Connection] = None
                   ) -> Optional['AppService']:
        try:
            return cls.cache_by_owner[(owner, prefix)]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT appservice.id, owner, prefix, bot, address, hs_token, '
                                  '       as_token, "user".login_token '
                                  'FROM appservice JOIN "user" ON "user".id=appservice.owner '
                                  'WHERE owner=$1 AND prefix=$2 ',
                                  owner, prefix)
        return AppService(**row) if row else None

    @staticmethod
    def _random(length: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    async def find_or_create(cls, user: User, prefix: str, *, bot: str = "bot", address: str = ""
                             ) -> 'AppService':
        try:
            return cls.cache_by_owner[(user.id, prefix)]
        except KeyError:
            pass
        async with cls.db.acquire() as conn, conn.transaction():
            az = await cls.find(user.id, prefix, conn=conn)
            if not az:
                uuid = uuid4()
                hs_token = cls._random(64)
                # The input AS token also contains the UUID, so we want this to be a bit shorter
                as_token = cls._random(27)
                az = AppService(id=uuid, owner=user.id, prefix=prefix, bot=bot, address=address,
                                hs_token=hs_token, as_token=as_token, login_token=user.login_token)
                az.created_ = True
                await az.insert(conn=conn)
            return az

    @classmethod
    async def get_many(cls, ids: List[UUID], *, conn: Optional[asyncpg.Connection] = None
                       ) -> Iterable['AppService']:
        conn = conn or cls.db
        rows = await conn.fetch('SELECT appservice.id, owner, prefix, bot, address, hs_token,'
                                '       as_token, "user".login_token '
                                'FROM appservice JOIN "user" ON "user".id=appservice.owner '
                                'WHERE appservice.id = ANY($1::uuid[])', ids)
        return (AppService(**row) for row in rows)

    async def insert(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        await conn.execute("INSERT INTO appservice "
                           "(id, owner, prefix, bot, address, hs_token, as_token) "
                           "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                           self.id, self.owner, self.prefix, self.bot, self.address,
                           self.hs_token, self.as_token)

    async def set_address(self, address: str, *,
                          conn: Optional[asyncpg.Connection] = None) -> None:
        if not address or self.address == address:
            return
        self.address = address
        conn = conn or self.db
        await conn.execute("UPDATE appservice SET address=$2 WHERE id=$1", self.id, self.address)

    async def delete(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        self._delete_from_cache()
        await conn.execute("DELETE FROM appservice WHERE id=$1", self.id)
