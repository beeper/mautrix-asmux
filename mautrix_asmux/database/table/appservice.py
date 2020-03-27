# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Optional, List, Iterable
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

    @classmethod
    async def get(cls, id: UUID, *, conn: Optional[asyncpg.Connection] = None
                  ) -> Optional['AppService']:
        conn = conn or cls.db
        row = await conn.fetchrow("SELECT appservice.id AS id, owner, prefix, bot, address, "
                                  '       hs_token, as_token, "user".login_token AS login_token '
                                  'FROM appservice, "user" WHERE appservice.id=$1::uuid '
                                  '                            AND "user".id=appservice.owner', id)
        return AppService(**row) if row else None

    @classmethod
    async def find(cls, owner: str, prefix: str, *, conn: Optional[asyncpg.Connection] = None
                   ) -> Optional['AppService']:
        conn = conn or cls.db
        row = await conn.fetchrow("SELECT appservice.id AS id, owner, prefix, bot, address, "
                                  '       hs_token, as_token, "user".login_token AS login_token '
                                  'FROM appservice, "user" WHERE owner=$1 AND prefix=$2 '
                                  '                          AND "user".id=appservice.owner',
                                  owner, prefix)
        return AppService(**row) if row else None

    @staticmethod
    def _random(length: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    async def find_or_create(cls, user: User, prefix: str, *, bot: str = "bot", address: str = ""
                             ) -> 'AppService':
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
        rows = await conn.fetch("SELECT id, owner, prefix, bot, address, hs_token, as_token "
                                "FROM appservice WHERE id = ANY($1::uuid[])", ids)
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
        await conn.execute("DELETE FROM appservice WHERE id=$1", self.id)
