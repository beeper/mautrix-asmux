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
from typing import Optional
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

    @classmethod
    async def get(cls, id: str, *, conn: Optional[asyncpg.Connection] = None
                  ) -> Optional['User']:
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT id, api_token, login_token FROM "user" WHERE id=$1', id)
        return User(**row) if row else None

    @classmethod
    async def find_by_api_token(cls, api_token: str, *, conn: Optional[asyncpg.Connection] = None
                                ) -> Optional['User']:
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT id, api_token, login_token FROM "user" '
                                  'WHERE api_token=$1', api_token)
        return User(**row) if row else None

    @staticmethod
    def _random(length: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    async def get_or_create(cls, id: str) -> 'User':
        async with cls.db.acquire() as conn, conn.transaction():
            user = await cls.get(id, conn=conn)
            if not user:
                user = User(id=id, api_token=cls._random(64), login_token=cls._random(64))
                await user.insert(conn=conn)
            return user

    async def insert(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        await conn.execute('INSERT INTO "user" (id, api_token, login_token) VALUES ($1, $2, $3)',
                           self.id, self.api_token, self.login_token)

    async def delete(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        await conn.execute('DELETE FROM "user" WHERE id=$1', self.id)
