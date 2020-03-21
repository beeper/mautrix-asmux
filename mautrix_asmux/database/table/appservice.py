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
from uuid import UUID

from attr import dataclass

from .base import Base


@dataclass
class AppService(Base):
    id: UUID
    owner: str
    prefix: str

    bot: str
    address: str
    hs_token: str
    as_token: str

    @classmethod
    async def get(cls, id: UUID) -> Optional['AppService']:
        row = await cls.db.fetchrow("SELECT id, owner, prefix, bot, address, hs_token, as_token "
                                    "FROM appservice WHERE id=$1", id)
        return AppService(**row) if row else None

    @classmethod
    async def find(cls, owner: str, prefix: str) -> Optional['AppService']:
        row = await cls.db.fetchrow("SELECT id, owner, prefix, bot, address, hs_token, as_token "
                                    "FROM appservice WHERE owner=$1 AND prefix=$2", owner, prefix)
        return AppService(**row) if row else None

    @classmethod
    async def get_many(cls, ids: List[UUID]) -> Iterable['AppService']:
        rows = await cls.db.fetch("SELECT id, owner, prefix, bot, address, hs_token, as_token "
                                  "FROM appservice WHERE id = ANY($1::uuid[])", ids)
        return (AppService(**row) for row in rows)
