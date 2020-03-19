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
from typing import Iterable, Optional
from uuid import UUID

from attr import dataclass

from mautrix.types import RoomID

from .base import Base


@dataclass
class Room(Base):
    id: str
    owner: UUID

    @classmethod
    async def get(cls, room_id: RoomID) -> Optional['Room']:
        row = await cls.db.fetchrow("SELECT id, owner FROM room WHERE id=$1", room_id)
        return Room(**row) if row else None

    async def insert(self) -> None:
        await self.db.execute("INSERT INTO room (id, owner) VALUES ($1, $2)", self.id, self.owner)

    async def delete(self) -> None:
        await self.db.execute("DELETE FROM room WHERE id=$1 AND owner=$2", self.id, self.owner)
