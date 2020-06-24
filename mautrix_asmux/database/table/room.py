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
from typing import Dict, Optional, ClassVar
from uuid import UUID

from attr import dataclass

from mautrix.types import RoomID

from .base import Base


@dataclass
class Room(Base):
    id: RoomID
    owner: UUID

    cache_by_id: ClassVar[Dict[RoomID, 'Room']] = {}

    def __attrs_post_init__(self) -> None:
        self.cache_by_id[self.id] = self

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]

    @classmethod
    async def get(cls, room_id: RoomID) -> Optional['Room']:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        row = await cls.db.fetchrow("SELECT id, owner FROM room WHERE id=$1", room_id)
        return Room(**row) if row else None

    async def insert(self) -> None:
        await self.db.execute("INSERT INTO room (id, owner) VALUES ($1, $2)", self.id, self.owner)

    async def delete(self) -> None:
        await self.db.execute("DELETE FROM room WHERE id=$1 AND owner=$2", self.id, self.owner)
