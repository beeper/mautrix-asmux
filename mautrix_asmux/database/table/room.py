# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
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

    def _add_to_cache(self) -> 'Room':
        self.cache_by_id[self.id] = self
        return self

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]

    @classmethod
    async def get(cls, room_id: RoomID) -> Optional['Room']:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        row = await cls.db.fetchrow("SELECT id, owner FROM room WHERE id=$1", room_id)
        return Room(**row)._add_to_cache() if row else None

    async def insert(self) -> None:
        await self.db.execute("INSERT INTO room (id, owner) VALUES ($1, $2)", self.id, self.owner)
        self._add_to_cache()

    async def delete(self) -> None:
        await self.db.execute("DELETE FROM room WHERE id=$1 AND owner=$2", self.id, self.owner)
        self._delete_from_cache()
