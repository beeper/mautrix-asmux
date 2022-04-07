# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import ClassVar, Dict, Optional
from uuid import UUID

from attr import dataclass

from mautrix.types import RoomID

from .base import Base


@dataclass
class Room(Base):
    id: RoomID
    owner: UUID
    deleted: bool

    cache_by_id: ClassVar[Dict[RoomID, "Room"]] = {}

    @classmethod
    def empty_cache(cls) -> None:
        cls.cache_by_id = {}

    def _add_to_cache(self) -> "Room":
        self.cache_by_id[self.id] = self
        return self

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]

    @classmethod
    async def get(cls, room_id: RoomID) -> Optional["Room"]:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        row = await cls.db.fetchrow("SELECT id, owner, deleted FROM room WHERE id=$1", room_id)
        return Room(**row)._add_to_cache() if row else None

    async def insert(self) -> None:
        q = "INSERT INTO room (id, owner, deleted) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING"
        await self.db.execute(q, self.id, self.owner, self.deleted)
        self._add_to_cache()

    async def delete(self) -> None:
        await self.db.execute("DELETE FROM room WHERE id=$1 AND owner=$2", self.id, self.owner)
        self._delete_from_cache()

    async def mark_deleted(self) -> None:
        self.deleted = True
        await self.db.execute("UPDATE room SET deleted=$2 WHERE id=$1", self.id, self.deleted)
