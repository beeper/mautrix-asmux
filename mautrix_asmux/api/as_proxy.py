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
from typing import Optional, List, Dict
from collections import defaultdict
from uuid import UUID
import logging
import asyncio

import aiohttp
from yarl import URL

from mautrix.types import JSON
from mautrix.appservice import AppServiceServerMixin

from ..database import Room, AppService


class AppServiceProxy(AppServiceServerMixin):
    log: logging.Logger = logging.getLogger("mau.proxy.as")
    loop: asyncio.AbstractEventLoop
    http: aiohttp.ClientSession

    hs_token: str
    mxid_prefix: str
    mxid_suffix: str

    def __init__(self, mxid_prefix: str, mxid_suffix: str, hs_token: str,
                 http: aiohttp.ClientSession, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.loop = loop
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_token = hs_token
        self.http = http

    async def post_events(self, appservice: AppService, events: List[JSON], txn_id: str) -> None:
        url = URL(appservice.address) / "_matrix" / "app" / "v1" / "transactions" / txn_id
        resp = await self.http.put(url.with_query({"access_token": appservice.hs_token}),
                                   json={"events": events})
        if resp.status >= 400:
            self.log.warning(f"Failed to post events to {url}: {resp.status} {await resp.text()}")

    async def register_room(self, event: JSON) -> Optional[Room]:
        try:
            if ((event["type"] != "m.room.member"
                 or not event["state_key"].startswith(self.mxid_prefix))):
                return None
        except KeyError:
            return None
        localpart: str = event["state_key"].lstrip(self.mxid_prefix).rstrip(self.mxid_suffix)
        try:
            owner, prefix, _ = localpart.split("_", 2)
        except ValueError:
            return None
        az = await AppService.find(owner, prefix)
        room = Room(id=event["room_id"], owner=az.id)
        await room.insert()
        return room

    async def handle_transaction(self, transaction_id: str, events: List[JSON]) -> None:
        data: Dict[UUID, List[JSON]] = defaultdict(lambda: [])
        for event in events:
            room = await Room.get(event["room_id"])
            if not room:
                room = await self.register_room(event)
            if room:
                data[room.owner].append(event)
        ids = await AppService.get_many(list(data.keys()))
        await asyncio.gather(*[self.post_events(appservice, events, transaction_id)
                               for appservice, events in zip(ids, data.values())], loop=self.loop)