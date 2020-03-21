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
from aiohttp import web, hdrs
from yarl import URL

from mautrix.types import JSON
from mautrix.appservice import AppServiceServerMixin

from .database import Room, AppService
from .config import Config


class MuxServer(AppServiceServerMixin):
    log: logging.Logger = logging.getLogger("mau.server")
    app: web.Application
    runner: web.AppRunner
    loop: asyncio.AbstractEventLoop
    http: aiohttp.ClientSession

    host: str
    port: int

    hs_domain: str
    hs_address: URL
    hs_token: str
    as_token: str
    global_prefix: str
    hs_suffix: str

    def __init__(self, config: Config, http: Optional[aiohttp.ClientSession],
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        super().__init__()
        self.host = config["mux.hostname"]
        self.port = config["mux.port"]
        self.hs_domain = config["homeserver.domain"]
        self.hs_address = URL(config["homeserver.address"])
        self.hs_token = config["appservice.hs_token"]
        self.as_token = config["appservice.as_token"]
        self.global_prefix = "@" + config["appservice.namespace.prefix"]
        self.hs_suffix = f":{self.hs_domain}"
        self.loop = loop or asyncio.get_event_loop()
        self.http = http
        self.app = web.Application()
        self.register_routes(self.app)
        self.app.router.add_route(hdrs.METH_ANY, "/_matrix/{spec:(client|media)}/{path:.+}",
                                  self.proxy)
        self.runner = web.AppRunner(self.app)

    async def start(self) -> None:
        self.log.debug("Starting web server")
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def stop(self) -> None:
        self.log.debug("Stopping web server")
        await self.runner.cleanup()

    async def proxy(self, request: web.Request) -> web.Response:
        try:
            auth = request.headers["Authorization"].lstrip("Bearer ")
            id = UUID(auth[:36])
            token = auth[37:]
        except (KeyError, AttributeError, IndexError, ValueError):
            return web.json_response(status=401, data={"error": "Missing or invalid auth header",
                                                       "errcode": "M_UNAUTHORIZED"})
        az = await AppService.get(id)
        if not az or az.as_token != token:
            return web.json_response(status=401, data={"error": "Incorrect auth token",
                                                       "errcode": "M_UNAUTHORIZED"})
        az_prefix = f"{self.global_prefix}{az.owner}_{az.prefix}_"

        query = request.query.copy()
        try:
            del query["access_token"]
        except KeyError:
            pass
        if "user_id" not in query:
            query["user_id"] = f"{az_prefix}{az.bot}{self.hs_suffix}"
        elif not query["user_id"].startswith(az_prefix):
            return web.json_response(status=403, data={
                "error": "Application service cannot masquerade as this local user.",
                "errcode": "M_FORBIDDEN"
            })
        elif not query["user_id"].endswith(self.hs_suffix):
            return web.json_response(status=403, data={
                "error": "Application service cannot masquerade as user on external homeserver.",
                "errcode": "M_FORBIDDEN",
            })

        headers = request.headers.copy()
        headers["Authorization"] = f"Bearer {self.as_token}"
        try:
            del headers["Host"]
        except KeyError:
            pass

        spec = request.match_info.get("spec", None)
        path = request.match_info.get("path", None)
        url = self.hs_address / "_matrix" / spec / path

        try:
            resp = await self.http.request(request.method, url, headers=headers,
                                           params=query, data=request.content)
        except aiohttp.ClientError:
            raise web.HTTPBadGateway(text="Failed to contact homeserver")
        return web.Response(status=resp.status, headers=resp.headers, body=resp.content)

    async def post_events(self, appservice: AppService, events: List[JSON], txn_id: str) -> None:
        url = URL(appservice.address) / "_matrix" / "app" / "v1" / "transactions" / txn_id
        resp = await self.http.put(url.with_query({"access_token": appservice.hs_token}),
                                   json={"events": events})
        if resp.status >= 400:
            self.log.warning(f"Failed to post events to {url}: {resp.status} {await resp.text()}")

    async def register_room(self, event: JSON) -> Optional[Room]:
        try:
            if ((event["type"] != "m.room.member"
                 or not event["state_key"].startswith(self.global_prefix))):
                return None
        except KeyError:
            return None
        localpart: str = event["state_key"].lstrip(self.global_prefix).rstrip(self.hs_suffix)
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
