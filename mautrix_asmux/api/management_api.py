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
from typing import Callable, Awaitable, Tuple
from uuid import UUID
import logging
import json
import re

from aiohttp import web, ClientSession
from yarl import URL

from mautrix.types import JSON

from ..database import AppService, User
from ..config import Config
from .errors import Error

part_regex = re.compile("^[a-z0-9=.-]{1,32}$")

Handler = Callable[[web.Request], Awaitable[web.Response]]


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def custom_dumps(*args, **kwargs):
    return json.dumps(*args, **kwargs, cls=UUIDEncoder)


class ManagementAPI:
    log: logging.Logger = logging.getLogger("mau.api.management")
    http: ClientSession

    global_prefix: str
    exclusive: bool
    server_name: str
    shared_secret: str
    hs_address: URL
    as_token: str

    def __init__(self, config: Config, http: ClientSession) -> None:
        self.global_prefix = config["appservice.namespace.prefix"]
        self.exclusive = config["appservice.namespace.exclusive"]
        self.server_name = config["homeserver.domain"]
        self.shared_secret = config["mux.shared_secret"]
        self.hs_address = URL(config["homeserver.address"])
        self.as_token = config["appservice.as_token"]

        self.http = http

        self.app = web.Application(middlewares=[self.check_auth])
        self.app.router.add_put("/appservice/{id}", self.provision_appservice)
        self.app.router.add_put("/appservice/{owner}/{prefix}", self.provision_appservice)
        self.app.router.add_get("/appservice/{id}", self.get_appservice)
        self.app.router.add_get("/appservice/{owner}/{prefix}", self.get_appservice)

    @web.middleware
    async def check_auth(self, req: web.Request, handler: Handler) -> web.Response:
        try:
            auth = req.headers["Authorization"]
            if not auth.startswith("Bearer "):
                raise Error.invalid_auth_header
            auth = auth[len("Bearer "):]
        except KeyError:
            try:
                auth = req.query["access_token"]
            except KeyError:
                raise Error.missing_auth_header
        if auth != self.shared_secret:
            raise Error.invalid_auth_token
        return await handler(req)

    def _make_registration(self, az: AppService) -> JSON:
        prefix = f"{re.escape(self.global_prefix)}{re.escape(az.owner)}_{re.escape(az.prefix)}"
        server_name = re.escape(self.server_name)
        return {
            "id": str(az.id),
            "as_token": f"{az.id}-{az.as_token}",
            "hs_token": az.hs_token,
            "login_shared_secret": az.login_token,
            "namespaces": {
                "users": [{
                    "regex": f"@{prefix}_.+:{server_name}",
                    "exclusive": self.exclusive,
                }],
                "aliases": [{
                    "regex": f"@{prefix}_.+:{server_name}",
                    "exclusive": self.exclusive,
                }],
            },
            "url": az.address,
            "sender_localpart": f"{prefix}_{re.escape(az.bot)}",
            "rate_limited": True,
        }

    async def get_appservice(self, req: web.Request) -> web.Response:
        try:
            uuid = req.match_info["id"]
        except KeyError:
            owner, prefix = req.match_info["owner"], req.match_info["prefix"]
            az = await AppService.find(owner, prefix)
        else:
            az = await AppService.get(uuid)
        if not az:
            raise Error.appservice_not_found
        return web.json_response(self._make_registration(az))

    async def _register_as_bot(self, az: AppService) -> None:
        localpart = f"{self.global_prefix}{az.owner}_{az.prefix}_{az.bot}"
        url = (self.hs_address / "_matrix/client/r0/register").with_query({"kind": "user"})
        await self.http.post(url, json={"username": localpart}, headers={
            "Authorization": f"Bearer {self.as_token}"
        })

    async def provision_appservice(self, req: web.Request) -> web.Response:
        try:
            data = await req.json()
        except json.JSONDecodeError:
            raise Error.request_not_json
        try:
            uuid = req.match_info["id"]
        except KeyError:
            owner, prefix = req.match_info["owner"], req.match_info["prefix"]
            if not part_regex.fullmatch(owner):
                raise Error.invalid_owner
            elif not part_regex.fullmatch(prefix):
                raise Error.invalid_prefix
            user = await User.get_or_create(owner)
            az = await AppService.find_or_create(user, prefix, bot=data.get("bot", "bot"),
                                                 address=data.get("address", ""))
            az.login_token = user.login_token
            if az.created_:
                try:
                    await self._register_as_bot(az)
                except Exception:
                    self.log.warning(f"Failed to register bridge bot {owner}_{prefix}_{az.bot}",
                                     exc_info=True)
                self.log.info(f"Created appservice {owner}_{prefix}")
        else:
            az = await AppService.get(uuid)
            if not az:
                raise Error.appservice_not_found
        if not az.created_:
            await az.set_address(data.get("address"))
        return web.json_response(self._make_registration(az), status=201 if az.created_ else 200)
