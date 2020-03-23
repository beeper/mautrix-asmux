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
from typing import Callable, Awaitable
from uuid import UUID
import json
import re

from aiohttp import web
import attr

from mautrix.types import JSON

from ..database import AppService
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
    global_prefix: str
    exclusive: bool
    server_name: str
    shared_secret: str

    def __init__(self, config: Config) -> None:
        self.global_prefix = config["appservice.namespace.prefix"]
        self.exclusive = config["appservice.namespace.exclusive"]
        self.server_name = config["homeserver.domain"]
        self.shared_secret = config["mux.shared_secret"]

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
            az = await AppService.find_or_create(owner, prefix, bot=data.get("bot", "bot"),
                                                 address=data.get("address", ""))
            # TODO register bot account and store device ID
        else:
            az = await AppService.get(uuid)
            if not az:
                raise Error.appservice_not_found
        if not az.created_:
            az.address = data.get("address", az.address)
            await az.update()
        return web.json_response(self._make_registration(az), status=201 if az.created_ else 200)
