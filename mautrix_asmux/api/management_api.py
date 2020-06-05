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
from typing import Callable, Awaitable, Optional
from uuid import UUID
import logging
import json
import re

from aiohttp import web, ClientSession
from yarl import URL
from attr import asdict

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
        self.app.router.add_get("/user/{id}", self.get_user)
        self.app.router.add_put("/appservice/{id}", self.provision_appservice)
        self.app.router.add_put("/appservice/{owner}/{prefix}", self.provision_appservice)
        self.app.router.add_get("/appservice/{id}", self.get_appservice)
        self.app.router.add_get("/appservice/{owner}/{prefix}", self.get_appservice)
        self.app.router.add_delete("/appservice/{id}", self.delete_appservice)
        self.app.router.add_delete("/appservice/{owner}/{prefix}", self.delete_appservice)

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
            user = await User.find_by_api_token(auth)
            if not user:
                raise Error.invalid_auth_token
            req["user"] = user
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
                    "regex": f"#{prefix}_.+:{server_name}",
                    "exclusive": self.exclusive,
                }],
            },
            "url": az.address,
            "sender_localpart": f"{prefix}_{re.escape(az.bot)}",
            "rate_limited": True,
        }

    async def _get_appservice(self, req: web.Request) -> AppService:
        try:
            uuid = UUID(req.match_info["id"])
        except ValueError:
            raise Error.invalid_uuid
        except KeyError:
            owner, prefix = req.match_info["owner"], req.match_info["prefix"]
            if "user" in req and req["user"].id != owner:
                raise Error.appservice_access_denied
            az = await AppService.find(owner, prefix)
        else:
            az = await AppService.get(uuid)
        return self._error_wrap(req, az)

    @staticmethod
    def _error_wrap(req: web.Request, az: Optional[AppService]) -> AppService:
        if not az:
            if "user" in req:
                # Don't leak existence of UUIDs to users
                raise Error.appservice_access_denied
            raise Error.appservice_not_found
        elif "user" in req and req["user"].id != az.owner:
            raise Error.appservice_access_denied
        return az

    async def get_user(self, req: web.Request) -> web.Response:
        find_user_id = req.match_info["id"]
        if "user" in req:
            user = req["user"]
            if user.id != find_user_id:
                raise Error.user_access_denied
        else:
            if req.query.get("create", "0") in ("1", "true", "t", "y", "yes"):
                if not part_regex.fullmatch(find_user_id):
                    raise Error.invalid_owner
                user = await User.get_or_create(find_user_id)
            else:
                user = await User.get(find_user_id)
                if not user:
                    raise Error.user_not_found
        return web.json_response(asdict(user))

    async def get_appservice(self, req: web.Request) -> web.Response:
        az = await self._get_appservice(req)
        return web.json_response(self._make_registration(az))

    async def delete_appservice(self, req: web.Request) -> web.Response:
        az = await self._get_appservice(req)
        await az.delete()
        return web.Response(status=204)

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
            uuid = UUID(req.match_info["id"])
        except ValueError:
            raise Error.invalid_uuid
        except KeyError:
            owner, prefix = req.match_info["owner"], req.match_info["prefix"]
            if not part_regex.fullmatch(owner):
                raise Error.invalid_owner
            elif not part_regex.fullmatch(prefix):
                raise Error.invalid_prefix
            if "user" in req:
                user = req["user"]
                if user.id != owner:
                    raise Error.appservice_access_denied
            else:
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
            az = self._error_wrap(req, await AppService.get(uuid))
        if not az.created_:
            await az.set_address(data.get("address"))
        return web.json_response(self._make_registration(az), status=201 if az.created_ else 200)
