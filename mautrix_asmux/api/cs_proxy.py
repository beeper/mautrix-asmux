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
from typing import Optional, Any, Tuple, Callable, Awaitable
from uuid import UUID
import asyncio
import logging
import hashlib
import hmac
import json
import time

import aiohttp
from aiohttp import web, hdrs
from yarl import URL
from multidict import CIMultiDict, MultiDict

from mautrix.client import ClientAPI
from mautrix.types import UserID

from ..database import AppService, User
from .errors import Error


Handler = Callable[[web.Request], Awaitable[web.Response]]


class ClientProxy:
    log: logging.Logger = logging.getLogger("mau.api.cs_proxy")
    http: aiohttp.ClientSession

    mxid_prefix: str
    mxid_suffix: str
    hs_address: URL
    as_token: str
    login_shared_secret: Optional[bytes]

    def __init__(self, mxid_prefix: str, mxid_suffix: str, hs_address: URL, as_token: str,
                 login_shared_secret: Optional[str], http: aiohttp.ClientSession) -> None:
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_address = hs_address.with_path("/_matrix")
        self.as_token = as_token
        self.login_shared_secret = (login_shared_secret.encode("utf-8")
                                    if login_shared_secret else None)
        self.http = http

        self.app = web.Application(middlewares=[self.cancel_logger])
        self.app.router.add_post("/client/r0/login", self.proxy_login)
        self.app.router.add_route(hdrs.METH_ANY, "/{spec:(client|media)}/{path:.+}", self.proxy)

    @web.middleware
    async def cancel_logger(self, req: web.Request, handler: Handler) -> web.Response:
        start = time.monotonic()
        try:
            return await handler(req)
        except asyncio.CancelledError:
            duration = round(time.monotonic() - start, 3)
            url = req.get("proxy_url", req.url)
            cleaned_query = url.query.copy()
            try:
                del cleaned_query["access_token"]
            except KeyError:
                pass
            self.log.debug(f"Proxying request {req.method} {url.with_query(cleaned_query).path_qs}"
                           f" cancelled after {duration} seconds")
            raise

    async def proxy_login(self, req: web.Request) -> web.Response:
        body = await req.read()
        headers = req.headers
        json_body = await req.json()
        if await self.convert_login_password(json_body):
            body = json.dumps(json_body)
            headers = headers.copy()
            headers["Content-Type"] = "application/json"
            # TODO remove this everywhere (in _proxy)?
            del headers["Content-Length"]
        return await self._proxy(req, self.hs_address / "client/r0/login",
                                 body=body, headers=headers)

    async def proxy(self, req: web.Request) -> web.Response:
        spec: str = req.match_info["spec"]
        path: str = req.match_info["path"]
        url = self.hs_address.with_path(req.raw_path.split("?", 1)[0])

        az = await self._find_appservice(req)
        if not az:
            return await self._proxy(req, url)

        headers, query = self._copy_data(req, az)

        body = req.content

        if spec == "client" and ((path == "r0/delete_devices" and req.method == "POST")
                                 or (path.startswith("r0/devices/") and req.method == "DELETE")):
            body = await req.read()
            json_body = await req.json()
            if await self.convert_login_password(json_body.get("auth", {}), az=az,
                                                 user_id=query["user_id"]):
                body = json.dumps(json_body).encode("utf-8")

        return await self._proxy(req, url, headers, query, body)

    async def _proxy(self, req: web.Request, url: URL, headers: Optional[CIMultiDict[str]] = None,
                     query: Optional[MultiDict[str]] = None, body: Any = None) -> web.Response:
        try:
            req["proxy_url"] = url.with_query(query or req.query)
            resp = await self.http.request(req.method, url,
                                           headers=headers or req.headers.copy(),
                                           params=query or req.query.copy(),
                                           data=body or req.content)
        except aiohttp.ClientError:
            raise Error.failed_to_contact_homeserver
        return web.Response(status=resp.status, headers=resp.headers, body=resp.content)

    async def _find_login_token(self, user_id: UserID) -> Optional[str]:
        if not user_id:
            return None
        elif user_id.startswith(self.mxid_prefix) and user_id.endswith(self.mxid_suffix):
            localpart = user_id[len(self.mxid_prefix):-len(self.mxid_suffix)]
            owner, prefix, _ = localpart.split("_", 2)
            az = await AppService.find(owner, prefix)
            if not az:
                return None
            return az.login_token
        else:
            localpart, _ = ClientAPI.parse_user_id(user_id)
            user = await User.get(localpart)
            if not user:
                return None
            return user.login_token

    async def convert_login_password(self, data: Any, az: Optional[AppService] = None,
                                     user_id: Optional[str] = None) -> bool:
        if not self.login_shared_secret:
            return False
        try:
            if data["type"] != "m.login.password":
                return False
            if not user_id:
                try:
                    user_id = data["user"]
                except KeyError:
                    if data["identifier"]["type"] != "m.id.user":
                        return False
                    user_id = data["identifier"]["user"]
            login_token = az.login_token if az else await self._find_login_token(user_id)
            if not login_token:
                return False
            expected_password = hmac.new(login_token.encode("utf-8"), user_id.encode("utf-8"),
                                         hashlib.sha512).hexdigest()
            if data["password"] == expected_password:
                data["password"] = hmac.new(self.login_shared_secret, user_id.encode("utf-8"),
                                            hashlib.sha512).hexdigest()
                return True
        except (KeyError, ValueError):
            pass
        return False

    @staticmethod
    async def _find_appservice(req: web.Request) -> Optional[AppService]:
        try:
            auth = req.headers["Authorization"]
            assert auth and auth.startswith("Bearer ")
            auth = auth[len("Bearer "):]
            uuid = UUID(auth[:36])
            token = auth[37:]
        #except KeyError:
        #    raise Error.missing_auth_header
        #except AssertionError:
        #    raise Error.invalid_auth_header
        #except ValueError:
        #    raise Error.invalid_auth_token
        except (KeyError, AssertionError, ValueError):
            return None
        az = await AppService.get(uuid)
        if not az or az.as_token != token:
            raise Error.invalid_auth_token
        return az

    def _copy_data(self, req: web.Request, az: AppService) -> Tuple[CIMultiDict, MultiDict]:
        query = req.query.copy()
        try:
            del query["access_token"]
        except KeyError:
            pass
        az_prefix = f"{self.mxid_prefix}{az.owner}_{az.prefix}_"
        if "user_id" not in query:
            query["user_id"] = f"{az_prefix}{az.bot}{self.mxid_suffix}"
        elif not query["user_id"].startswith(az_prefix):
            raise Error.invalid_user_id
        elif not query["user_id"].endswith(self.mxid_suffix):
            raise Error.external_user_id

        headers = req.headers.copy()
        headers["Authorization"] = f"Bearer {self.as_token}"
        try:
            del headers["Host"]
        except KeyError:
            pass

        return headers, query
