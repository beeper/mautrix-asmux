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
from uuid import UUID
import logging

import aiohttp
from aiohttp import web, hdrs
from yarl import URL

from ..database import AppService
from .errors import Error


class ClientProxy:
    log: logging.Logger = logging.getLogger("mau.proxy.cs")
    http: aiohttp.ClientSession

    mxid_prefix: str
    mxid_suffix: str
    hs_address: URL
    as_token: str

    def __init__(self, mxid_prefix: str, mxid_suffix: str, hs_address: URL, as_token: str,
                 http: aiohttp.ClientSession) -> None:
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_address = hs_address
        self.as_token = as_token
        self.http = http

        self.app = web.Application()
        self.app.router.add_route(hdrs.METH_ANY, "/{spec:(client|media)}/{path:.+}", self.proxy)

    async def proxy(self, request: web.Request) -> web.Response:
        try:
            auth = request.headers["Authorization"]
            assert auth and auth.startswith("Bearer ")
            auth = auth.lstrip("Bearer ")
            uuid = UUID(auth[:36])
            token = auth[37:]
        except KeyError:
            raise Error.missing_auth_header
        except AssertionError:
            raise Error.invalid_auth_header
        except ValueError:
            raise Error.invalid_auth_token
        az = await AppService.get(uuid)
        if not az or az.as_token != token:
            raise Error.invalid_auth_token
        az_prefix = f"{self.mxid_prefix}{az.owner}_{az.prefix}_"

        query = request.query.copy()
        try:
            del query["access_token"]
        except KeyError:
            pass
        if "user_id" not in query:
            query["user_id"] = f"{az_prefix}{az.bot}{self.mxid_suffix}"
        elif not query["user_id"].startswith(az_prefix):
            raise Error.invalid_user_id
        elif not query["user_id"].endswith(self.mxid_suffix):
            raise Error.external_user_id

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
            raise Error.failed_to_contact_homeserver
        return web.Response(status=resp.status, headers=resp.headers, body=resp.content)
