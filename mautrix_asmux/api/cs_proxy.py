# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional, Any, Tuple, Callable, Awaitable, Dict, List, Union
from contextlib import asynccontextmanager
from collections import defaultdict
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
from mautrix.types import UserID, RoomID, EventType

from ..database import AppService, User
from .errors import Error

Handler = Callable[[web.Request], Awaitable[web.Response]]


class HTTPCustomError(web.HTTPError):
    def __init__(self, status_code: int, *args, **kwargs) -> None:
        self.status_code = status_code
        super().__init__(*args, **kwargs)


class ClientProxy:
    log: logging.Logger = logging.getLogger("mau.api.cs_proxy")
    http: aiohttp.ClientSession

    mxid_prefix: str
    mxid_suffix: str
    hs_address: URL
    as_token: str
    login_shared_secret: Optional[bytes]

    dm_locks: Dict[UserID, asyncio.Lock]
    user_ids: Dict[str, UserID]

    def __init__(self, mxid_prefix: str, mxid_suffix: str, hs_address: URL, as_token: str,
                 login_shared_secret: Optional[str], http: aiohttp.ClientSession) -> None:
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_address = hs_address.with_path("/_matrix")
        self.as_token = as_token
        self.login_shared_secret = (login_shared_secret.encode("utf-8")
                                    if login_shared_secret else None)
        self.dm_locks = defaultdict(lambda: asyncio.Lock())
        self.user_ids = {}
        self.http = http

        self.app = web.Application(middlewares=[self.cancel_logger])
        self.app.router.add_post("/client/r0/login", self.proxy_login)
        self.app.router.add_put("/client/unstable/net.maunium.asmux/dms", self.update_dms)
        self.app.router.add_patch("/client/unstable/net.maunium.asmux/dms", self.update_dms)
        self.app.router.add_route(hdrs.METH_ANY, "/{spec:(client|media)}/{path:.+}", self.proxy)

    @staticmethod
    def request_log_fmt(req: web.Request) -> str:
        url = req.get("proxy_url", req.url)
        proxy_for = req.get("proxy_for", "<unknown user>")
        cleaned_query = url.query.copy()
        try:
            del cleaned_query["access_token"]
        except KeyError:
            pass
        return f"{req.method} {url.with_query(cleaned_query).path_qs} for {proxy_for}"

    @web.middleware
    async def cancel_logger(self, req: web.Request, handler: Handler) -> web.Response:
        start = time.monotonic()
        try:
            return await handler(req)
        except asyncio.CancelledError:
            duration = round(time.monotonic() - start, 3)
            self.log.debug(f"Proxying request {self.request_log_fmt(req)}"
                           f" cancelled after {duration} seconds")
            raise

    @asynccontextmanager
    async def _get(self, url: URL, auth: str) -> web.Response:
        try:
            async with self.http.get(url, headers={"Authorization": f"Bearer {auth}"}) as resp:
                if resp.status >= 400:
                    raise HTTPCustomError(status_code=resp.status, headers=resp.headers,
                                          body=await resp.read())
                yield resp
        except aiohttp.ClientError:
            raise Error.failed_to_contact_homeserver

    @asynccontextmanager
    async def _put(self, url: URL, auth: str, json: Dict[str, Any]) -> web.Response:
        try:
            async with self.http.put(url, headers={"Authorization": f"Bearer {auth}"}, json=json
                                     ) as resp:
                if resp.status >= 400:
                    raise HTTPCustomError(status_code=resp.status, headers=resp.headers,
                                          body=await resp.read())
                yield resp
        except aiohttp.ClientError:
            raise Error.failed_to_contact_homeserver

    async def get_user_id(self, token: str) -> Optional[UserID]:
        token_hash = hashlib.sha512(token.encode("utf-8")).hexdigest()
        try:
            return self.user_ids[token_hash]
        except KeyError:
            url = self.hs_address / "client" / "r0" / "account" / "whoami"
            async with self._get(url, token) as resp:
                # TODO handle other errors?
                if resp.status == 401:
                    raise Error.invalid_auth_token
                user_id = self.user_ids[token_hash] = (await resp.json())["user_id"]
            return user_id

    def _remove_current_dms(self, dms: Dict[UserID, List[RoomID]], username: str, bridge: str
                            ) -> Dict[UserID, List[RoomID]]:
        prefix = f"{self.mxid_prefix}{username}_{bridge}_"
        suffix = self.mxid_suffix
        bot = f"{prefix}bot{suffix}"

        return {user_id: rooms for user_id, rooms in dms.items()
                if not (user_id.startswith(prefix) and user_id.endswith(suffix)
                        and user_id != bot)}

    async def update_dms(self, req: web.Request) -> web.Response:
        try:
            auth = req.headers["Authorization"]
            assert auth.startswith("Bearer ")
            auth = auth[len("Bearer "):]
        except (KeyError, AssertionError):
            raise Error.invalid_auth_header
        try:
            data = await req.json()
        except json.JSONDecodeError:
            raise Error.request_not_json

        user_id = await self.get_user_id(auth)
        az = await self._find_appservice(req, header="X-Asmux-Auth", raise_errors=True)
        if user_id != f"@{az.owner}{self.mxid_suffix}":
            raise Error.mismatching_user

        async with self.dm_locks[user_id]:
            url = (self.hs_address / "client" / "r0"
                   / "user" / user_id / "account_data" / str(EventType.DIRECT))
            async with self._get(url, auth) as resp:
                dms = await resp.json()
            if req.method == "PUT":
                dms = self._remove_current_dms(dms, az.owner, az.prefix)
            dms.update(data)
            async with self._put(url, auth, dms):
                return web.json_response({})

    async def proxy_login(self, req: web.Request) -> web.Response:
        body = await req.read()
        headers = req.headers
        json_body = await req.json()
        if json_body.get("type") != "m.login.password":
            return await self.proxy(req, _spec_override="client", _path_override="r0/login",
                                    _body_override=body)
        if await self.convert_login_password(json_body):
            body = json.dumps(json_body)
            headers = headers.copy()
            headers["Content-Type"] = "application/json"
            # TODO remove this everywhere (in _proxy)?
            del headers["Content-Length"]
        return await self._proxy(req, self.hs_address / "client/r0/login",
                                 body=body, headers=headers)

    async def proxy(self, req: web.Request, *, _spec_override: str = "", _path_override: str = "",
                    _body_override: Any = None) -> web.Response:
        spec: str = _spec_override or req.match_info["spec"]
        path: str = _path_override or req.match_info["path"]
        url = self.hs_address.with_path(req.raw_path.split("?", 1)[0], encoded=True)

        az = await self._find_appservice(req)
        if not az:
            req["proxy_for"] = "<no auth>"
            return await self._proxy(req, url, body=_body_override)
        req["proxy_for"] = f"{az.owner}/{az.prefix}"

        headers, query = self._copy_data(req, az)

        body = _body_override or req.content

        if spec == "client" and ((path == "r0/delete_devices" and req.method == "POST")
                                 or (path.startswith("r0/devices/") and req.method == "DELETE")):
            body = await req.read()
            json_body = await req.json()
            if await self.convert_login_password(json_body.get("auth", {}), az=az,
                                                 user_id=query["user_id"]):
                body = json.dumps(json_body).encode("utf-8")
        # Disabled at least for now, incoming remote events are fairly high-traffic especially for
        # users with big telegram accounts
        #
        # if ((spec == "client" and path.startswith("r0/rooms/")
        #      and ("/send/m.room.message/" in path or "/send/m.room.encrypted/" in path)
        #      and query["user_id"].startswith(f"{self.mxid_prefix}{az.owner}_{az.prefix}_")
        #      and query["user_id"] != f"{self.mxid_prefix}{az.owner}_{az.prefix}_"
        #                              f"{az.bot}{self.mxid_suffix}")):
        #     asyncio.ensure_future(track("Incoming remote event", f"@{az.owner}{self.mxid_suffix}",
        #                                 bridge_type=az.prefix, bridge_id=str(az.id)))

        return await self._proxy(req, url, headers, query, body)

    async def _proxy(self, req: web.Request, url: URL, headers: Optional[CIMultiDict[str]] = None,
                     query: Optional[MultiDict[str]] = None, body: Any = None) -> web.Response:
        try:
            req["proxy_url"] = url.with_query(query or req.query)
            resp = await self.http.request(req.method, url,
                                           headers=headers or req.headers.copy(),
                                           params=query or req.query.copy(),
                                           data=body or req.content)
        except aiohttp.ClientError as e:
            self.log.debug(f"{type(e)} proxying request {self.request_log_fmt(req)}: {e}")
            raise Error.failed_to_contact_homeserver
        if resp.status >= 400:
            self.log.debug(f"Got HTTP {resp.status} proxying request {self.request_log_fmt(req)}")
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
    async def _find_appservice(req: web.Request, header: str = "Authorization",
                               raise_errors: bool = False) -> Optional[AppService]:
        try:
            auth = req.headers[header]
            assert auth
            if not header.startswith("X-"):
                assert auth.startswith("Bearer ")
                auth = auth[len("Bearer "):]
            uuid = UUID(auth[:36])
            token = auth[37:]
        except (KeyError, AssertionError):
            if raise_errors:
                raise Error.missing_auth_header
            return None
        except ValueError:
            if raise_errors:
                raise Error.invalid_auth_token
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
