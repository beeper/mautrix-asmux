# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Mapping, Optional, cast
from contextlib import asynccontextmanager
from uuid import UUID
import asyncio
import hashlib
import hmac
import json
import logging
import time

from aiohttp import hdrs, web
from aioredis import Redis
from aioredis.lock import Lock
from multidict import CIMultiDict, MultiDict
from yarl import URL
import aiohttp

from mautrix.client import ClientAPI
from mautrix.types import EventType, RoomID, UserID
from mautrix.util.opt_prometheus import Counter

from ..database import AppService, Room, User
from ..util import copy_headers_no_host
from .errors import Error

if TYPE_CHECKING:
    from ..server import MuxServer

Handler = Callable[[web.Request], Awaitable[web.Response]]

REQUESTS_RECEIVED = Counter(
    "asmux_requests_received",
    "Number of client-server API requests received from bridges",
    labelnames=["owner", "bridge", "method", "endpoint"],
)
REQUESTS_RECEIVED_AGGREGATE = Counter(
    "asmux_requests_received_aggregate",
    "Number of client-server API requests received from bridges (without user info)",
    labelnames=["bridge", "method", "endpoint"],
)
REQUESTS_HANDLED = Counter(
    "asmux_requests_handled",
    "Number of client-server API requests handled",
    labelnames=["owner", "bridge", "method", "endpoint"],
)
REQUESTS_HANDLED_AGGREGATE = Counter(
    "asmux_requests_handled_aggregate",
    "Number of client-server API requests handled (without user info)",
    labelnames=["bridge", "method", "endpoint"],
)


class HTTPCustomError(web.HTTPError):
    def __init__(self, status_code: int, headers: Mapping[str, str], *args, **kwargs) -> None:
        self.status_code = status_code
        headers = {**headers}
        headers.pop("Transfer-Encoding")
        super().__init__(*args, **kwargs, headers=headers)


class ClientProxy:
    log: logging.Logger = logging.getLogger("mau.api.cs_proxy")
    http: aiohttp.ClientSession

    mxid_prefix: str
    mxid_suffix: str
    hs_address: URL
    as_token: str
    login_shared_secret: Optional[bytes]

    dm_locks: dict[UserID, Lock]
    user_ids: dict[str, UserID]

    def __init__(
        self,
        server: "MuxServer",
        mxid_prefix: str,
        mxid_suffix: str,
        hs_address: URL,
        as_token: str,
        login_shared_secret: Optional[str],
        http: aiohttp.ClientSession,
        redis: Redis,
    ) -> None:
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_address = hs_address.with_path("/_matrix")
        self.as_token = as_token
        self.login_shared_secret = (
            login_shared_secret.encode("utf-8") if login_shared_secret else None
        )
        self.dm_locks = {}
        self.user_ids = {}
        self.http = http
        self.redis = redis

        self.app = web.Application(middlewares=[self.cancel_logger])  # type: ignore
        self.app.router.add_post("/client/r0/login", self.proxy_login)
        self.app.router.add_post("/client/v3/login", self.proxy_login)
        self.app.router.add_get("/client/unstable/fi.mau.as_sync", server.as_websocket.handle_ws)
        self.app.router.add_put("/client/unstable/com.beeper.asmux/dms", self.update_dms)
        self.app.router.add_patch("/client/unstable/com.beeper.asmux/dms", self.update_dms)
        # Deprecated, use com.beeper.asmux
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
            self.log.debug(
                f"Proxying request {self.request_log_fmt(req)}"
                f" cancelled after {duration} seconds"
            )
            raise

    @asynccontextmanager
    async def _get(
        self, url: URL, auth: str, raise_error: bool = True
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        try:
            async with self.http.get(url, headers={"Authorization": f"Bearer {auth}"}) as resp:
                if raise_error and resp.status >= 400:
                    raise HTTPCustomError(
                        status_code=resp.status, headers=resp.headers, body=await resp.read()
                    )
                yield resp
        except aiohttp.ClientError:
            raise Error.failed_to_contact_homeserver

    @asynccontextmanager
    async def _put(
        self, url: URL, auth: str, json: dict[str, Any]
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        try:
            async with self.http.put(
                url, headers={"Authorization": f"Bearer {auth}"}, json=json
            ) as resp:
                if resp.status >= 400:
                    raise HTTPCustomError(
                        status_code=resp.status, headers=resp.headers, body=await resp.read()
                    )
                yield resp
        except aiohttp.ClientError:
            raise Error.failed_to_contact_homeserver

    async def get_user_id(self, token: str) -> Optional[UserID]:
        token_hash = hashlib.sha512(token.encode("utf-8")).hexdigest()
        try:
            return self.user_ids[token_hash]
        except KeyError:
            url = self.hs_address / "client" / "v3" / "account" / "whoami"
            async with self._get(url, token, raise_error=False) as resp:
                # TODO handle other errors?
                if resp.status in (401, 403):
                    raise Error.invalid_auth_token
                user_id = self.user_ids[token_hash] = (await resp.json())["user_id"]
            return user_id

    def _remove_current_dms(
        self, dms: dict[UserID, list[RoomID]], username: str, bridge: str
    ) -> dict[UserID, list[RoomID]]:
        prefix = f"{self.mxid_prefix}{username}_{bridge}_"
        suffix = self.mxid_suffix
        bot = f"{prefix}bot{suffix}"

        return {
            user_id: rooms
            for user_id, rooms in dms.items()
            if not (user_id.startswith(prefix) and user_id.endswith(suffix) and user_id != bot)
        }

    def get_dms_lock(self, user_id):
        if user_id not in self.dm_locks:
            lock = Lock(
                self.redis,
                f"dms-lock-{user_id}",
                sleep=1.0,
                timeout=180,  # ensure the lock is released after 180s in case of a crash
            )
            self.dm_locks[user_id] = lock
        return self.dm_locks[user_id]

    async def update_dms(self, req: web.Request) -> web.Response:
        try:
            auth = req.headers["Authorization"]
            assert auth.startswith("Bearer ")
            auth = auth[len("Bearer ") :]
        except (KeyError, AssertionError):
            raise Error.invalid_auth_header
        try:
            data = await req.json()
        except json.JSONDecodeError:
            raise Error.request_not_json

        user_id = await self.get_user_id(auth)
        az = await self.find_appservice(req, header="X-Asmux-Auth", raise_errors=True)
        assert az is not None
        if user_id != f"@{az.owner}{self.mxid_suffix}":
            raise Error.mismatching_user

        async with self.get_dms_lock(user_id):
            url = (
                self.hs_address
                / "client/v3/user"
                / user_id
                / "account_data"
                / str(EventType.DIRECT)
            )
            try:
                async with self._get(url, auth) as resp:
                    dms = await resp.json()
            except HTTPCustomError as e:
                if e.status_code == 404:
                    dms = {}
                else:
                    raise
            if req.method == "PUT":
                dms = self._remove_current_dms(dms, az.owner, az.prefix)
            dms.update(data)
            async with self._put(url, auth, dms):
                return web.json_response({})

    async def proxy_login(self, req: web.Request) -> web.Response:
        body = await req.read()
        headers = copy_headers_no_host(req.headers)
        json_body = await req.json()
        if json_body.get("type") != "m.login.password":
            return await self.proxy(
                req, _spec_override="client", _path_override="v3/login", _body_override=body
            )
        if await self.convert_login_password(json_body):
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
            # TODO remove this everywhere (in _proxy)?
            del headers["Content-Length"]
        resp, _ = await self._proxy(
            req, self.hs_address / "client/v3/login", body=body, headers=headers
        )
        return resp

    async def proxy(
        self,
        req: web.Request,
        *,
        _spec_override: str = "",
        _path_override: str = "",
        _body_override: Any = None,
    ) -> web.Response:
        spec: str = _spec_override or req.match_info["spec"]
        path: str = _path_override or req.match_info["path"]
        url = self.hs_address.with_path(req.raw_path.split("?", 1)[0], encoded=True)

        az = await self.find_appservice(req)
        if not az:
            req["proxy_for"] = "<no auth>"
            no_host_headers = copy_headers_no_host(req.headers)
            resp, _ = await self._proxy(req, url, headers=no_host_headers, body=_body_override)
            return resp
        req["proxy_for"] = az.name

        headers, query = self._copy_data(req, az)

        body = _body_override or req.content
        unversioned_path = path.removeprefix("r0/").removeprefix("v3/")

        if spec == "client" and (
            (unversioned_path == "delete_devices" and req.method == "POST")
            or (unversioned_path.startswith("devices/") and req.method == "DELETE")
        ):
            body = await req.read()
            json_body = await req.json()
            if await self.convert_login_password(
                json_body.get("auth", {}), az=az, user_id=query["user_id"]
            ):
                body = json.dumps(json_body).encode("utf-8")

        resp, client_resp = await self._proxy(req, url, headers, query, body, az=az)

        if spec == "client" and unversioned_path == "createRoom":
            await self._register_room_from_create(az, resp, client_resp)

        return resp

    async def _register_room_from_create(
        self, az: AppService, resp: web.Response, client_resp: aiohttp.ClientResponse
    ) -> None:
        if not 200 <= client_resp.status < 300:
            return

        resp.body = await client_resp.read()
        resp.headers.pop("Transfer-Encoding", "")
        resp.headers.pop("Content-Length", "")
        resp_data = json.loads(resp.body)
        if "room_id" in resp_data:
            room = Room(id=resp_data["room_id"], owner=az.id, deleted=False)
            self.log.debug(
                f"Registering {az.name} ({az.id}) as the owner of {room.id}"
                " based on /createRoom response"
            )
            await room.insert()

    @staticmethod
    def _relevant_path_part(url: URL) -> Optional[str]:
        path = url.path.replace("/_matrix/media/r0", "/_matrix/media/v3").replace(
            "/_matrix/client/r0", "/_matrix/client/v3"
        )
        parts = path.split("/")
        if path.startswith("/_matrix/media/v3/"):
            return f"/media/{parts[4]}"
        elif path.startswith("/_matrix/client/v3/rooms/") and len(parts) > 6:
            if len(parts) > 7:
                return f"/rooms/.../{parts[6]}/..."
            else:
                return f"/rooms/.../{parts[6]}"
        elif path.startswith(
            "/_matrix/client/unstable/org.matrix.msc2716/rooms/"
        ) and path.endswith("/batch_send"):
            return f"/unstable/rooms/.../batch_send"
        elif path.startswith("/_matrix/client/v3/user/") and len(parts) > 6:
            if len(parts) > 8 and parts[6] == "rooms":
                end = "/..." if len(parts) > 9 else ""
                return f"/user/.../rooms/.../{parts[8]}{end}"
            else:
                end = "/..." if len(parts) > 7 else ""
                return f"/user/.../{parts[6]}{end}"
        elif path.startswith("/_matrix/client/v3/directory/room/"):
            return "/directory/room/..."
        elif path.startswith("/_matrix/client/v3/directory/list/room/"):
            return "/directory/list/room/..."
        elif path.startswith("/_matrix/client/v3/join/"):
            return "/join/..."
        elif path.startswith("/_matrix/client/v3/sendToDevice/"):
            return "/sendToDevice/..."
        elif path.startswith("/_matrix/client/v3/devices/") and len(parts) > 5 and parts[5]:
            return "/devices/..."
        elif path.startswith("/_matrix/client/v3/pushrules/") and len(parts) > 5 and parts[5]:
            return "/pushrules/..."
        elif path.startswith("/_matrix/client/v3/presence/") and path.endswith("/status"):
            return f"/presence/.../status"
        elif path.startswith("/_matrix/client/v3/profile/"):
            if len(parts) > 6:
                return f"/profile/.../{parts[6]}"
            return "/profile/..."
        elif path.startswith("/_matrix/client/v3/"):
            return path[len("/_matrix/client/v3") :]
        elif path.startswith("/_matrix/client/unstable/"):
            return path[len("/_matrix/client") :]
        else:
            return path

    async def _proxy(
        self,
        req: web.Request,
        url: URL,
        headers: Optional[CIMultiDict[str]] = None,
        query: Optional[MultiDict[str]] = None,
        body: Any = None,
        az: Optional[AppService] = None,
    ) -> tuple[web.Response, aiohttp.ClientResponse]:
        metric_labels = {
            "method": req.method,
            "endpoint": self._relevant_path_part(url),
            "owner": "",
            "bridge": "",
        }
        if az:
            metric_labels["owner"] = az.owner
            metric_labels["bridge"] = az.prefix
        aggregate_labels = {**metric_labels}
        aggregate_labels.pop("owner", None)
        REQUESTS_RECEIVED.labels(**metric_labels).inc()
        REQUESTS_RECEIVED_AGGREGATE.labels(**aggregate_labels).inc()
        try:
            req["proxy_url"] = url.with_query(query or req.query)
            resp = await self.http.request(
                req.method,
                url,
                headers=headers or req.headers.copy(),
                params=query or req.query.copy(),
                data=body or req.content,
            )
        except aiohttp.ClientError as e:
            self.log.warning(
                f"{type(e).__name__} proxying request {self.request_log_fmt(req)}: {e}"
            )
            raise Error.failed_to_contact_homeserver
        finally:
            REQUESTS_HANDLED.labels(**metric_labels).inc()
            REQUESTS_HANDLED_AGGREGATE.labels(**aggregate_labels).inc()
        if resp.status >= 400 and resp.status not in (401, 403):
            self.log.debug(f"Got HTTP {resp.status} proxying request {self.request_log_fmt(req)}")
        return web.Response(status=resp.status, headers=resp.headers, body=resp.content), resp

    async def _find_login_token(self, user_id: Optional[str]) -> Optional[str]:
        if not user_id:
            return None
        elif user_id.startswith(self.mxid_prefix) and user_id.endswith(self.mxid_suffix):
            localpart = user_id[len(self.mxid_prefix) : -len(self.mxid_suffix)]
            owner, prefix, _ = localpart.split("_", 2)
            az = await AppService.find(owner, prefix)
            if not az:
                return None
            return az.login_token
        else:
            localpart, _ = ClientAPI.parse_user_id(cast(UserID, user_id))
            user = await User.get(localpart)
            if not user:
                return None
            return user.login_token

    async def convert_login_password(
        self, data: Any, az: Optional[AppService] = None, user_id: Optional[str] = None
    ) -> bool:
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
            if not login_token or not user_id:
                return False
            expected_password = hmac.new(
                login_token.encode("utf-8"), user_id.encode("utf-8"), hashlib.sha512
            ).hexdigest()
            if data["password"] == expected_password:
                data["password"] = hmac.new(
                    self.login_shared_secret, user_id.encode("utf-8"), hashlib.sha512
                ).hexdigest()
                return True
        except (KeyError, ValueError):
            pass
        return False

    @staticmethod
    async def find_appservice(
        req: web.Request, header: str = "Authorization", raise_errors: bool = False
    ) -> Optional[AppService]:
        try:
            auth = req.headers[header]
            assert auth
            if not header.startswith("X-"):
                assert auth.startswith("Bearer ")
                auth = auth[len("Bearer ") :]
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

    def _copy_data(self, req: web.Request, az: AppService) -> tuple[CIMultiDict, MultiDict]:
        query = req.query.copy()
        try:
            del query["access_token"]
        except KeyError:
            pass
        try:
            del query["com.beeper.also_allow_user"]
        except KeyError:
            pass
        if req.path.endswith("/batch_send"):
            query["com.beeper.also_allow_user"] = f"@{az.owner}{self.mxid_suffix}"
        az_prefix = f"{self.mxid_prefix}{az.owner}_{az.prefix}_"
        if "user_id" not in query:
            query["user_id"] = f"{az_prefix}{az.bot}{self.mxid_suffix}"
        elif not query["user_id"].startswith(az_prefix):
            raise Error.invalid_user_id
        elif not query["user_id"].endswith(self.mxid_suffix):
            raise Error.external_user_id

        headers = copy_headers_no_host(req.headers)
        headers["Authorization"] = f"Bearer {self.as_token}"
        headers["X-Beeper-Bridge"] = az.name

        return headers, query
