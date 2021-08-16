# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Callable, Awaitable, Optional, TYPE_CHECKING
from uuid import UUID
import logging
import base64
import json
import copy
import io
import re

from ruamel.yaml.comments import CommentedMap
from aiohttp import web, ClientSession
from yarl import URL

from mautrix.types import JSON, RoomID
from mautrix.client import ClientAPI
from mautrix.util.bridge_state import BridgeState, BridgeStateEvent
from mautrix.util.config import yaml, RecursiveDict

from ..database import AppService, User, Room
from ..config import Config
from .errors import Error

if TYPE_CHECKING:
    from ..server import MuxServer

part_regex = re.compile("^[a-z0-9=.-]{1,32}$")

Handler = Callable[[web.Request], Awaitable[web.Response]]
AuthCallback = Callable[[web.Request, str], Awaitable[None]]

DEFAULT_CFG_LIFETIME = 5 * 60
MIN_CFG_LIFETIME = 30
MAX_CFG_LIFETIME = 60 * 60

BridgeState.default_source = "asmux"
BridgeState.human_readable_errors.update({
    "ping-no-remote": "Couldn't make ping: no address configured",
    "websocket-not-connected": "The bridge is not connected to the server",
    "io-timeout": "Timeout while waiting for ping response",
    "http-connection-error": "HTTP client error while pinging: {message}",
    "ping-fatal-error": "Fatal error while pinging: {message}",
    "websocket-fatal-error": "Fatal error while pinging through websocket: {message}",
    "http-fatal-error": "Fatal error while pinging through HTTP: {message}",
    "http-not-json": "Non-JSON ping response",
})


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
    server: 'MuxServer'

    global_prefix: str
    exclusive: bool
    server_name: str
    shared_secret: str
    hs_address: URL
    public_address: URL
    namespace_prefix: str
    config_templates: dict[str, RecursiveDict]
    as_token: str

    app: web.Application
    mxauth_app: web.Application
    public_app: web.Application
    websocket_app: web.Application

    def __init__(self, config: Config, http: ClientSession, server: 'MuxServer') -> None:
        self.global_prefix = config["appservice.namespace.prefix"]
        self.exclusive = config["appservice.namespace.exclusive"]
        self.server_name = config["homeserver.domain"]
        self.shared_secret = config["mux.shared_secret"]
        self.hs_address = URL(config["homeserver.address"])
        self.public_address = URL(config["mux.public_address"])
        self.as_token = config["appservice.as_token"]
        self.namespace_prefix = config["appservice.namespace.prefix"]
        self.config_templates = {}

        for bridge, file in config["mux.bridge_config_template_files"].items():
            with open(file) as stream:
                self.config_templates[bridge] = RecursiveDict(yaml.load(stream), CommentedMap)

        self.http = http
        self.server = server

        self.app = web.Application(middlewares=[self.check_auth])
        self.app.router.add_get("/user/{id}", self.get_user)
        self.app.router.add_put("/user/{id}", self.put_user)
        self.app.router.add_get("/user/{id}/proxy", self.get_user_proxy)
        self.app.router.add_put("/user/{id}/proxy", self.put_user_proxy)
        self.app.router.add_post("/appservice/{id}/ping", self.ping_appservice)
        self.app.router.add_post("/appservice/{owner}/{prefix}/ping", self.ping_appservice)
        self.app.router.add_put("/appservice/{id}", self.provision_appservice)
        self.app.router.add_put("/appservice/{owner}/{prefix}", self.provision_appservice)
        self.app.router.add_get("/appservice/{id}", self.get_appservice)
        self.app.router.add_get("/appservice/{owner}/{prefix}", self.get_appservice)
        self.app.router.add_delete("/appservice/{id}", self.delete_appservice)
        self.app.router.add_delete("/appservice/{owner}/{prefix}", self.delete_appservice)
        self.app.router.add_delete("/room/{room_id}", self.delete_room)

        self.mxauth_app = web.Application(middlewares=[self.check_mx_auth])
        self.mxauth_app.router.add_get("/user/{id}/proxy", self.get_user_proxy)

        self.public_app = web.Application()
        self.public_app.router.add_get("/config/{prefix}/register", self.register_config)
        self.public_app.router.add_get("/config/{prefix}/download", self.download_config)

        self.websocket_app = web.Application(middlewares=[self.check_ws_auth])

        self._cors = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
            "Access-Control-Allow-Methods": "GET, PUT, POST, OPTIONS",
        }

    async def _check_auth_generic(self, req: web.Request, handler: Handler, callback: AuthCallback
                                  ) -> web.Response:
        if req.method == "OPTIONS":
            return web.Response(headers=self._cors)
        try:
            auth = req.headers["Authorization"]
            if not auth.startswith("Bearer "):
                raise Error.invalid_auth_header
            auth = auth[len("Bearer "):]
        except KeyError:
            raise Error.missing_auth_header
        if auth == self.shared_secret:
            req["is_root"] = True
        else:
            req["is_root"] = False
            await callback(req, auth)
        resp = await handler(req)
        resp.headers.update(self._cors)
        return resp

    async def _check_auth_websocket(self, req: web.Request, handler: Handler,
                                    callback: AuthCallback, prefix: str = "com.beeper.asmux.auth-"
                                    ) -> web.Response:
        if req.method == "OPTIONS":
            return web.Response(headers=self._cors)
        try:
            for proto in req.headers["Sec-WebSocket-Protocol"].split(","):
                proto = proto.strip()
                if proto.startswith(prefix):
                    auth = proto[len(prefix):]
                    break
            else:
                raise Error.invalid_auth_header
        except KeyError:
            raise Error.missing_auth_header
        await callback(req, auth)
        resp = await handler(req)
        resp.headers.update(self._cors)
        return resp

    @staticmethod
    async def _normal_auth_callback(req: web.Request, auth: str) -> None:
        user = await User.find_by_api_token(auth)
        if not user:
            raise Error.invalid_auth_token
        req["user"] = user
        req["user_direct_auth"] = False

    async def _mx_auth_callback(self, req: web.Request, auth: str) -> None:
        user_id = await self.server.cs_proxy.get_user_id(auth)
        # We can ignore the server name since get_user_id will only use the local server
        localpart, _ = ClientAPI.parse_user_id(user_id)
        user = await User.get(localpart)
        if not user:
            raise Error.user_not_found
        req["user"] = user
        req["user_direct_auth"] = True

    @web.middleware
    async def check_auth(self, req: web.Request, handler: Handler) -> web.Response:
        return await self._check_auth_generic(req, handler, self._normal_auth_callback)

    @web.middleware
    async def check_mx_auth(self, req: web.Request, handler: Handler) -> web.Response:
        return await self._check_auth_generic(req, handler, self._mx_auth_callback)

    @web.middleware
    async def check_ws_auth(self, req: web.Request, handler: Handler) -> web.Response:
        return await self._check_auth_websocket(req, handler, self._normal_auth_callback)

    def _make_registration(self, az: AppService, config_password: Optional[str] = None,
                           config_password_lifetime: Optional[int] = None) -> JSON:
        prefix = f"{re.escape(self.global_prefix)}{re.escape(az.owner)}_{re.escape(az.prefix)}"
        server_name = re.escape(self.server_name)
        registration = {
            "id": str(az.id),
            "as_token": az.real_as_token,
            "hs_token": az.hs_token,
            # TODO deprecate this in favor of the one below
            "login_shared_secret": az.login_token,
            "com.beeper.asmux": {
                "login_shared_secret": az.login_token,
            },
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
        if config_password:
            registration["com.beeper.asmux"]["config_password"] = config_password
            registration["com.beeper.asmux"]["config_password_lifetime"] = config_password_lifetime
        return registration

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

    @staticmethod
    async def _get_user(req: web.Request, allow_create: bool) -> User:
        find_user_id = req.match_info["id"]
        if "user" in req:
            user = req["user"]
            if user.id != find_user_id:
                raise Error.user_access_denied
        else:
            if allow_create:
                if not part_regex.fullmatch(find_user_id):
                    raise Error.invalid_owner
                user = await User.get_or_create(find_user_id)
            else:
                user = await User.get(find_user_id)
                if not user:
                    raise Error.user_not_found
        return user

    async def get_user(self, req: web.Request) -> web.Response:
        allow_create = req.query.get("create", "0") in ("1", "true", "t", "y", "yes")
        user = await self._get_user(req, allow_create=allow_create)
        return web.json_response(user.to_dict())

    async def put_user(self, req: web.Request) -> web.Response:
        # try:
        #     data = await req.json()
        # except json.JSONDecodeError:
        #     raise Error.request_not_json
        user = await self._get_user(req, allow_create=True)
        # await user.edit()
        return web.json_response(user.to_dict())

    async def get_user_proxy(self, req: web.Request) -> web.Response:
        user = await self._get_user(req, allow_create=False)
        if not user.proxy_config:
            raise Error.proxy_not_setup
        proxy_cfg_resp = user.proxy_config_response(include_private_key=req["user_direct_auth"])
        return web.json_response(proxy_cfg_resp)

    async def put_user_proxy(self, req: web.Request) -> web.Response:
        try:
            data = await req.json()
        except json.JSONDecodeError:
            raise Error.request_not_json
        user = await self._get_user(req, allow_create=False)
        proxy_config = user.proxy_config
        if not proxy_config or req.query.get("regenerate", "false").lower() in ("1", "t", "true"):
            proxy_config = {
                "socks": user.generate_socks_config(),
                "ssh": user.generate_ssh_key(),
            }
        proxy_config["ssh"] = {
            **data,
            "publicKey": proxy_config["ssh"]["publicKey"],
            "privateKey": proxy_config["ssh"]["privateKey"],
            "passphrase": proxy_config["ssh"]["passphrase"],
        }
        await user.edit(proxy_config=proxy_config)
        proxy_cfg_resp = user.proxy_config_response(include_private_key=False)
        return web.json_response(proxy_cfg_resp)

    async def get_appservice(self, req: web.Request) -> web.Response:
        az = await self._get_appservice(req)
        return web.json_response(self._make_registration(az))

    async def ping_appservice(self, req: web.Request) -> web.Response:
        az = await self._get_appservice(req)
        remote_id = req.query.get("remote_id", "unknown")

        try:
            if not az.push:
                pong = await self.server.as_websocket.ping(az, remote_id)
            elif az.address:
                pong = await self.server.as_http.ping(az, remote_id)
            else:
                self.log.warning(f"Not pinging {az.name}: no address configured")
                pong = BridgeState(state_event=BridgeStateEvent.UNKNOWN_ERROR,
                                   error="ping-no-remote").fill()
        except Exception as e:
            self.log.exception(f"Fatal error pinging {az.name}")
            pong = BridgeState(state_event=BridgeStateEvent.UNKNOWN_ERROR,
                               error="ping-fatal-error", message=str(e)).fill()
        return web.json_response(pong.serialize())

    async def delete_appservice(self, req: web.Request) -> web.Response:
        az = await self._get_appservice(req)
        await az.delete()
        return web.Response(status=204)

    @staticmethod
    async def delete_room(req: web.Request) -> web.Response:
        if not req["is_root"]:
            raise Error.room_delete_access_denied
        room_id = RoomID(req.match_info["room_id"])
        room = await Room.get(room_id)
        if not room:
            raise Error.room_not_found
        await room.delete()
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
                                                 address=data.get("address", ""),
                                                 push=data.get("push", True))
            az.login_token = user.login_token
            if az.created_:
                try:
                    await self._register_as_bot(az)
                except Exception:
                    self.log.warning(f"Failed to register bridge bot {owner}_{prefix}_{az.bot}",
                                     exc_info=True)
                self.log.info(f"Created appservice {az.name} ({az.id})")
        else:
            az = self._error_wrap(req, await AppService.get(uuid))
        if not az.created_:
            await az.set_address(data.get("address"))
            await az.set_push(data.get("push"))
        config_password = None
        lifetime = None
        if data.get("generate_config"):
            if az.prefix not in self.config_templates:
                raise Error.config_download_unsupported
            lifetime = data.get("config_password_lifetime", DEFAULT_CFG_LIFETIME)
            lifetime = max(min(lifetime, MAX_CFG_LIFETIME), MIN_CFG_LIFETIME)
            config_password = await az.generate_password(lifetime)
        status = 201 if az.created_ else 200
        az.created_ = False
        return web.json_response(self._make_registration(az, config_password, lifetime),
                                 status=status)

    @staticmethod
    async def _authorize_config_download(req: web.Request) -> AppService:
        try:
            prefix = req.match_info["prefix"]
        except KeyError:
            raise Error.missing_fields
        try:
            header = req.headers["Authorization"].removeprefix("Basic ")
            username, password = (base64.b64decode(header)
                                  .decode("utf-8")
                                  .split(":", 1))
        except (KeyError, ValueError, TypeError):
            raise Error.invalid_auth_header
        az = await AppService.find(username, prefix)
        if az is None:
            raise Error.appservice_not_found
        if not az.check_password(password):
            raise Error.invalid_config_password
        return az

    async def register_config(self, req: web.Request) -> web.Response:
        az = await self._authorize_config_download(req)
        new_password = await az.generate_password(lifetime=None)
        self.log.debug(f"Generated new config download password for {az.name}")
        config_url = (self.public_address
                      .with_path(f"/_matrix/asmux/public/config/{az.prefix}/download")
                      .with_user(az.owner)
                      .with_password(new_password))
        return web.Response(status=303, headers={"Location": str(config_url)})

    def _configure_imessage(self, az: AppService) -> str:
        config = copy.deepcopy(self.config_templates["imessage"])
        config["homeserver.address"] = str(self.public_address)
        config["homeserver.websocket_proxy"] = str(self.public_address.with_scheme("wss"))
        config["homeserver.domain"] = self.server_name
        config["homeserver.asmux"] = True

        config["appservice.id"] = str(az.id)
        config["appservice.bot.username"] = f"_{az.owner}_{az.prefix}_{az.bot}"
        config["appservice.as_token"] = az.real_as_token
        config["appservice.hs_token"] = az.hs_token
        config["bridge.user"] = f"@{az.owner}:{self.server_name}"
        config["bridge.username_template"] = (f"{self.namespace_prefix}{az.owner}_{az.prefix}_"
                                              "{{.}}")
        config["bridge.login_shared_secret"] = az.login_token
        with io.StringIO() as output:
            yaml.dump(config._data, output)
            return output.getvalue()

    async def download_config(self, req: web.Request) -> web.Response:
        az = await self._authorize_config_download(req)
        return web.Response(status=200, content_type="text/yaml",
                            body=self._configure_imessage(az))
