# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Dict, Optional, Any, Set, TYPE_CHECKING
from collections import defaultdict
from uuid import UUID
import functools
import asyncio
import logging
import time
import json

from aiohttp import web
from aiohttp.http import WSCloseCode

from ..database import AppService, User
from .as_proxy import Pong
from .cs_proxy import ClientProxy
from .errors import Error
from .websocket_util import WebsocketHandler

if TYPE_CHECKING:
    from ..server import MuxServer


class BridgeMonitor:
    log: logging.Logger = logging.getLogger("mau.api.bridge_monitor")
    server: 'MuxServer'
    _listeners: Dict[str, Dict[int, WebsocketHandler]]
    _status: Dict[UUID, Pong]
    _locks: Dict[UUID, asyncio.Lock]
    _prev_ws_id: int
    _stopping: bool

    def __init__(self, server: 'MuxServer') -> None:
        self.server = server
        self._status = {}
        self._locks = defaultdict(lambda: asyncio.Lock())
        self._listeners = defaultdict(lambda: {})
        self._prev_ws_id = 0
        self._stopping = False

    async def stop(self) -> None:
        self._stopping = True
        self.log.debug("Disconnecting websockets")
        await asyncio.gather(*[ws.close(code=WSCloseCode.SERVICE_RESTART,
                                        status="server_shutting_down")
                               for user_sockets in self._listeners.values()
                               for ws in user_sockets.values()])

    async def handle_ping(self, _: WebsocketHandler, data: Dict[str, Any], *, user: User
                          ) -> Dict[str, Any]:
        bridge_prefixes = set(data["bridges"])
        appservices = {bridge: await AppService.find(user.id, bridge)
                       for bridge in bridge_prefixes}
        not_found = []
        tasks = {}
        results = {}
        for bridge, az in appservices.items():
            if az is None:
                not_found.append(bridge)
                continue
            tasks[bridge] = asyncio.create_task(self.ping(az, always_notify=True))
        for bridge, task in tasks.items():
            results[bridge] = await task
        return {"not_found": not_found, "pongs": results}

    # Handles /_matrix/asmux/user/{id}/bridge_state
    # Used by clients to listen to real-time bridge connection state changes
    async def handle_ws(self, req: web.Request) -> web.WebSocketResponse:
        if self._stopping:
            raise Error.server_shutting_down
        user: User = req["user"]
        self._prev_ws_id += 1
        ws_id = self._prev_ws_id
        ws = WebsocketHandler(type_name="Bridge monitor websocket connection",
                              proto="com.beeper.asmux.bridge_state",
                              log=self.log.getChild(f"{ws_id}-{user.id}"))
        ws.set_handler("ping_bridges", functools.partial(self.handle_ping, user=user))
        await ws.prepare(req)
        try:
            self._listeners[user.id][ws_id] = ws
            await ws.handle()
        finally:
            del self._listeners[user.id][ws_id]
        return ws.response

    # Handles /_matrix/client/unstable/com.beeper.asmux/pong
    # Called by non-websocket bridges to update the bridge connection state
    async def update_pong(self, req: web.Request) -> web.Response:
        az = await ClientProxy.find_appservice(req, raise_errors=True)

        try:
            data = await req.json()
        except json.JSONDecodeError:
            raise Error.request_not_json

        await self.set_pong(az, data)
        return web.json_response({})

    async def _actually_ping(self, appservice: AppService) -> Pong:
        try:
            if not appservice.push:
                return await self.server.as_websocket.ping(appservice)
            elif appservice.address:
                return await self.server.as_http.ping(appservice)
            else:
                self.log.warning(f"Not pinging {appservice.name}: no address configured")
                return {"ok": False, "error_source": "asmux", "error": "ping-no-remote",
                        "message": f"Couldn't make ping: no address configured"}
        except Exception as e:
            self.log.exception(f"Fatal error pinging {appservice.name}")
            return {"ok": False, "error_source": "asmux", "error": "ping-fatal-error",
                    "message": f"Fatal error while pinging: {e}"}

    @staticmethod
    def _ensure_pong_makes_sense(pong: Pong) -> None:
        if "ok" not in pong:
            pong["ok"] = False
        if "timestamp" not in pong:
            pong["timestamp"] = int(time.time())
        if "ttl" not in pong:
            pong["ttl"] = 180
        if not pong["ok"]:
            if "error_source" not in pong:
                pong["error_source"] = "unknown"
            if "error" not in pong:
                pong["error"] = "unknown-error"
            if "message" not in pong:
                pong["message"] = "Ping returned unknown error"

    async def _notify_listeners(self, appservice: AppService, pong: Pong) -> None:
        data = {"appservice": appservice.prefix, **pong}
        tasks = [ws.send(command="bridge_status", data=data)
                 for ws in self._listeners[appservice.owner].values()]
        self.log.debug(f"Broadcasting {appservice.name} pong to websockets: {pong}")
        await asyncio.gather(*tasks)

    async def set_pong(self, appservice: AppService, pong: Optional[Pong]) -> None:
        async with self._locks[appservice.id]:
            if pong is None:
                # Just delete the cache, no notification
                try:
                    del self._status[appservice.id]
                except KeyError:
                    pass
                return
            self._ensure_pong_makes_sense(pong)
            self._status[appservice.id] = pong
        await self._notify_listeners(appservice, pong)

    async def ping(self, appservice: AppService, always_notify: bool = False) -> Pong:
        async with self._locks[appservice.id]:
            try:
                pong = self._status[appservice.id]
                if pong["timestamp"] + pong["ttl"] < int(time.time()):
                    if always_notify:
                        asyncio.create_task(self._notify_listeners(appservice, pong))
                    return pong
            except KeyError:
                pass
            pong = await self._actually_ping(appservice)
            self._ensure_pong_makes_sense(pong)
            self._status[appservice.id] = pong
        asyncio.create_task(self._notify_listeners(appservice, pong))
        return pong
