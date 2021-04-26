# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Dict, Any, TYPE_CHECKING
from uuid import UUID
import functools
import logging
import asyncio

from aiohttp import web
from aiohttp.http import WSCloseCode

from ..database import AppService
from .cs_proxy import ClientProxy
from .errors import Error
from .as_proxy import Events, Pong
from .websocket_util import WebsocketHandler

if TYPE_CHECKING:
    from ..server import MuxServer


WS_CLOSE_REPLACED = 4001


class AppServiceWebsocketHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_websocket")
    websockets: Dict[UUID, WebsocketHandler]
    server: 'MuxServer'
    _stopping: bool

    def __init__(self, server: 'MuxServer') -> None:
        self.server = server
        self.websockets = {}
        self.requests = {}
        self._stopping = False

    async def stop(self) -> None:
        self._stopping = True
        self.log.debug("Disconnecting websockets")
        await asyncio.gather(*[ws.close(code=WSCloseCode.SERVICE_RESTART,
                                        status="server_shutting_down")
                               for ws in self.websockets.values()])

    async def handle_status(self, _: WebsocketHandler, data: Dict[str, Any], *, az: AppService
                            ) -> None:
        await self.server.bridge_monitor.set_pong(az, data)

    async def handle_ws(self, req: web.Request) -> web.WebSocketResponse:
        if self._stopping:
            raise Error.server_shutting_down
        az = await ClientProxy.find_appservice(req, raise_errors=True)
        if az.push:
            raise Error.appservice_ws_not_enabled
        ws = WebsocketHandler(type_name="Websocket transaction connection",
                              proto="fi.mau.as_sync",
                              log=self.log.getChild(az.name))
        ws.set_handler("bridge_status", functools.partial(self.handle_status, az=az))
        await ws.prepare(req)
        log = self.log.getChild(az.name)
        if az.id in self.websockets:
            log.debug(f"New websocket connection coming in, closing old one")
            await self.websockets[az.id].close(code=WS_CLOSE_REPLACED, status="conn_replaced")
        try:
            self.websockets[az.id] = ws
            await self.server.bridge_monitor.set_pong(az, None)
            await ws.send(command="connect", status="connected")
            await ws.handle()
        finally:
            if self.websockets.get(az.id) == ws:
                del self.websockets[az.id]
                if not self._stopping:
                    asyncio.create_task(self.server.bridge_monitor.set_pong(az, {
                        "ok": False, "error_source": "asmux", "error": "websocket-not-connected",
                        "message": "The bridge is not connected to the server"
                    }))
        return ws.response

    async def post_events(self, appservice: AppService, events: Events) -> bool:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            # TODO buffer transactions
            self.log.warning(f"Not sending transaction {events.txn_id} to {appservice.name}: "
                             f"websocket not connected")
            return False
        self.log.debug(f"Sending transaction {events.txn_id} to {appservice.name} via websocket")
        await ws.send(command="transaction", status="ok",
                      txn_id=events.txn_id, events=events.pdu, ephemeral=events.edu)
        return True

    async def ping(self, appservice: AppService) -> Pong:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            return {"ok": False, "error_source": "asmux", "error": "websocket-not-connected",
                    "message": "The bridge does not have an active websocket connection to asmux"}
        try:
            pong = await asyncio.wait_for(ws.request("ping"), timeout=45)
        except asyncio.TimeoutError:
            return {"ok": False, "error_source": "asmux", "error": "io-timeout",
                    "message": "Timeout while waiting for ping response"}
        except Exception as e:
            return {"ok": False, "error_source": "asmux", "error": "websocket-fatal-error",
                    "message": f"Fatal error while pinging: {e}"}
        return pong
