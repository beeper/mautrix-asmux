# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Dict, TYPE_CHECKING
from uuid import UUID
import logging
import asyncio

from aiohttp import web
from aiohttp.http import WSMessage, WSMsgType, WSCloseCode

from ..database import AppService
from .cs_proxy import ClientProxy
from .errors import Error

if TYPE_CHECKING:
    from ..server import MuxServer
    from .as_proxy import Events


class AppServiceWebsocketHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_websocket")
    server: 'MuxServer'
    websockets: Dict[UUID, web.WebSocketResponse]

    def __init__(self, server: 'MuxServer') -> None:
        self.server = server
        self.websockets = {}

    async def stop(self) -> None:
        self.log.debug("Disconnecting websockets")
        await asyncio.gather(*[ws.close(code=WSCloseCode.SERVICE_RESTART,
                                        message=b'{"status": "server_shutting_down"}')
                               for ws in self.websockets.values()])

    async def sync(self, req: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        az = await ClientProxy.find_appservice(req, raise_errors=True)
        if az.push:
            raise Error.appservice_ws_not_enabled
        await ws.prepare(req)
        if az.id in self.websockets:
            self.log.debug(f"New websocket connection coming in for {az.name}, closing old one")
            await self.websockets[az.id].close(code=WSCloseCode.OK,
                                               message=b'{"status": "conn_replaced"}')
            pass
        self.websockets[az.id] = ws
        self.log.debug(f"Websocket transaction connection opened to {az.name}")
        try:
            await ws.send_json({"status": "connected"})
            msg: WSMessage
            async for msg in ws:
                if msg.type == WSMsgType.ERROR:
                    self.log.error(f"Error in websocket connection to {az.name}",
                                   exc_info=ws.exception())
                    break
                elif msg.type == WSMsgType.CLOSE:
                    break
                elif msg.type == WSMsgType.TEXT:
                    data = msg.json()
        finally:
            if self.websockets.get(az.id) == ws:
                del self.websockets[az.id]
        self.log.debug(f"Websocket transaction connection closed to {az.name}")
        return ws

    async def post_events(self, appservice: AppService, events: 'Events') -> bool:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            # TODO buffer transactions
            self.log.warning(f"Not sending transaction {events.txn_id} to {appservice.name}: "
                             f"websocket not connected")
            return False
        self.log.debug(f"Sending transaction {events.txn_id} to {appservice.name} via websocket")
        await ws.send_json({
            "status": "ok",
            "txn_id": events.txn_id,
            "events": events.pdu,
            "ephemeral": events.edu,
        })
        return True
