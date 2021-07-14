# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Dict, Any, Union, Optional
from uuid import UUID
import logging
import asyncio

from aiohttp import web
from aiohttp.http import WSCloseCode

from mautrix.util.bridge_state import BridgeState

from ..database import AppService
from .cs_proxy import ClientProxy
from .errors import Error
from .as_proxy import Events
from .websocket_util import WebsocketHandler

WS_CLOSE_REPLACED = 4001


class AppServiceWebsocketHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_websocket")
    websockets: Dict[UUID, WebsocketHandler]
    status_endpoint: Optional[str]
    _stopping: bool

    def __init__(self, status_endpoint: Optional[str] = None) -> None:
        self.status_endpoint = status_endpoint
        self.websockets = {}
        self.requests = {}
        self._stopping = False

    async def stop(self) -> None:
        self._stopping = True
        self.log.debug("Disconnecting websockets")
        await asyncio.gather(*[ws.close(code=WSCloseCode.SERVICE_RESTART,
                                        status="server_shutting_down")
                               for ws in self.websockets.values()])

    async def send_bridge_status(self, az: AppService, state: Union[Dict[str, Any], BridgeState]
                                 ) -> None:
        if not self.status_endpoint:
            return
        if not isinstance(state, BridgeState):
            state = BridgeState.deserialize(state)
        self.log.debug(f"Sending bridge status for {az.name} to API server: {state}")
        await state.send(url=self.status_endpoint.format(owner=az.owner, prefix=az.prefix),
                         token=az.real_as_token, log=self.log, log_sent=False)

    async def handle_ws(self, req: web.Request) -> web.WebSocketResponse:
        if self._stopping:
            raise Error.server_shutting_down
        az = await ClientProxy.find_appservice(req, raise_errors=True)
        if az.push:
            raise Error.appservice_ws_not_enabled
        ws = WebsocketHandler(type_name="Websocket transaction connection",
                              proto="fi.mau.as_sync",
                              log=self.log.getChild(az.name))
        ws.set_handler("bridge_status", lambda handler, data: self.send_bridge_status(az, data))
        await ws.prepare(req)
        log = self.log.getChild(az.name)
        if az.id in self.websockets:
            log.debug(f"New websocket connection coming in, closing old one")
            await self.websockets[az.id].close(code=WS_CLOSE_REPLACED, status="conn_replaced")
        try:
            self.websockets[az.id] = ws
            await ws.send(command="connect", status="connected")
            await ws.handle()
        finally:
            if self.websockets.get(az.id) == ws:
                del self.websockets[az.id]
                if not self._stopping:
                    # TODO figure out remote IDs properly
                    await self.send_bridge_status(az, BridgeState(
                        ok=False, error="websocket-not-connected",  # remote_id="*"
                    ).fill())
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

    async def ping(self, appservice: AppService, remote_id: str) -> BridgeState:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            return BridgeState(ok=False, error="websocket-not-connected").fill()
        try:
            raw_pong = await asyncio.wait_for(ws.request("ping", remote_id=remote_id), timeout=45)
        except asyncio.TimeoutError:
            return BridgeState(ok=False, error="io-timeout").fill()
        except Exception as e:
            return BridgeState(ok=False, error="websocket-fatal-error", message=str(e)).fill()
        return BridgeState.deserialize(raw_pong)
