# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Dict, Optional, Any
from uuid import UUID, uuid4
import logging
import asyncio
import json

from aiohttp import web
from aiohttp.http import WSMessage, WSMsgType, WSCloseCode

from ..database import AppService
from .cs_proxy import ClientProxy
from .errors import Error
from .as_proxy import Events, Pong


class AppServiceWebsocketHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_websocket")
    websockets: Dict[UUID, web.WebSocketResponse]
    requests: Dict[UUID, asyncio.Future]

    def __init__(self) -> None:
        self.websockets = {}
        self.requests = {}

    async def stop(self) -> None:
        self.log.debug("Disconnecting websockets")
        await asyncio.gather(*[ws.close(code=WSCloseCode.SERVICE_RESTART,
                                        message=b'{"status": "server_shutting_down"}')
                               for ws in self.websockets.values()])

    async def _handle_websocket_text(self, log: logging.Logger, msg: WSMessage) -> None:
        try:
            data = msg.json()
        except json.JSONDecodeError:
            log.debug(f"Non-JSON data received: {msg.data}")
            data = None
        try:
            req_id = UUID(data.pop("id"))
            waiter = self.requests[req_id]
        except (KeyError, ValueError, TypeError):
            log.debug(f"Unhandled data received: {data}")
        else:
            waiter.set_result(data)
            log.debug(f"Received response to {req_id}: {data}")

    async def handle_ws(self, req: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        az = await ClientProxy.find_appservice(req, raise_errors=True)
        if az.push:
            raise Error.appservice_ws_not_enabled
        await ws.prepare(req)
        log = self.log.getChild(az.name)
        if az.id in self.websockets:
            log.debug(f"New websocket connection coming in, closing old one")
            await self.websockets[az.id].close(code=WSCloseCode.OK,
                                               message=b'{"status": "conn_replaced"}')
            pass
        self.websockets[az.id] = ws
        log.debug(f"Websocket transaction connection opened")
        try:
            await ws.send_json({"status": "connected"})
            msg: WSMessage
            async for msg in ws:
                if msg.type == WSMsgType.ERROR:
                    log.error(f"Error in websocket connection", exc_info=ws.exception())
                    break
                elif msg.type == WSMsgType.CLOSE:
                    log.debug("Websocket close message received")
                    break
                elif msg.type == WSMsgType.TEXT:
                    try:
                        await self._handle_websocket_text(log, msg)
                    except Exception:
                        log.exception("Error handling websocket text message")
                else:
                    log.debug("Unhandled websocket message of type %s: %s", msg.type, msg.data)
        except Exception:
            log.exception("Fatal error in websocket handler")
        finally:
            if self.websockets.get(az.id) == ws:
                del self.websockets[az.id]
        self.log.debug(f"Websocket transaction connection closed to {az.name}")
        return ws

    async def post_events(self, appservice: AppService, events: Events) -> bool:
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

    async def request(self, appservice: AppService, command: str,
                      data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            return None
        req_id = uuid4()
        self.requests[req_id] = fut = asyncio.get_running_loop().create_future()
        await ws.send_json({
            "command": command,
            "id": req_id,
            "data": data,
        })
        return await fut

    async def ping(self, appservice: AppService) -> Pong:
        try:
            pong = await asyncio.wait_for(self.request(appservice, "ping"), timeout=30)
        except asyncio.TimeoutError:
            return {"ok": False, "error_source": "asmux", "error": "io-timeout",
                    "message": "Timeout while waiting for ping response"}
        except Exception as e:
            return {"ok": False, "error_source": "asmux", "error": "websocket-fatal-error",
                    "message": f"Fatal error while pinging: {e}"}
        if pong is None:
            return {"ok": False, "error_source": "asmux", "error": "websocket-not-connected",
                    "message": "The bridge does not have an active websocket connection to asmux"}
        return pong
