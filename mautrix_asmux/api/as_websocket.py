# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Any, Dict, Union, Optional
from uuid import UUID
import logging
import asyncio
import json
import time

import aiohttp
from yarl import URL
from aiohttp import web
from aiohttp.http import WSCloseCode

from mautrix.util.bridge_state import BridgeState, BridgeStateEvent, GlobalBridgeState
from mautrix.util.logging import TraceLogger
from mautrix.util.opt_prometheus import Gauge
from mautrix.errors import make_request_error, standard_error, MatrixStandardRequestError

from ..database import AppService
from ..config import Config
from .cs_proxy import ClientProxy
from .errors import Error
from .as_proxy import Events, make_ping_error, migrate_state_data
from .websocket_util import WebsocketHandler

WS_CLOSE_REPLACED = 4001
CONNECTED_WEBSOCKETS = Gauge("asmux_connected_websockets",
                             "Bridges connected to the appservice transaction websocket",
                             labelnames=["owner", "bridge"])


@standard_error("FI.MAU.SYNCPROXY.NOT_ACTIVE")
class SyncProxyNotActive(MatrixStandardRequestError):
    pass


class AppServiceWebsocketHandler:
    log: TraceLogger = logging.getLogger("mau.api.as_websocket")
    websockets: dict[UUID, WebsocketHandler]
    remote_status_endpoint: Optional[str]
    bridge_status_endpoint: Optional[str]
    sync_proxy: Optional[URL]
    sync_proxy_token: Optional[str]
    sync_proxy_own_address: Optional[str]
    hs_token: str
    hs_domain: str
    mxid_prefix: str
    mxid_suffix: str
    _stopping: bool

    def __init__(self, config: Config, mxid_prefix: str, mxid_suffix: str) -> None:
        self.remote_status_endpoint = config["mux.remote_status_endpoint"]
        self.bridge_status_endpoint = config["mux.bridge_status_endpoint"]
        self.sync_proxy = (URL(config["mux.sync_proxy.url"]) if config["mux.sync_proxy.url"]
                           else None)
        self.sync_proxy_token = config["mux.sync_proxy.token"]
        self.sync_proxy_own_address = config["mux.sync_proxy.asmux_address"]
        self.hs_token = config["appservice.hs_token"]
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.websockets = {}
        self.requests = {}
        self._stopping = False

    async def stop(self) -> None:
        self._stopping = True
        self.log.debug("Disconnecting websockets")
        await asyncio.gather(*(ws.close(code=WSCloseCode.SERVICE_RESTART,
                                        status="server_shutting_down")
                               for ws in self.websockets.values()))

    async def send_remote_status(self, az: AppService, state: Union[dict[str, Any], BridgeState]
                                 ) -> None:
        if not self.remote_status_endpoint:
            return
        if not isinstance(state, BridgeState):
            state = BridgeState.deserialize(migrate_state_data(state, is_global=False))
        self.log.debug(f"Sending remote status for {az.name} to API server: {state}")
        await state.send(url=self.remote_status_endpoint.format(owner=az.owner, prefix=az.prefix),
                         token=az.real_as_token, log=self.log, log_sent=False)

    async def send_bridge_status(self, az: AppService, state_event: BridgeStateEvent) -> None:
        if not self.bridge_status_endpoint:
            return
        self.log.debug(f"Sending bridge status for {az.name} to API server: {state_event}")
        headers = {"Authorization": f"Bearer {az.real_as_token}"}
        body = {"stateEvent": state_event.serialize()}
        url = self.bridge_status_endpoint.format(owner=az.owner, prefix=az.prefix)
        try:
            async with aiohttp.ClientSession() as sess, sess.post(url, json=body,
                                                                  headers=headers) as resp:
                if not 200 <= resp.status < 300:
                    text = await resp.text()
                    text = text.replace("\n", "\\n")
                    self.log.warning(f"Unexpected status code {resp.status} "
                                     f"sending bridge state update: {text}")
        except Exception as e:
            self.log.warning(f"Failed to send updated bridge state: {e}")

    @staticmethod
    async def _get_response(resp: aiohttp.ClientResponse) -> Dict[str, Any]:
        text = await resp.text()
        errcode = error = resp_data = None
        try:
            resp_data = await resp.json()
            errcode = resp_data["errcode"]
            error = resp_data["error"]
        except (json.JSONDecodeError, aiohttp.ContentTypeError, KeyError, TypeError):
            pass
        if resp.status >= 400:
            raise make_request_error(resp.status, text, errcode, error)
        return resp_data

    async def start_sync_proxy(self, az: AppService, data: Dict[str, Any]) -> Dict[str, Any]:
        url = self.sync_proxy.with_path("/_matrix/client/unstable/fi.mau.syncproxy") / str(az.id)
        headers = {"Authorization": f"Bearer {self.sync_proxy_token}"}
        req = {
            "appservice_id": str(az.id),
            "user_id": f"{self.mxid_prefix}{az.owner}_{az.prefix}_{az.bot}{self.mxid_suffix}",
            "bot_access_token": data["access_token"],
            "device_id": data["device_id"],
            "hs_token": self.hs_token,
            "address": self.sync_proxy_own_address,
            "is_proxy": True,
        }
        self.log.debug(f"Requesting sync proxy start for {az.id}")
        self.log.trace("Sync proxy data: %s", req)
        async with aiohttp.ClientSession() as sess, sess.put(url, json=req, headers=headers
                                                             ) as resp:
            return await self._get_response(resp)

    @staticmethod
    async def ping_server(_1: WebsocketHandler, _2: Dict[str, Any]) -> Dict[str, Any]:
        return {"timestamp": int(time.time() * 1000)}

    async def stop_sync_proxy(self, az: AppService) -> None:
        url = self.sync_proxy.with_path("/_matrix/client/unstable/fi.mau.syncproxy") / str(az.id)
        headers = {"Authorization": f"Bearer {self.sync_proxy_token}"}
        self.log.debug(f"Requesting sync proxy stop for {az.id}")
        try:
            async with aiohttp.ClientSession() as sess, sess.delete(url, headers=headers) as resp:
                await self._get_response(resp)
            self.log.debug(f"Stopped sync proxy for {az.id}")
        except SyncProxyNotActive as e:
            self.log.debug(f"Failed to request sync proxy stop for {az.id}: {e}")
        except Exception as e:
            self.log.warning(f"Failed to request sync proxy stop for {az.id}: "
                             f"{type(e).__name__}: {e}")
            self.log.trace("Sync proxy stop error", exc_info=True)

    async def handle_ws(self, req: web.Request) -> web.WebSocketResponse:
        if self._stopping:
            raise Error.server_shutting_down
        az = await ClientProxy.find_appservice(req, raise_errors=True)
        if az.push:
            raise Error.appservice_ws_not_enabled
        identifier = req.headers.get("X-Mautrix-Process-ID", "unidentified")
        ws = WebsocketHandler(type_name="Websocket transaction connection",
                              proto="fi.mau.as_sync",
                              log=self.log.getChild(az.name).getChild(identifier))
        ws.set_handler("bridge_status", lambda handler, data: self.send_remote_status(az, data))
        ws.set_handler("start_sync", lambda handler, data: self.start_sync_proxy(az, data))
        ws.set_handler("ping", self.ping_server)
        await ws.prepare(req)
        try:
            old_websocket = self.websockets.pop(az.id)
        except KeyError:
            pass
        else:
            ws.log.debug(f"New websocket connection coming in, closing old one")
            await old_websocket.close(code=WS_CLOSE_REPLACED, status="conn_replaced")
        try:
            self.websockets[az.id] = ws
            CONNECTED_WEBSOCKETS.labels(owner=az.owner, bridge=az.prefix).inc()
            await ws.send(command="connect", status="connected")
            await ws.handle()
        finally:
            CONNECTED_WEBSOCKETS.labels(owner=az.owner, bridge=az.prefix).dec()
            if self.websockets.get(az.id) == ws:
                del self.websockets[az.id]
                asyncio.create_task(self.stop_sync_proxy(az))
                if not self._stopping:
                    await self.send_bridge_status(az, BridgeState(
                        state_event=BridgeStateEvent.BRIDGE_UNREACHABLE,
                        error="websocket-not-connected",
                    ).fill())
        return ws.response

    async def post_events(self, appservice: AppService, events: Events) -> str:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            # TODO buffer transactions
            self.log.warning(f"Not sending transaction {events.txn_id} to {appservice.name}: "
                             f"websocket not connected")
            return "websocket-not-connected"
        self.log.debug(f"Sending transaction {events.txn_id} to {appservice.name} via websocket")
        try:
            await ws.send(raise_errors=True, command="transaction", status="ok",
                          txn_id=events.txn_id, **events.serialize())
        except Exception:
            return "websocket-send-fail"
        return "ok"

    async def post_syncproxy_error(self, appservice: AppService, txn_id: str, data: dict[str, Any]
                                   ) -> str:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            self.log.warning(f"Not sending syncproxy error {txn_id} to {appservice.name}: "
                             f"websocket not connected")
            return "websocket-not-connected"
        self.log.debug(f"Sending transaction {txn_id} to {appservice.name} via websocket")
        try:
            await ws.send(raise_errors=True, command="syncproxy_error", status="ok",
                          data={"txn_id": txn_id, **data})
        except Exception:
            return "websocket-send-fail"
        return "ok"

    async def ping(self, appservice: AppService) -> GlobalBridgeState:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            return make_ping_error("websocket-not-connected")
        try:
            raw_pong = await asyncio.wait_for(ws.request("ping"), timeout=45)
        except asyncio.TimeoutError:
            return make_ping_error("io-timeout")
        except Exception as e:
            self.log.warning(f"Failed to ping {appservice.name} ({appservice.id}) via websocket",
                             exc_info=True)
            return make_ping_error("websocket-fatal-error", message=str(e))
        return GlobalBridgeState.deserialize(migrate_state_data(raw_pong))
