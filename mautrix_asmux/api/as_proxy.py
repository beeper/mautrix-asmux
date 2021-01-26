# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional, List, Dict, NamedTuple
from collections import defaultdict
from uuid import UUID
import logging
import asyncio
import time

from yarl import URL
from aiohttp import web
from aiohttp.http import WSMessage, WSMsgType, WSCloseCode
import aiohttp

from mautrix.types import JSON
from mautrix.appservice import AppServiceServerMixin

from ..database import Room, AppService
from ..mixpanel import track
from .cs_proxy import ClientProxy
from .errors import Error

Events = NamedTuple('Events', pdu=List[JSON], edu=List[JSON])


class AppServiceProxy(AppServiceServerMixin):
    log: logging.Logger = logging.getLogger("mau.api.as_proxy")
    loop: asyncio.AbstractEventLoop
    http: aiohttp.ClientSession

    hs_token: str
    mxid_prefix: str
    mxid_suffix: str
    websockets: Dict[UUID, web.WebSocketResponse]

    def __init__(self, mxid_prefix: str, mxid_suffix: str, hs_token: str,
                 http: aiohttp.ClientSession, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(ephemeral_events=True)
        self.loop = loop
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_token = hs_token
        self.http = http
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
        finally:
            if self.websockets.get(az.id) == ws:
                del self.websockets[az.id]
        self.log.debug(f"Websocket transaction connection closed to {az.name}")
        return ws

    def _get_tracking_event_type(self, appservice: AppService, event: JSON) -> Optional[str]:
        limit = int(time.time() * 1000) - 5 * 60 * 1000
        if event.get("origin_server_ts", limit) < limit:
            return None  # message is too old
        elif event.get("type", None) not in ("m.room.message", "m.room.encrypted"):
            return None  # not a message
        elif event.get("sender", None) != f"@{appservice.owner}{self.mxid_suffix}":
            return None  # message isn't from the user
        content = event.get("content")
        if not isinstance(content, dict):
            content = {}
        relates_to = event.get("m.relates_to")
        if not isinstance(relates_to, dict):
            relates_to = {}
        if relates_to.get("rel_type", None) == "m.replace":
            return None  # message is an edit
        for bridge in ("telegram", "whatsapp", "facebook", "hangouts", "amp", "twitter", "signal",
                       "instagram"):
            if content.get(f"net.maunium.{bridge}.puppet", False):
                return "Outgoing remote event"
        if content.get("source", None) in ("slack", "discord"):
            return "Outgoing remote event"
        return "Outgoing Matrix event"

    async def track_events(self, appservice: AppService, events: Events) -> None:
        for event in events.pdu:
            event_type = self._get_tracking_event_type(appservice, event)
            if event_type:
                await track(event_type, event["sender"],
                            bridge_type=appservice.prefix, bridge_id=str(appservice.id))

    async def _post_events_ws(self, appservice: AppService, events: Events, txn_id: str) -> None:
        try:
            ws = self.websockets[appservice.id]
        except KeyError:
            # TODO buffer transactions
            self.log.warning(f"Not sending transaction {txn_id} to {appservice.name}: "
                             f"websocket not connected")
            return
        self.log.debug(f"Posting {len(events.pdu)} PDUs and {len(events.edu)} EDUs from "
                       f"transaction {txn_id} to {appservice.name} through websocket")
        await ws.send_json({
            "status": "ok",
            "txn_id": txn_id,
            "events": events.pdu,
            "ephemeral": events.edu,
        })

    async def _post_events_http(self, appservice: AppService, events: Events, txn_id: str) -> None:
        self.log.debug(f"Posting {len(events.pdu)} PDUs and {len(events.edu)} EDUs from "
                       f"transaction {txn_id} to {appservice.name}")
        url = URL(appservice.address) / "_matrix/app/v1/transactions" / txn_id
        try:
            resp = await self.http.put(url.with_query({"access_token": appservice.hs_token}),
                                       json={"events": events.pdu, "ephemeral": events.edu})
        except Exception:
            self.log.warning(f"Failed to post events to {url}", exc_info=True)
        else:
            if resp.status >= 400:
                self.log.warning(f"Failed to post events to {url}:"
                                 f" {resp.status} {await resp.text()}")
            else:
                await self.track_events(appservice, events)

    async def post_events(self, appservice: AppService, events: Events, txn_id: str) -> None:
        if not appservice.push:
            await self._post_events_ws(appservice, events, txn_id)
        elif appservice.address:
            await self._post_events_http(appservice, events, txn_id)
        else:
            self.log.warning(f"Not sending transaction {txn_id} to {appservice.name}: "
                             "no address configured")
            return

    async def register_room(self, event: JSON) -> Optional[Room]:
        try:
            if ((event["type"] != "m.room.member"
                 or not event["state_key"].startswith(self.mxid_prefix))):
                return None
        except KeyError:
            return None
        user_id: str = event["state_key"]
        if ((not user_id or not user_id.startswith(self.mxid_prefix)
             or not user_id.endswith(self.mxid_suffix))):
            return None
        localpart: str = user_id[len(self.mxid_prefix):-len(self.mxid_suffix)]
        try:
            owner, prefix, _ = localpart.split("_", 2)
        except ValueError:
            return None
        az = await AppService.find(owner, prefix)
        room = Room(id=event["room_id"], owner=az.id)
        self.log.debug(f"Registering {az.name} ({az.id}) as the owner of {room.id}")
        await room.insert()
        return room

    async def handle_transaction(self, txn_id: str, events: List[JSON],
                                 ephemeral: Optional[List[JSON]] = None) -> None:
        data: Dict[UUID, Events] = defaultdict(lambda: Events([], []))
        for event in events:
            room_id = event["room_id"]
            room = await Room.get(room_id)
            if not room:
                room = await self.register_room(event)
            if room:
                data[room.owner].pdu.append(event)
            else:
                self.log.warning(f"No target found for event in {room_id}")
        for event in ephemeral or []:
            room_id = event.get("room_id")
            if room_id:
                room = await Room.get(room_id)
                if room:
                    data[room.owner].edu.append(event)
            elif event.get("type") == "m.presence":
                # TODO find all appservices that care about the sender's presence.
                pass
        appservices = {appservice.id: appservice for appservice
                       in await AppService.get_many(list(data.keys()))}
        asyncio.ensure_future(asyncio.wait([self.post_events(appservices.get(owner), evts, txn_id)
                                            for owner, evts in data.items()]))
