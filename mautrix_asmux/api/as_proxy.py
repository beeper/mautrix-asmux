# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, List, Dict, NamedTuple
from collections import defaultdict
from uuid import UUID
import logging
import asyncio
import time

from yarl import URL
from aiohttp import web, ClientError
from aiohttp.http import WSMessage, WSMsgType, WSCloseCode
import aiohttp

from mautrix.types import JSON
from mautrix.appservice import AppServiceServerMixin
from mautrix.util.opt_prometheus import Counter

from ..database import Room, AppService
from ..posthog import track
from .cs_proxy import ClientProxy
from .errors import Error

Events = NamedTuple('Events', txn_id=str, pdu=List[JSON], edu=List[JSON], types=List[str])

RECEIVED_EVENTS = Counter("asmux_received_events", "Number of incoming events",
                          labelnames=["type"])
DROPPED_EVENTS = Counter("asmux_dropped_events", "Number of events with no target appservice",
                         labelnames=["type"])
ACCEPTED_EVENTS = Counter("asmux_accepted_events",
                          "Number of events that have a target appservice",
                          labelnames=["owner", "bridge", "type"])
SUCCESSFUL_EVENTS = Counter("asmux_successful_events",
                            "Number of PDUs that were successfully sent to the target appservice",
                            labelnames=["owner", "bridge", "type"])
FAILED_EVENTS = Counter("asmux_failed_events",
                        "Number of PDUs that were successfully sent to the target appservice",
                        labelnames=["owner", "bridge", "type"])


class AppServiceProxy(AppServiceServerMixin):
    log: logging.Logger = logging.getLogger("mau.api.as_proxy")
    loop: asyncio.AbstractEventLoop
    http: aiohttp.ClientSession

    hs_token: str
    mxid_prefix: str
    mxid_suffix: str
    websockets: Dict[UUID, web.WebSocketResponse]
    locks: Dict[UUID, asyncio.Lock]

    def __init__(self, mxid_prefix: str, mxid_suffix: str, hs_token: str,
                 http: aiohttp.ClientSession, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(ephemeral_events=True)
        self.loop = loop
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_token = hs_token
        self.http = http
        self.websockets = {}
        self.locks = defaultdict(lambda: asyncio.Lock())

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

    async def _post_events_ws(self, appservice: AppService, events: Events) -> bool:
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

    async def _post_events_http(self, appservice: AppService, events: Events) -> bool:
        attempt = 0
        url = URL(appservice.address) / "_matrix/app/v1/transactions" / events.txn_id
        err_prefix = (f"Failed to send transaction {events.txn_id} "
                      f"({len(events.pdu)}p/{len(events.edu)}e) to {url}")
        retries = 10 if len(events.pdu) > 0 else 2
        backoff = 1
        while attempt < retries:
            attempt += 1
            self.log.debug(f"Sending transaction {events.txn_id} to {appservice.name} "
                           f"via HTTP, attempt #{attempt}")
            try:
                resp = await self.http.put(url.with_query({"access_token": appservice.hs_token}),
                                           json={"events": events.pdu, "ephemeral": events.edu})
            except ClientError as e:
                self.log.warning(f"{err_prefix}: {e}")
            except Exception:
                self.log.exception(f"{err_prefix}")
                break
            else:
                if resp.status >= 400:
                    self.log.warning(f"{err_prefix}: HTTP {resp.status}: {await resp.text()!r}")
                else:
                    return True
            await asyncio.sleep(backoff)
            backoff *= 1.5
        self.log.warning(f"Gave up trying to send {events.txn_id} to {appservice.name}")
        return False

    async def post_events(self, appservice: AppService, events: Events) -> None:
        async with self.locks[appservice.id]:
            for type in events.types:
                ACCEPTED_EVENTS.labels(owner=appservice.owner, bridge=appservice.prefix,
                                       type=type).inc()
            ok = False
            try:
                if not appservice.push:
                    ok = await self._post_events_ws(appservice, events)
                elif appservice.address:
                    ok = await self._post_events_http(appservice, events)
                else:
                    self.log.warning(f"Not sending transaction {events.txn_id} "
                                     f"to {appservice.name}: no address configured")
            except Exception:
                self.log.exception(f"Fatal error sending transaction {events.txn_id} "
                                   f"to {appservice.name}")
            if ok:
                self.log.debug(f"Successfully sent {events.txn_id} to {appservice.name}")
                self.loop.create_task(self.track_events(appservice, events))
            metric = SUCCESSFUL_EVENTS if ok else FAILED_EVENTS
            for type in events.types:
                metric.labels(owner=appservice.owner, bridge=appservice.prefix, type=type).inc()

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
        self.log.debug(f"Received transaction {txn_id} with {len(events)} PDUs "
                       f"and {len(ephemeral or [])} EDUs")
        data: Dict[UUID, Events] = defaultdict(lambda: Events(txn_id, [], [], []))
        for event in events:
            RECEIVED_EVENTS.labels(type=event.get("type", "")).inc()
            room_id = event["room_id"]
            room = await Room.get(room_id)
            if not room:
                room = await self.register_room(event)
            if room:
                data[room.owner].pdu.append(event)
                data[room.owner].types.append(event.get("type", ""))
            else:
                self.log.warning(f"No target found for event in {room_id}")
                DROPPED_EVENTS.labels(type=event.get("type", "")).inc()
        for event in ephemeral or []:
            RECEIVED_EVENTS.labels(type=event.get("type", "")).inc()
            room_id = event.get("room_id")
            if room_id:
                room = await Room.get(room_id)
                if room:
                    data[room.owner].edu.append(event)
                    data[room.owner].types.append(event.get("type", ""))
            # elif event.get("type") == "m.presence":
            #     TODO find all appservices that care about the sender's presence.
            #     pass
            else:
                DROPPED_EVENTS.labels(type=event.get("type", "")).inc()
        for appservice_id, events in data.items():
            appservice = await AppService.get(appservice_id)
            self.log.debug(f"Preparing to send {len(events.pdu)} PDUs and {len(events.edu)} EDUs "
                           f"from transaction {events.txn_id} to {appservice.name}")
            self.loop.create_task(self.post_events(appservice, events))
