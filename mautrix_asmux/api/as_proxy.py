# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, List, Dict, NamedTuple, TypedDict, TYPE_CHECKING
from collections import defaultdict
from uuid import UUID
import logging
import asyncio

import aiohttp

from mautrix.types import JSON
from mautrix.appservice import AppServiceServerMixin
from mautrix.util.opt_prometheus import Counter

from ..database import Room, AppService
from ..posthog import track_events

if TYPE_CHECKING:
    from ..server import MuxServer

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


class Pong(TypedDict):
    ok: bool
    error_source: str
    error: str
    message: str


class AppServiceProxy(AppServiceServerMixin):
    log: logging.Logger = logging.getLogger("mau.api.as_proxy")
    loop: asyncio.AbstractEventLoop
    http: aiohttp.ClientSession

    hs_token: str
    mxid_prefix: str
    mxid_suffix: str
    locks: Dict[UUID, asyncio.Lock]

    def __init__(self, server: 'MuxServer', mxid_prefix: str, mxid_suffix: str, hs_token: str,
                 http: aiohttp.ClientSession, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(ephemeral_events=True)
        self.server = server
        self.loop = loop
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_token = hs_token
        self.http = http
        self.locks = defaultdict(lambda: asyncio.Lock())

    async def ping(self, appservice: AppService) -> Pong:
        try:
            if not appservice.push:
                pong = await self.server.as_websocket.ping(appservice)
            elif appservice.address:
                pong = await self.server.as_http.ping(appservice)
            else:
                self.log.warning(f"Not pinging {appservice.name}: no address configured")
                return {"ok": False, "error_source": "asmux", "error": "ping-no-remote",
                        "message": f"Couldn't make ping: no address configured"}
            if "ok" not in pong:
                pong["ok"] = False
            if not pong["ok"]:
                if "error_source" not in pong:
                    pong["error_source"] = "unknown"
                if "error" not in pong:
                    pong["error"] = "unknown-error"
                if "message" not in pong:
                    pong["message"] = "Ping returned unknown error"
        except Exception as e:
            self.log.exception(f"Fatal error pinging {appservice.name}")
            return {"ok": False, "error_source": "asmux", "error": "ping-fatal-error",
                    "message": f"Fatal error while pinging: {e}"}

    async def post_events(self, appservice: AppService, events: Events) -> None:
        async with self.locks[appservice.id]:
            for type in events.types:
                ACCEPTED_EVENTS.labels(owner=appservice.owner, bridge=appservice.prefix,
                                       type=type).inc()
            ok = False
            try:
                if not appservice.push:
                    ok = await self.server.as_websocket.post_events(appservice, events)
                elif appservice.address:
                    ok = await self.server.as_http.post_events(appservice, events)
                else:
                    self.log.warning(f"Not sending transaction {events.txn_id} "
                                     f"to {appservice.name}: no address configured")
            except Exception:
                self.log.exception(f"Fatal error sending transaction {events.txn_id} "
                                   f"to {appservice.name}")
            if ok:
                self.log.debug(f"Successfully sent {events.txn_id} to {appservice.name}")
                self.loop.create_task(track_events(appservice, events))
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
