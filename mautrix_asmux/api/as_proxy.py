# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, Any, Awaitable, Union, List, TYPE_CHECKING
from collections import defaultdict
from uuid import UUID
import logging
import asyncio
import time

from attr import dataclass
from aiohttp import web
import aiohttp
import attr

from mautrix.api import HTTPAPI
from mautrix.types import JSON, DeviceOTKCount, DeviceLists, UserID
from mautrix.appservice import AppServiceServerMixin
from mautrix.util.opt_prometheus import Counter
from mautrix.util.logging import TraceLogger
from mautrix.util.bridge_state import BridgeStateEvent, GlobalBridgeState, BridgeState
from mautrix.util.message_send_checkpoint import (
    MessageSendCheckpoint,
    MessageSendCheckpointReportedBy,
    MessageSendCheckpointStatus,
    MessageSendCheckpointStep,
    CHECKPOINT_TYPES,
)

from ..database import Room, AppService
from ..segment import track_events
from ..util import is_double_puppeted
from .errors import Error

if TYPE_CHECKING:
    from ..server import MuxServer
    from .as_websocket import AppServiceWebsocketHandler
    from .as_http import AppServiceHTTPHandler

    CheckpointSender = Union['AppServiceProxy', AppServiceWebsocketHandler, AppServiceHTTPHandler]

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


def make_ping_error(error: str, message: Optional[str] = None,
                    state_event: BridgeStateEvent = BridgeStateEvent.BRIDGE_UNREACHABLE
                    ) -> GlobalBridgeState:
    state_event = BridgeState(state_event=state_event, error=error, message=message)
    return GlobalBridgeState(remote_states=None, bridge_state=state_event)


def migrate_state_data(raw_pong: dict[str, Any], is_global: bool = True) -> dict[str, Any]:
    if "ok" in raw_pong and "state_event" not in raw_pong:
        raw_pong["state_event"] = (BridgeStateEvent.CONNECTED if raw_pong["ok"]
                                   else BridgeStateEvent.UNKNOWN_ERROR)
    if is_global and "remoteState" not in raw_pong:
        raw_pong = {
            "remoteState": {
                raw_pong.get("remote_id", "unknown"): raw_pong,
            },
            "bridgeState": {
                "state_event": BridgeStateEvent.RUNNING,
                "source": "asmux",
            },
        }
    return raw_pong


async def send_message_checkpoints(self: 'CheckpointSender', az: AppService, data: JSON) -> None:
    url = f"{self.checkpoint_url}/bridgebox/{az.owner}/bridge/{az.prefix}/send_message_metrics"
    headers = {"Authorization": f"Bearer {az.real_as_token}"}
    try:
        async with self.api_server_sess.post(url, json=data, headers=headers) as resp:
            if not 200 <= resp.status < 300:
                text = await resp.text()
                text = text.replace("\n", "\\n")
                self.log.warning(f"Unexpected status code {resp.status} sending message send"
                                 f" checkpoints for {az.name}: {text}")
    except Exception as e:
        self.log.warning(f"Failed to send message send checkpoints for {az.name}: {e}")


@dataclass
class Events:
    txn_id: str
    pdu: list[JSON] = attr.ib(factory=lambda: [])
    edu: list[JSON] = attr.ib(factory=lambda: [])
    types: list[str] = attr.ib(factory=lambda: [])
    otk_count: dict[UserID, DeviceOTKCount] = attr.ib(factory=lambda: {})
    device_lists: DeviceLists = attr.ib(factory=lambda: DeviceLists(changed=[], left=[]))

    def serialize(self) -> dict[str, Any]:
        output = {
            "events": self.pdu
        }
        if self.edu:
            output["ephemeral"] = self.edu
        if self.otk_count:
            output["device_one_time_keys_count"] = {user_id: otk.serialize()
                                                    for user_id, otk in self.otk_count.items()}
        if self.device_lists.changed or self.device_lists.left:
            output["device_lists"] = self.device_lists.serialize()
        return output

    @property
    def is_empty(self) -> bool:
        return (not self.pdu and not self.edu and not self.otk_count
                and not self.device_lists.changed and not self.device_lists.left)

    def pop_expired_pdu(self, owner: str, max_age: int) -> List[JSON]:
        now = int(time.time() * 1000)
        filtered = []
        new_pdu = []
        for evt in self.pdu:
            if evt.get("sender") != owner or is_double_puppeted(evt):
                continue
            ts = evt.get("origin_server_ts", {})
            if ts + max_age < now:
                filtered.append(evt)
            else:
                new_pdu.append(evt)
        self.pdu = new_pdu
        return filtered


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
    log: TraceLogger = logging.getLogger("mau.api.as_proxy")
    http: aiohttp.ClientSession

    hs_token: str
    mxid_prefix: str
    mxid_suffix: str
    locks: dict[UUID, asyncio.Lock]
    checkpoint_url: str
    api_server_sess: aiohttp.ClientSession

    def __init__(self, server: 'MuxServer', mxid_prefix: str, mxid_suffix: str, hs_token: str,
                 checkpoint_url: str, http: aiohttp.ClientSession) -> None:
        super().__init__(ephemeral_events=True)
        self.server = server
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_token = hs_token
        self.http = http
        self.locks = defaultdict(lambda: asyncio.Lock())
        self.checkpoint_url = checkpoint_url
        self.api_server_sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5),
                                                     headers={"User-Agent": HTTPAPI.default_ua})

    checkpoint_types = {str(evt_type) for evt_type in CHECKPOINT_TYPES}

    def should_send_checkpoint(self, az: AppService, event: JSON) -> bool:
        return (
            event.get("type") in self.checkpoint_types
            and event.get("sender") == f"@{az.owner}{self.mxid_suffix}"
            and not is_double_puppeted(event)
        )

    async def send_message_send_checkpoints(self, az: AppService, events: Events):
        if not self.checkpoint_url:
            return

        checkpoints = []
        for event in events.pdu:
            if not self.should_send_checkpoint(az, event):
                continue

            homeserver_checkpoint = MessageSendCheckpoint(
                event_id=event.get("event_id"),
                room_id=event.get("room_id"),
                step=MessageSendCheckpointStep.HOMESERVER,
                timestamp=event.get("origin_server_ts"),
                status=MessageSendCheckpointStatus.SUCCESS,
                event_type=event.get("type"),
                reported_by=MessageSendCheckpointReportedBy.ASMUX
            )

            try:
                client_checkpoint = attr.evolve(
                    homeserver_checkpoint,
                    step=MessageSendCheckpointStep.CLIENT,
                    timestamp=event["content"]["com.beeper.origin_client_ts"],
                )
            except (KeyError, TypeError):
                pass
            else:
                checkpoints.append(client_checkpoint.serialize())

            checkpoints.append(homeserver_checkpoint.serialize())

        if not checkpoints:
            return

        self.log.debug(f"Sending message send checkpoints for {az.name} to API server.")
        await send_message_checkpoints(self, az, {"checkpoints": checkpoints})

    async def post_events(self, az: AppService, events: Events) -> str:
        async with self.locks[az.id]:
            for type in events.types:
                ACCEPTED_EVENTS.labels(owner=az.owner, bridge=az.prefix, type=type).inc()

            asyncio.create_task(self.send_message_send_checkpoints(az, events))
            if not az.push:
                self.log.trace(f"Queueing {events.txn_id} to {az.name}")
                await self.server.as_websocket.queue_events(az, events)
                return "ok"
            elif not az.address:
                self.log.warning(f"Not sending transaction {events.txn_id} to {az.name}: "
                                 f"no address configured")
                return "no-address"
            try:
                self.log.trace("Sending transaction to %s: %s", az.name, events)
                status = await self.server.as_http.post_events(az, events)
            except Exception:
                self.log.exception(f"Fatal error sending transaction {events.txn_id} to {az.name}")
                status = "fatal-error"
            if status == "ok":
                self.log.debug(f"Successfully sent {events.txn_id} to {az.name}")
                asyncio.create_task(track_events(az, events))
            metric = SUCCESSFUL_EVENTS if status == "ok" else FAILED_EVENTS
            for type in events.types:
                metric.labels(owner=az.owner, bridge=az.prefix, type=type).inc()
            return status

    async def ping(self, az: AppService) -> GlobalBridgeState:
        try:
            if not az.push:
                pong = await self.server.as_websocket.ping(az)
            elif az.address:
                pong = await self.server.as_http.ping(az)
            else:
                self.log.warning(f"Not pinging {az.name}: no address configured")
                pong = make_ping_error("ping-no-remote")
        except Exception as e:
            self.log.exception(f"Fatal error pinging {az.name}")
            pong = make_ping_error("ping-fatal-error", message=str(e))
        user_id = f"@{az.owner}{self.mxid_suffix}"
        pong.bridge_state.fill()
        pong.bridge_state.user_id = user_id
        pong.bridge_state.remote_id = None
        pong.bridge_state.remote_name = None
        for remote in (pong.remote_states or {}).values():
            remote.source = remote.source or "bridge"
            remote.timestamp = remote.timestamp or int(time.time())
            remote.user_id = user_id
        return pong

    async def _get_az_from_user_id(self, user_id: UserID) -> Optional[AppService]:
        if ((not user_id or not user_id.startswith(self.mxid_prefix)
             or not user_id.endswith(self.mxid_suffix))):
            return None
        localpart: str = user_id[len(self.mxid_prefix):-len(self.mxid_suffix)]
        try:
            owner, prefix, _ = localpart.split("_", 2)
        except ValueError:
            return None
        return await AppService.find(owner, prefix)

    async def register_room(self, event: JSON) -> Optional[Room]:
        try:
            if ((event["type"] != "m.room.member"
                 or not event["state_key"].startswith(self.mxid_prefix))):
                return None
        except KeyError:
            return None
        user_id: UserID = event["state_key"]
        az = await self._get_az_from_user_id(user_id)
        if not az:
            return None
        room = Room(id=event["room_id"], owner=az.id, deleted=False)
        self.log.debug(f"Registering {az.name} ({az.id}) as the owner of {room.id}")
        await room.insert()
        return room

    async def _collect_events(self, events: list[JSON], output: dict[UUID, Events], ephemeral: bool
                              ) -> None:
        for event in (events or []):
            RECEIVED_EVENTS.labels(type=event.get("type", "")).inc()
            room_id = event.get("room_id")
            to_user_id = event.get("to_user_id")
            if room_id:
                room = await Room.get(room_id)
                if not room and not ephemeral:
                    room = await self.register_room(event)
                if room and not room.deleted:
                    output_array = output[room.owner].edu if ephemeral else output[room.owner].pdu
                    output_array.append(event)
                    output[room.owner].types.append(event.get("type", ""))
                else:
                    self.log.debug(f"No target found for event in {room_id}")
                    DROPPED_EVENTS.labels(type=event.get("type", "")).inc()
            elif to_user_id:
                az = await self._get_az_from_user_id(to_user_id)
                if az:
                    output[az.id].edu.append(event)
                else:
                    self.log.debug(f"No target found for to-device event to {to_user_id}")
                    DROPPED_EVENTS.labels(type=event.get("type", "")).inc()
            # elif event.get("type") == "m.presence":
            #     TODO find all appservices that care about the sender's presence.
            #     pass

    async def _collect_otk_count(self, otk_count: Optional[dict[UserID, DeviceOTKCount]],
                                 output: dict[UUID, Events]) -> None:
        if not otk_count:
            return
        for user_id, otk_count in otk_count.items():
            az = await self._get_az_from_user_id(user_id)
            if az:
                # TODO metrics/logs for received OTK counts?
                output[az.id].otk_count[user_id] = otk_count

    async def _send_transactions(self, events: dict[UUID, Events], synchronous_to: list[str]
                                 ) -> dict[str, Any]:
        wait_for: dict[UUID, Awaitable[str]] = {}

        for appservice_id, events in events.items():
            appservice = await AppService.get(appservice_id)
            self.log.debug(f"Preparing to send {len(events.pdu)} PDUs and {len(events.edu)} EDUs "
                           f"from transaction {events.txn_id} to {appservice.name}")
            task = asyncio.create_task(self.post_events(appservice, events))
            if str(appservice.id) in synchronous_to:
                wait_for[appservice.id] = task

        if not synchronous_to:
            return {"com.beeper.asmux.synchronous": False}

        sent_to: dict[str, str] = {}
        if wait_for:
            for appservice_id, task in wait_for.items():
                sent_to[str(appservice_id)] = await task
        return {
            "com.beeper.asmux.sent_to": sent_to,
            "com.beeper.asmux.synchronous": True,
        }

    async def handle_transaction(self, txn_id: str, *, events: list[JSON], extra_data: JSON,
                                 ephemeral: Optional[list[JSON]] = None,
                                 device_otk_count: Optional[dict[UserID, DeviceOTKCount]] = None,
                                 device_lists: Optional[DeviceLists] = None) -> Any:
        outgoing_txn_id = extra_data.get("fi.mau.syncproxy.transaction_id", txn_id)
        log_txn_id = (txn_id if outgoing_txn_id == txn_id
                      else f"{outgoing_txn_id} (wrapped in {txn_id})")
        self.log.debug(f"Received transaction {log_txn_id} with {len(events or [])} PDUs "
                       f"and {len(ephemeral or [])} EDUs")
        synchronous_to = extra_data.get("com.beeper.asmux.synchronous_to", [])
        data: dict[UUID, Events] = defaultdict(lambda: Events(outgoing_txn_id))

        await self._collect_events(events, output=data, ephemeral=False)
        await self._collect_events(ephemeral, output=data, ephemeral=True)
        await self._collect_otk_count(device_otk_count, output=data)
        # TODO on device list changes, send notification to all bridges
        # await self._collect_device_lists(device_lists, output=data)
        # Special case to handle device lists from the sync proxy
        if len(synchronous_to) == 1:
            data[UUID(synchronous_to[0])].device_lists = device_lists

        return await self._send_transactions(data, synchronous_to)

    async def handle_syncproxy_error(self, request: web.Request) -> web.Response:
        txn_id, data = await self._read_transaction_header(request)
        try:
            appservice_id = UUID(request.query["appservice_id"])
        except KeyError:
            raise Error.missing_appservice_id_query
        except ValueError:
            raise Error.invalid_appservice_id_query
        appservice = await AppService.get(appservice_id)
        if appservice is None:
            raise Error.appservice_not_found
        outgoing_txn_id = data.pop("fi.mau.syncproxy.transaction_id", txn_id)

        sent_to = {}
        async with self.locks[appservice.id]:
            try:
                self.log.trace("Sending error transaction %s to %s: %s", outgoing_txn_id,
                               appservice.name, data)
                if appservice.push:
                    raise Error.syncproxy_error_not_supported
                sent_to[str(appservice_id)] = await self.server.as_websocket.post_syncproxy_error(
                    appservice, outgoing_txn_id, data
                )
            except web.HTTPException:
                raise
            except Exception:
                self.log.exception("Fatal error sending syncproxy error transaction "
                                   f"{outgoing_txn_id} to {appservice.name}")
        self.transactions.add(txn_id)
        return web.json_response({
            "com.beeper.asmux.sent_to": sent_to,
            "com.beeper.asmux.synchronous": True,
        })
