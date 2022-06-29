# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import TYPE_CHECKING, Any, Awaitable, Optional, Tuple, Union, cast
from collections import defaultdict
from uuid import UUID
import asyncio
import logging

from aiohttp import web
from aioredis import Redis
import aiohttp
import attr

from mautrix.api import HTTPAPI
from mautrix.appservice import AppServiceServerMixin
from mautrix.types import JSON, DeviceLists, DeviceOTKCount, UserID
from mautrix.util.bridge_state import BridgeState, BridgeStateEvent
from mautrix.util.logging import TraceLogger
from mautrix.util.message_send_checkpoint import (
    MessageSendCheckpoint,
    MessageSendCheckpointReportedBy,
    MessageSendCheckpointStatus,
    MessageSendCheckpointStep,
)
from mautrix.util.opt_prometheus import Counter

from ..database import AppService, Room
from ..segment import track_event
from ..util import log_task_exceptions, should_forward_pdu
from .as_util import Events
from .errors import Error

if TYPE_CHECKING:
    from ..server import MuxServer
    from .as_http import AppServiceHTTPHandler
    from .as_websocket import AppServiceWebsocketHandler

    CheckpointSender = Union["AppServiceProxy", AppServiceWebsocketHandler, AppServiceHTTPHandler]

BridgeState.default_source = "asmux"
BridgeState.human_readable_errors.update(
    {
        "ping-no-remote": "Couldn't make ping: no address configured",
        "websocket-not-connected": "The bridge is not connected to the server",
        "io-timeout": "Timeout while waiting for ping response",
        "http-connection-error": "HTTP client error while pinging: {message}",
        "ping-fatal-error": "Fatal error while pinging: {message}",
        "websocket-fatal-error": "Fatal error while pinging through websocket: {message}",
        "http-fatal-error": "Fatal error while pinging through HTTP: {message}",
        "http-not-json": "Non-JSON ping response",
    }
)

redactable_types = ("m.room.message", "m.room.encrypted", "m.sticker", "m.reaction")


def migrate_state_data(raw_pong: dict[str, Any], is_global: bool = True) -> JSON:
    if "ok" in raw_pong and "state_event" not in raw_pong:
        raw_pong["state_event"] = (
            BridgeStateEvent.CONNECTED if raw_pong["ok"] else BridgeStateEvent.UNKNOWN_ERROR
        )
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
    return cast(JSON, raw_pong)


async def send_message_checkpoints(
    self: "CheckpointSender", az: AppService, data: Union[dict[str, Any], JSON]
) -> None:
    url = f"{self.checkpoint_url}/bridgebox/{az.owner}/bridge/{az.prefix}/send_message_metrics"
    headers = {"Authorization": f"Bearer {az.real_as_token}"}
    try:
        async with self.api_server_sess.post(url, json=data, headers=headers) as resp:
            if not 200 <= resp.status < 300:
                text = await resp.text()
                text = text.replace("\n", "\\n")
                self.log.warning(
                    f"Unexpected status code {resp.status} sending message send"
                    f" checkpoints for {az.name}: {text}"
                )
    except Exception as e:
        self.log.warning(f"Failed to send message send checkpoints for {az.name}: {e}")


RECEIVED_EVENTS = Counter(
    "asmux_received_events", "Number of incoming events", labelnames=["type"]
)
DROPPED_EVENTS = Counter(
    "asmux_dropped_events", "Number of dropped events", labelnames=["type", "reason"]
)
ACCEPTED_EVENTS = Counter(
    "asmux_accepted_events",
    "Number of events that have a target appservice",
    labelnames=["owner", "bridge", "type"],
)


class AppServiceProxy(AppServiceServerMixin):
    log: TraceLogger = cast(TraceLogger, logging.getLogger("mau.api.as_proxy"))
    http: aiohttp.ClientSession

    hs_token: str
    mxid_prefix: str
    mxid_suffix: str
    az_locks: dict[UUID, asyncio.Lock]
    checkpoint_url: str
    api_server_sess: aiohttp.ClientSession

    def __init__(
        self,
        server: "MuxServer",
        mxid_prefix: str,
        mxid_suffix: str,
        hs_token: str,
        checkpoint_url: str,
        http: aiohttp.ClientSession,
        redis: Redis,
    ) -> None:
        super().__init__(ephemeral_events=True)
        self.server = server
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.hs_token = hs_token
        self.http = http
        self.redis = redis
        self.checkpoint_url = checkpoint_url
        self.az_locks = {}
        self.api_server_sess = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5), headers={"User-Agent": HTTPAPI.default_ua}
        )

    def get_appservice_lock(self, az):
        if az.id not in self.az_locks:
            lock = asyncio.Lock()
            self.az_locks[az.id] = lock
        return self.az_locks[az.id]

    async def send_message_send_checkpoints(self, az: AppService, events: Events):
        if not self.checkpoint_url:
            return

        checkpoints = []
        for event in events.pdu:
            homeserver_checkpoint = MessageSendCheckpoint(
                event_id=event.get("event_id"),
                room_id=event.get("room_id"),
                step=MessageSendCheckpointStep.HOMESERVER,
                timestamp=event.get("origin_server_ts"),
                status=MessageSendCheckpointStatus.SUCCESS,
                event_type=event.get("type"),
                reported_by=MessageSendCheckpointReportedBy.ASMUX,
            )

            try:
                client_checkpoint = attr.evolve(
                    homeserver_checkpoint,
                    step=MessageSendCheckpointStep.CLIENT,
                    timestamp=event["content"]["com.beeper.origin_client_ts"],
                )
                if "com.beeper.origin_client_type" in event["content"]:
                    client_checkpoint.client_type = event["content"][
                        "com.beeper.origin_client_type"
                    ]
                if "com.beeper.origin_client_version" in event["content"]:
                    client_checkpoint.client_version = event["content"][
                        "com.beeper.origin_client_version"
                    ]
            except (KeyError, TypeError):
                pass
            else:
                checkpoints.append(client_checkpoint.serialize())

            checkpoints.append(homeserver_checkpoint.serialize())

        if not checkpoints:
            return

        self.log.debug(f"Sending message send checkpoints for {az.name} (step: HOMESERVER/CLIENT)")
        await send_message_checkpoints(self, az, cast(JSON, {"checkpoints": checkpoints}))

    async def post_events(self, az: AppService, events: Events) -> str:
        async with self.get_appservice_lock(az):
            for type in events.types:
                ACCEPTED_EVENTS.labels(owner=az.owner, bridge=az.prefix, type=type).inc()

            asyncio.create_task(
                log_task_exceptions(self.log, self.send_message_send_checkpoints(az, events)),
            )

            return await self.server.as_requester.send_transaction(az, events)

    async def _get_az_from_user_id(self, user_id: UserID) -> Optional[AppService]:
        if (
            not user_id
            or not user_id.startswith(self.mxid_prefix)
            or not user_id.endswith(self.mxid_suffix)
        ):
            return None
        localpart: str = user_id[len(self.mxid_prefix) : -len(self.mxid_suffix)]
        try:
            owner, prefix, _ = localpart.split("_", 2)
        except ValueError:
            return None
        return await AppService.find(owner, prefix)

    async def register_room(self, event: JSON) -> Tuple[Optional[Room], Optional[AppService]]:
        try:
            if event["type"] != "m.room.member" or not event["state_key"].startswith(
                self.mxid_prefix
            ):
                return None, None
        except KeyError:
            return None, None
        user_id: UserID = event["state_key"]
        az = await self._get_az_from_user_id(user_id)
        if not az:
            return None, None
        room = Room(id=event["room_id"], owner=az.id, deleted=False)
        self.log.debug(f"Registering {az.name} ({az.id}) as the owner of {room.id}")
        await room.insert()
        return room, az

    async def _collect_events(
        self, events: Optional[list[JSON]], output: dict[UUID, Events], ephemeral: bool
    ) -> None:
        for event in events or []:
            evt_type = event.get("type", "")
            RECEIVED_EVENTS.labels(type=evt_type).inc()

            if evt_type in redactable_types and len(event.get("content", {})) == 0:
                DROPPED_EVENTS.labels(reason="event probably redacted", type=evt_type).inc()
                continue

            room_id = event.get("room_id")
            to_user_id = event.get("to_user_id")
            if room_id:
                room = await Room.get(room_id)
                if not room and not ephemeral:
                    room, az = await self.register_room(event)
                elif room:
                    az = await AppService.get(room.owner)
                else:
                    az = None
                if not room:
                    self.log.debug(f"No target found for event in {room_id}")
                    DROPPED_EVENTS.labels(reason="no target room found", type=evt_type).inc()
                elif room.deleted:
                    DROPPED_EVENTS.labels(reason="target room deleted", type=evt_type).inc()
                elif not ephemeral and not should_forward_pdu(az, event, self.mxid_suffix):
                    DROPPED_EVENTS.labels(reason="event filtered", type=evt_type).inc()
                    track_event(az, event)
                else:
                    output_array = output[room.owner].edu if ephemeral else output[room.owner].pdu
                    output_array.append(event)
                    output[room.owner].types.append(evt_type)

            elif to_user_id:
                az = await self._get_az_from_user_id(to_user_id)
                if az:
                    output[az.id].edu.append(event)
                else:
                    self.log.debug(f"No target found for to-device event to {to_user_id}")
                    DROPPED_EVENTS.labels(type=evt_type).inc()
            # elif event.get("type") == "m.presence":
            #     TODO find all appservices that care about the sender's presence.
            #     pass

    async def _collect_otk_count(
        self, otk_count: Optional[dict[UserID, DeviceOTKCount]], output: dict[UUID, Events]
    ) -> None:
        if not otk_count:
            return
        for user_id, user_otk_count in otk_count.items():
            az = await self._get_az_from_user_id(user_id)
            if az:
                # TODO metrics/logs for received OTK counts?
                output[az.id].otk_count[user_id] = user_otk_count

    async def _send_transactions(self, events: dict[UUID, Events]) -> dict[str, Any]:
        wait_for: dict[UUID, Awaitable[str]] = {}

        for appservice_id, az_events in events.items():
            appservice = await AppService.get(appservice_id)
            if appservice is None:
                continue
            self.log.debug(
                f"Preparing to send {len(az_events.pdu)} PDUs and {len(az_events.edu)} EDUs "
                f"from transaction {az_events.txn_id} to {appservice.name}"
            )
            wait_for[appservice.id] = asyncio.create_task(
                log_task_exceptions(self.log, self.post_events(appservice, az_events)),
            )

        sent_to: dict[str, str] = {}
        if wait_for:
            for appservice_id, az_task in wait_for.items():
                sent_to[str(appservice_id)] = await az_task
        return {
            # Technically this is not the case as we push to Redis first, but this is only for syncproxy
            # which is due to be removed once AS' receive to-device events.
            "com.beeper.asmux.sent_to": sent_to,
            "com.beeper.asmux.synchronous": True,
        }

    async def handle_transaction(
        self,
        txn_id: str,
        *,
        events: list[JSON],
        extra_data: JSON,
        ephemeral: Optional[list[JSON]] = None,
        device_otk_count: Optional[dict[UserID, DeviceOTKCount]] = None,
        device_lists: Optional[DeviceLists] = None,
    ) -> Any:
        outgoing_txn_id = extra_data.get("fi.mau.syncproxy.transaction_id", txn_id)
        log_txn_id = (
            txn_id if outgoing_txn_id == txn_id else f"{outgoing_txn_id} (wrapped in {txn_id})"
        )
        self.log.debug(
            f"Received transaction {log_txn_id} with {len(events or [])} PDUs "
            f"and {len(ephemeral or [])} EDUs"
        )
        synchronous_to = extra_data.get("com.beeper.asmux.synchronous_to", [])
        data: dict[UUID, Events] = defaultdict(lambda: Events(outgoing_txn_id))

        await self._collect_events(events, output=data, ephemeral=False)
        await self._collect_events(ephemeral, output=data, ephemeral=True)
        await self._collect_otk_count(device_otk_count, output=data)
        # TODO on device list changes, send notification to all bridges
        # await self._collect_device_lists(device_lists, output=data)
        # Special case to handle device lists from the sync proxy
        if len(synchronous_to) == 1:
            data[UUID(synchronous_to[0])].device_lists = device_lists or DeviceLists()

        return await self._send_transactions(data)

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
        async with self.get_appservice_lock(appservice):
            sent_to[str(appservice_id)] = await self.server.as_requester.send_syncproxy_error(
                appservice,
                outgoing_txn_id,
                data,
            )
        self.transactions.add(txn_id)
        return web.json_response(
            {
                "com.beeper.asmux.sent_to": sent_to,
                "com.beeper.asmux.synchronous": True,
            }
        )
