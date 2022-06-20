# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2022 Beeper, Inc. All rights reserved.
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union, cast
from uuid import UUID, uuid4
import asyncio
import json
import logging
import time

from aioredis import Redis

from mautrix.types import UserID
from mautrix.util.bridge_state import BridgeStateEvent, GlobalBridgeState
from mautrix.util.logging import TraceLogger

from ..database import AppService
from ..redis import RedisPubSub
from .as_proxy import Events
from .as_util import make_ping_error
from .errors import Error, WebsocketErrorResponse, WebsocketNotConnected
from .websocket_util import SENSITIVE_REQUESTS

if TYPE_CHECKING:
    from ..server import MuxServer

PING_REQUEST_CHANNEL = "bridge-ping-requests"
COMMAND_REQUEST_CHANNEL = "bridge-command-requests"
SYNCPROXY_ERROR_REQUEST_CHANNEL = "bridge-syncproxy-error-requests"
WAKEUP_NOTIFICATION_CHANNEL = "bridge-wakeup-notifications"

# Minimum delay since last websocket push before pre-emptively making a wakeup push on new message
PREEMPTIVE_WAKEUP_PUSH_DELAY = 30


def get_ping_request_queue(az: AppService) -> str:
    return f"bridge-ping-request-{az.id}"


def get_command_request_queue(az: AppService, request_id: str) -> str:
    return f"bridge-command-request-{az.id}-{request_id}"


def get_syncproxy_error_request_queue(az: AppService, txn_id: str) -> str:
    return f"bridge-syncproxy-error-request-{az.id}-{txn_id}"


class RequestTimedOut(Exception):
    pass


class AppServiceRequester:
    """
    The AS requester abstracts away the differences between http and websocket based
    appservices.
    """

    log: TraceLogger = cast(TraceLogger, logging.getLogger("mau.api.as_requester"))

    def __init__(
        self,
        server: "MuxServer",
        mxid_prefix: str,
        mxid_suffix: str,
        redis: Redis,
        redis_pubsub: RedisPubSub,
    ):
        self.server = server
        self.mxid_prefix = mxid_prefix
        self.mxid_suffix = mxid_suffix
        self.redis = redis
        self.redis_pubsub = redis_pubsub

        self.in_flight_command_requests: set[str] = set()
        self.in_flight_syncproxy_error_requests: set[str] = set()

    async def setup(self):
        self.log.info("Setting up Redis ping subscriptions")

        await self.redis_pubsub.subscribe(
            **{
                PING_REQUEST_CHANNEL: self.handle_bridge_ping_request,
                COMMAND_REQUEST_CHANNEL: self.handle_bridge_command_request,
                SYNCPROXY_ERROR_REQUEST_CHANNEL: self.handle_syncproxy_error_request,
                WAKEUP_NOTIFICATION_CHANNEL: self.handle_wakeup_appservice_notification,
            },
        )

    async def _handle_request_over_redis(
        self,
        log_msg: str,
        request_channel: str,
        request_queue: str,
        request_data: Union[str, dict],
        timeout_s: int,
    ):
        attempt = 1
        max_attempts = 5
        timeout_per_req = timeout_s / max_attempts

        while True:
            self.log.debug(f"Requesting {log_msg} (attempt={attempt}/{max_attempts})")
            await self.redis.publish(request_channel, json.dumps(request_data))
            response = await self.redis.blpop(request_queue, timeout=timeout_per_req)
            if response is not None:
                return response[1]

            if attempt >= max_attempts:
                self.log.warning(f"Gave up waiting for response over Redis for {log_msg}")
                raise RequestTimedOut
            attempt += 1

    async def _push_request_response(self, request_queue, result):
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.rpush(request_queue, json.dumps(result))
            # Expire the queue after 5 minutes if nothing consumes it
            pipe.expire(request_queue, 300)
            await pipe.execute()

    async def send_wakeup(self, az: AppService) -> None:
        asyncio.create_task(self.server.as_websocket.wakeup_appservice(az))
        await self.notify_appservice_wakeup(az)

    # Transactions (http & websocket)

    async def send_wakeup_if_not_connected(self, az: AppService) -> None:
        """
        If the bridge/websocket doesn't connect over the next ~30s, resend the
        wakeup request.
        """

        ping = await self.ping(az)
        if ping.bridge_state.state_event == BridgeStateEvent.BRIDGE_UNREACHABLE:
            self.log.info("Resending wakeup request after no connection to %s", az.name)
            await self.send_wakeup(az)

    async def send_transaction(self, az: AppService, events: Events) -> str:
        """
        Send a transaction of events to a target appservice, either via HTTP push
        or queued for a websocket to pull.
        """

        if az.push and not az.address:
            self.log.warning(
                f"Not sending transaction {events.txn_id} to {az.name}: no address configured",
            )
            return "no-address"

        self.log.trace(f"Queueing {events.txn_id} to {az.name}")

        if az.push:
            await self.server.as_http.get_queue(az).push(events)
            await self.server.as_http.ensure_pusher_running(az)
        else:
            await self.server.as_websocket.get_queue(az).push(events)
            if self.server.as_websocket.should_wakeup(
                az,
                min_time_since_last_push=PREEMPTIVE_WAKEUP_PUSH_DELAY,
                min_time_since_ws_message=PREEMPTIVE_WAKEUP_PUSH_DELAY,
            ):
                await self.send_wakeup(az)

            # TODO: is this still required?
            asyncio.create_task(self.send_wakeup_if_not_connected(az))

        return "ok"

    # Pings (http & websocket)

    async def handle_bridge_ping_request(self, message: str) -> None:
        """
        Handles and executes websocket ping requests as requested via Redis.
        """

        az = await AppService.get(UUID(json.loads(message)))
        if az and self.server.as_websocket.has_az_websocket(az):
            self.log.debug(f"Handling ping request for AZ: {az.name}")
            pong = await self.server.as_websocket.ping(az)
            await self._push_request_response(
                get_ping_request_queue(az),
                pong.serialize(),
            )

    async def request_bridge_ping(self, az: AppService) -> GlobalBridgeState:
        """
        This function requests a ping for a bridge websocket via Redis and returns
        the response. This is implemented in a loop that retries up to 5 * 10s times
        to receive the response before giving up. The `handle_bridge_ping_request`
        method executes the actual ping requests this function sends.
        """

        ping_request_queue = get_ping_request_queue(az)

        try:
            response = await self._handle_request_over_redis(
                log_msg=f"ping (appService={az.name}",
                request_channel=PING_REQUEST_CHANNEL,
                request_queue=ping_request_queue,
                request_data=str(az.id),
                timeout_s=50,
            )
        except RequestTimedOut:
            return make_ping_error("websocket-unknown-error")

        data = json.loads(response)
        # Workaround for: https://github.com/mautrix/python/pull/98
        if "remote_states" not in data:
            data["remoteState"] = None
        return GlobalBridgeState.deserialize(data)

    async def ping(self, az: AppService) -> GlobalBridgeState:
        try:
            if not az.push:
                pong = await self.request_bridge_ping(az)
            elif az.address:
                pong = await self.server.as_http.ping(az)
            else:
                self.log.warning(f"Not pinging {az.name}: no address configured")
                pong = make_ping_error("ping-no-remote")
        except Exception as e:
            self.log.exception(f"Fatal error pinging {az.name}")
            pong = make_ping_error("ping-fatal-error", message=str(e))

        user_id = UserID(f"@{az.owner}{self.mxid_suffix}")
        pong.bridge_state.fill()
        pong.bridge_state.user_id = user_id
        pong.bridge_state.remote_id = None
        pong.bridge_state.remote_name = None

        for remote in (pong.remote_states or {}).values():
            remote.source = remote.source or "bridge"
            remote.timestamp = remote.timestamp or int(time.time())
            remote.user_id = user_id

        return pong

    # Wakeup notifications (websocket only)

    async def handle_wakeup_appservice_notification(self, message: str) -> None:
        """
        Updates internal previous wakeup time for an appservice.
        """

        az = await AppService.get(UUID(message))
        if az:
            self.server.as_websocket.set_prev_wakeup_push(az)

    async def notify_appservice_wakeup(self, az: AppService) -> None:
        if not az.push:
            await self.redis.publish(WAKEUP_NOTIFICATION_CHANNEL, str(az.id))

    # Syncproxy errors (websocket only)

    async def handle_syncproxy_error_request(self, message: str) -> None:
        request = json.loads(message)
        txn_id: str = request["txn_id"]

        if txn_id in self.in_flight_syncproxy_error_requests:
            self.log.debug(f"Already handling syncproxy error request: {txn_id}")
            return

        az = await AppService.get(UUID(request["az_id"]))
        data: dict[str, Any] = request["data"]

        if az and self.server.as_websocket.has_az_websocket(az):
            self.in_flight_syncproxy_error_requests.add(txn_id)
            self.log.trace("Sending error transaction %s to %s: %s", txn_id, az.name, data)
            response = await self.server.as_websocket.post_syncproxy_error(az, txn_id, data)
            syncproxy_error_request_queue = get_syncproxy_error_request_queue(az, txn_id)
            await self._push_request_response(
                syncproxy_error_request_queue,
                response,
            )
            self.in_flight_syncproxy_error_requests.remove(txn_id)

    async def send_syncproxy_error(self, az: AppService, txn_id: str, data: dict[str, Any]) -> str:
        if az.push:
            raise Error.syncproxy_error_not_supported

        syncproxy_error_request_queue = get_syncproxy_error_request_queue(az, txn_id)

        try:
            response = await self._handle_request_over_redis(
                log_msg=f"post syncproxy error (appService={az.name}, txnId={txn_id})",
                request_channel=SYNCPROXY_ERROR_REQUEST_CHANNEL,
                request_queue=syncproxy_error_request_queue,
                request_data={
                    "az_id": str(az.id),
                    "txn_id": txn_id,
                    "data": data,
                },
                timeout_s=30,
            )
        except RequestTimedOut:
            return "websocket-send-fail"

        return json.loads(response)

    # Commands (websocket only)

    async def handle_bridge_command_request(self, message: str) -> None:
        """
        Handles and executes websocket commands as requested via Redis.
        """

        request = json.loads(message)
        request_id: str = request["req_id"]

        if request_id in self.in_flight_command_requests:
            self.log.debug(f"Already handling command request: {request_id}")
            return

        az = await AppService.get(UUID(request["az_id"]))
        command: str = request["command"]
        data: dict[str, Any] = request["data"]

        if az and self.server.as_websocket.has_az_websocket(az):
            self.in_flight_command_requests.add(request_id)
            log_data = data if command not in SENSITIVE_REQUESTS else "content omitted"
            self.log.debug(f"Handling command request for AZ: {az.name}: {command} ({log_data})")
            status: int = 200
            resp: Union[None, str, dict[str, Any]] = None

            try:
                resp = await self.server.as_websocket.post_command(az, command, data)
            except WebsocketErrorResponse as e:
                status = 400
                resp = e.data
            except Exception as e:
                self.log.warning(
                    f"Error sending command {command} to {az.name}: {type(e).__name__}: {e}",
                )
                status = 502
                resp = str(e)
                if isinstance(e, WebsocketNotConnected):
                    resp = "websocket not connected"
                    status = 503
                elif isinstance(e, asyncio.TimeoutError):
                    resp = "timed out waiting for response"
                    status = 504

            result = {"status": status, "resp": resp}

            command_request_queue = get_command_request_queue(az, request_id)
            await self._push_request_response(command_request_queue, result)
            self.in_flight_command_requests.remove(request_id)

    async def exec_command(
        self,
        az: AppService,
        command: str,
        data: dict[str, Any],
    ) -> tuple[int, Union[None, str, dict[str, Any]]]:
        if az.push:
            raise Error.exec_not_supported

        if self.server.as_websocket.should_wakeup(az):
            await self.send_wakeup(az)

        request_id = str(uuid4())
        command_request_queue = get_command_request_queue(az, request_id)

        try:
            response = await self._handle_request_over_redis(
                log_msg=(
                    f"command (appService={az.name}, requestId={request_id}, command={command})"
                ),
                request_channel=COMMAND_REQUEST_CHANNEL,
                request_queue=command_request_queue,
                request_data={
                    "az_id": str(az.id),
                    "req_id": request_id,
                    "command": command,
                    "data": data,
                },
                timeout_s=15,
            )
        except RequestTimedOut:
            return 504, "timed out waiting for response"

        data = json.loads(response)
        return data["status"], data["resp"]
