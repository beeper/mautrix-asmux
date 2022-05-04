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
from mautrix.util.bridge_state import GlobalBridgeState
from mautrix.util.logging import TraceLogger

from ..database import AppService
from ..redis import RedisPubSub
from .as_proxy import Events
from .as_util import make_ping_error, send_failed_metrics, send_successful_metrics
from .errors import Error, WebsocketErrorResponse, WebsocketNotConnected

if TYPE_CHECKING:
    from ..server import MuxServer

PING_REQUEST_CHANNEL = "bridge-ping-requests"
WAKEUP_REQUEST_CHANNEL = "bridge-wakeup-requests"
COMMAND_REQUEST_CHANNEL = "bridge-command-requests"


def get_ping_request_queue(az: AppService) -> str:
    return f"bridge-ping-request-{az.id}"


def get_command_request_queue(az: AppService, request_id: str) -> str:
    return f"bridge-command-request-{az.id}-{request_id}"


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

    async def setup(self):
        self.log.info("Setting up Redis ping subscriptions")

        await self.redis_pubsub.subscribe(
            **{
                PING_REQUEST_CHANNEL: self.handle_bridge_ping_request,
                WAKEUP_REQUEST_CHANNEL: self.handle_wakeup_appservice_request,
                COMMAND_REQUEST_CHANNEL: self.handle_bridge_command_request,
            },
        )

    # Transactions (http & websocket)

    async def send_transaction(self, az: AppService, events: Events) -> str:
        """
        Send a transaction of events to a target appservice, either via HTTP push
        or queued for a websocket to pull.
        """

        # NOTE: metrics for websocket events are only sent once the txn is
        # delivered in the AppServiceWebsocketHandler.
        if not az.push:
            self.log.trace(f"Queueing {events.txn_id} to {az.name}")
            queue = self.server.as_websocket.get_queue(az)
            await queue.push(events)
            if events.pdu:
                await self.wakeup_if_timeout(az)
            return "ok"

        if not az.address:
            self.log.warning(
                f"Not sending transaction {events.txn_id} to {az.name}: no address configured",
            )
            return "no-address"

        try:
            self.log.trace("Sending transaction to %s: %s", az.name, events)
            status = await self.server.as_http.post_events(az, events)
        except Exception:
            self.log.exception(f"Fatal error sending transaction {events.txn_id} to {az.name}")
            status = "fatal-error"

        if status == "ok":
            self.log.debug(f"Successfully sent {events.txn_id} to {az.name}")
            send_successful_metrics(az, events)
        else:
            send_failed_metrics(az, events)
        return status

    # Pings (http & websocket)

    async def handle_bridge_ping_request(self, message: str) -> None:
        """
        Handles and executes websocket ping requests as requested via Redis.
        """

        az = await AppService.get(UUID(message))
        if az and self.server.as_websocket.has_az_websocket(az):
            self.log.debug(f"Handling ping request for AZ: {az.name}")
            pong = await self.server.as_websocket.ping(az)
            ping_request_queue = get_ping_request_queue(az)

            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.rpush(ping_request_queue, json.dumps(pong.serialize()))
                # Expire the queue after 5 minutes if nothing consumes it
                pipe.expire(ping_request_queue, 300)
                await pipe.execute()

    async def request_bridge_ping(self, az: AppService) -> GlobalBridgeState:
        """
        This function requests a ping for a bridge websocket via Redis and returns
        the response. This is implemented in a loop that retries up to 5 * 10s times
        to receive the response before giving up. The `handle_bridge_ping_request`
        method executes the actual ping requests this function sends.
        """

        ping_request_queue = get_ping_request_queue(az)
        attempts = 1
        max_attempts = 5
        timeout_per_req = 30 / max_attempts  # 30s

        while True:
            self.log.debug(
                f"Requesting ping for AZ: {az.name} (attempt={attempts}/{max_attempts})",
            )
            await self.redis.publish(PING_REQUEST_CHANNEL, str(az.id))
            response = await self.redis.blpop(ping_request_queue, timeout=timeout_per_req)
            if response is not None:
                response = response[1]
                break

            if attempts >= max_attempts:
                self.log.warning(
                    f"Gave up waiting for ping response over Redis for {az.name} ({az.id})",
                )
                return make_ping_error("websocket-unknown-error")
            attempts += 1

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

    # Wakeups (websocket only)

    async def handle_wakeup_appservice_request(self, message: str) -> None:
        """
        Handles wakeup requests via Redis and executes wakeup if required.
        """

        az = await AppService.get(UUID(message))
        if az and self.server.as_websocket.has_az_websocket(az):
            self.log.debug(f"Handling wakeup request for: {az.name}")
            if self.server.as_websocket.should_wakeup(az, only_if_ws_timeout=True):
                asyncio.create_task(self.server.as_websocket.wakeup_appservice(az))

    async def wakeup_if_timeout(self, az: AppService) -> None:
        if not az.push:
            await self.redis.publish(WAKEUP_REQUEST_CHANNEL, str(az.id))

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
            self.log.debug(f"Handling command request for AZ: {az.name}: {command} ({data})")
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

            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.rpush(command_request_queue, json.dumps(result))
                # Expire the queue after 5 minutes if nothing consumes it
                pipe.expire(command_request_queue, 300)
                await pipe.execute()

            self.in_flight_command_requests.remove(request_id)

    async def exec_command(
        self,
        az: AppService,
        command: str,
        data: dict[str, Any],
    ) -> tuple[int, Union[None, str, dict[str, Any]]]:
        if az.push:
            raise Error.exec_not_supported

        request_id = str(uuid4())
        request = json.dumps(
            {
                "az_id": str(az.id),
                "req_id": request_id,
                "command": command,
                "data": data,
            },
        )

        command_request_queue = get_command_request_queue(az, request_id)

        attempts = 1
        max_attempts = 5
        timeout_per_req = 15 / max_attempts  # 15s

        while True:
            self.log.debug(
                f"Requesting command for AZ: {az.name} "
                f"(attempt={attempts}/{max_attempts}, requestId={request_id})",
            )
            await self.redis.publish(COMMAND_REQUEST_CHANNEL, request)
            response = await self.redis.blpop(command_request_queue, timeout=timeout_per_req)
            if response is not None:
                response = response[1]
                break

            if attempts >= max_attempts:
                self.log.warning(
                    "Gave up waiting for command response over Redis for "
                    f"{az.name} (requestId={request_id})",
                )
                return 504, "timed out waiting for response"
            attempts += 1

        data = json.loads(response)
        return data["status"], data["resp"]
