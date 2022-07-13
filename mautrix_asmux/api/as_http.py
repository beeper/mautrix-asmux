# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from uuid import UUID
import asyncio
import json
import logging
import time

from aiohttp import ClientError, ClientTimeout, ContentTypeError
from aioredis import Redis
from aioredis.lock import Lock
from yarl import URL
import aiohttp

from mautrix.api import HTTPAPI
from mautrix.util.bridge_state import GlobalBridgeState
from mautrix.util.message_send_checkpoint import (
    MessageSendCheckpoint,
    MessageSendCheckpointReportedBy,
    MessageSendCheckpointStatus,
    MessageSendCheckpointStep,
)

from ..database import AppService
from ..util import log_task_exceptions
from .as_proxy import Events, migrate_state_data, send_message_checkpoints
from .as_queue import AppServiceQueue
from .as_util import make_ping_error, send_failed_metrics, send_successful_metrics

RESTART_PUSHERS_QUEUE = "bridge-pushers-to-start"


class AppServiceHTTPHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_http")
    http: aiohttp.ClientSession
    mxid_suffix: str
    checkpoint_url: str
    queues: dict[UUID, AppServiceQueue]
    pusher_locks: dict[UUID, Lock]
    pusher_tasks: dict[UUID, asyncio.Task]

    def __init__(
        self,
        mxid_suffix: str,
        checkpoint_url: str,
        http: aiohttp.ClientSession,
        redis: Redis,
    ) -> None:
        self.mxid_suffix = mxid_suffix
        self.http = http
        self.redis = redis
        self.api_server_sess = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
            headers={"User-Agent": HTTPAPI.default_ua},
        )
        self.checkpoint_url = checkpoint_url
        self.queues = {}
        self.pusher_locks = {}
        self.pusher_tasks = {}

    async def setup(self) -> None:
        while True:
            az_id = await self.redis.rpop(RESTART_PUSHERS_QUEUE)
            if not az_id:
                break

            az = await AppService.get(az_id.decode())
            if az:
                await self.ensure_pusher_running(az)

    async def stop_pushers(self):
        self.log.debug("Stoping pushers")
        for az_id, task in self.pusher_tasks.items():
            if not task.done():
                task.cancel()
                await self.redis.lpush(RESTART_PUSHERS_QUEUE, str(az_id))

    def get_queue(self, az: AppService) -> AppServiceQueue:
        return self.queues.setdefault(
            az.id,
            AppServiceQueue(
                redis=self.redis,
                mxid_suffix=self.mxid_suffix,
                az=az,
            ),
        )

    def get_lock(self, az: AppService) -> Lock:
        return self.pusher_locks.setdefault(
            az.id,
            Lock(
                self.redis,
                f"bridge-push-lock-{az.id}",
                sleep=1.0,
                timeout=60,
            ),
        )

    async def ensure_pusher_running(self, az: AppService) -> None:
        # Shortcut: we have the pusher and it's running
        task = self.pusher_tasks.get(az.id)
        if task and not task.done():
            return

        # Another asmux instance is running the pusher, nothing to do here
        if await self.get_lock(az).locked():
            return

        self.log.debug("Starting HTTP pusher for %s", az.name)
        self.pusher_tasks[az.id] = asyncio.create_task(
            log_task_exceptions(self.log, self.post_events_from_queue(az)),
        )

    async def post_events_from_queue(self, az: AppService) -> None:
        lock = self.get_lock(az)
        queue = self.get_queue(az)

        async with lock:
            while True:
                async with queue.next(yield_empty=True) as txn:
                    if txn is None:  # stops the pusher, releasing the lock
                        break
                    status = await self.post_events(lock, az, txn)
                    if status == "ok":
                        send_successful_metrics(az, txn)
                    else:
                        send_failed_metrics(az, txn)

                await lock.extend(60, replace_ttl=True)

        self.log.debug("Stopping HTTP pusher for %s", az.name)

    async def post_events(self, lock: Lock, az: AppService, events: Events) -> str:
        attempt = 0
        url = URL(az.address) / "_matrix/app/v1/transactions" / events.txn_id
        err_prefix = (
            f"Failed to send transaction {events.txn_id} "
            f"({len(events.pdu)}p/{len(events.edu)}e) to {url}"
        )
        retries = 10 if len(events.pdu) > 0 else 2
        backoff = 1.0
        last_error = ""
        while attempt < retries:
            attempt += 1
            self.log.debug(
                f"Sending transaction {events.txn_id} to {az.name} via HTTP, attempt #{attempt}"
            )
            try:
                resp = await self.http.put(
                    url.with_query({"access_token": az.hs_token}),
                    json=events.serialize(),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
            except ClientError as e:
                last_error = str(e)
                self.log.debug(f"{err_prefix}: {last_error}")
            except Exception:
                last_error = ""
                self.log.exception(f"{err_prefix}")
                break
            else:
                if resp.status >= 400:
                    last_error = f"HTTP {resp.status}: {await resp.text()!r}"
                    self.log.debug(f"{err_prefix}: {last_error}")
                else:
                    self.log.debug(f"Successfully sent {events.txn_id} to {az.name}")
                    return "ok"
            # Don't sleep after last attempt
            if attempt < retries:
                # The first few attempts only have a few seconds of backoff,
                # so let's not spam checkpoints for those.
                if attempt > 3:
                    self.report_error(
                        az,
                        events,
                        MessageSendCheckpointStatus.WILL_RETRY,
                        info=str(last_error),
                        retry_num=attempt - 1,
                    )
                await lock.extend(backoff)
                await asyncio.sleep(backoff)
                backoff *= 1.5
        last_error = f" (last error: {last_error})" if last_error else ""
        self.report_error(
            az,
            events,
            MessageSendCheckpointStatus.PERM_FAILURE,
            info=last_error.strip(),
            retry_num=attempt - 1,
        )
        self.log.warning(f"Gave up trying to send {events.txn_id} to {az.name}" + last_error)
        return "http-gave-up"

    def report_error(
        self,
        az: AppService,
        txn: Events,
        status: MessageSendCheckpointStatus,
        info: str = "",
        retry_num: int = 0,
    ) -> None:
        if not txn.pdu:
            return
        checkpoints = [
            MessageSendCheckpoint(
                event_id=evt.get("event_id"),
                room_id=evt.get("room_id"),
                step=MessageSendCheckpointStep.BRIDGE,
                timestamp=int(time.time() * 1000),
                status=status,
                event_type=evt.get("type"),
                reported_by=MessageSendCheckpointReportedBy.ASMUX,
                retry_num=retry_num,
                info=info,
            ).serialize()
            for evt in txn.pdu
        ]
        asyncio.create_task(send_message_checkpoints(self, az, {"checkpoints": checkpoints}))

    async def ping(self, az: AppService) -> GlobalBridgeState:
        url = (URL(az.address) / "_matrix/app/com.beeper.bridge_state").with_query(
            {
                "user_id": f"@{az.owner}{self.mxid_suffix}",
                # TODO remove after making sure it's safe to remove
                "remote_id": "",
            }
        )
        headers = {"Authorization": f"Bearer {az.hs_token}"}
        try:
            resp = await self.http.post(url, headers=headers, timeout=ClientTimeout(total=45))
        except asyncio.TimeoutError:
            return make_ping_error("io-timeout")
        except ClientError as e:
            return make_ping_error("http-connection-error", message=str(e))
        except Exception as e:
            self.log.warning(f"Failed to ping {az.name} ({az.id}) via HTTP", exc_info=True)
            return make_ping_error("http-fatal-error", message=str(e))
        try:
            raw_pong = await resp.json()
        except (json.JSONDecodeError, ContentTypeError):
            if resp.status >= 300:
                return make_ping_error(
                    f"ping-http-{resp.status}",
                    f"Ping returned non-JSON body and HTTP {resp.status}",
                )
            return make_ping_error("http-not-json")
        return GlobalBridgeState.deserialize(migrate_state_data(raw_pong))
