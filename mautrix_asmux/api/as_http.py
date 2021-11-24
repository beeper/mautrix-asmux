# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
import logging
import asyncio
import json
import time

from yarl import URL
from aiohttp import ClientError, ClientTimeout, ContentTypeError
import aiohttp

from mautrix.api import HTTPAPI
from mautrix.util.bridge_state import GlobalBridgeState
from mautrix.util.message_send_checkpoint import (
    MessageSendCheckpoint, MessageSendCheckpointStep, MessageSendCheckpointReportedBy,
    MessageSendCheckpointStatus
)

from ..database import AppService
from ..util import should_send_checkpoint
from .as_proxy import Events, make_ping_error, migrate_state_data, send_message_checkpoints


class AppServiceHTTPHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_http")
    http: aiohttp.ClientSession
    mxid_suffix: str
    checkpoint_url: str

    def __init__(self, mxid_suffix: str, checkpoint_url: str, http: aiohttp.ClientSession) -> None:
        self.mxid_suffix = mxid_suffix
        self.http = http
        self.api_server_sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5),
                                                     headers={"User-Agent": HTTPAPI.default_ua})
        self.checkpoint_url = checkpoint_url

    async def post_events(self, az: AppService, events: Events) -> str:
        attempt = 0
        url = URL(az.address) / "_matrix/app/v1/transactions" / events.txn_id
        err_prefix = (f"Failed to send transaction {events.txn_id} "
                      f"({len(events.pdu)}p/{len(events.edu)}e) to {url}")
        retries = 10 if len(events.pdu) > 0 else 2
        backoff = 1
        last_error = ""
        while attempt < retries:
            attempt += 1
            self.log.debug(f"Sending transaction {events.txn_id} to {az.name} "
                           f"via HTTP, attempt #{attempt}")
            try:
                resp = await self.http.put(url.with_query({"access_token": az.hs_token}),
                                           json=events.serialize())
            except ClientError as e:
                last_error = e
                self.log.debug(f"{err_prefix}: {last_error}")
            except Exception:
                last_error = None
                self.log.exception(f"{err_prefix}")
                break
            else:
                if resp.status >= 400:
                    last_error = f"HTTP {resp.status}: {await resp.text()!r}"
                    self.log.debug(f"{err_prefix}: {last_error}")
                else:
                    return "ok"
            # Don't sleep after last attempt
            if attempt < retries:
                # The first few attempts only have a few seconds of backoff,
                # so let's not spam checkpoints for those.
                if attempt > 3:
                    self.report_error(az, events, MessageSendCheckpointStatus.WILL_RETRY,
                                      info=str(last_error), retry_num=attempt - 1)
                await asyncio.sleep(backoff)
                backoff *= 1.5
        last_error = f" (last error: {last_error})" if last_error else ""
        self.report_error(az, events, MessageSendCheckpointStatus.PERM_FAILURE,
                          info=last_error.strip(), retry_num=attempt - 1)
        self.log.warning(f"Gave up trying to send {events.txn_id} to {az.name}" + last_error)
        return "http-gave-up"

    def report_error(self, az: AppService, txn: Events, status: MessageSendCheckpointStatus,
                     info: str = "", retry_num: int = 0) -> None:
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
            if should_send_checkpoint(az, evt, self.mxid_suffix)
        ]
        asyncio.create_task(send_message_checkpoints(self, az, {"checkpoints": checkpoints}))

    async def ping(self, az: AppService) -> GlobalBridgeState:
        url = (URL(az.address) / "_matrix/app/com.beeper.bridge_state").with_query({
            "user_id": f"@{az.owner}{self.mxid_suffix}",
            # TODO remove after making sure it's safe to remove
            "remote_id": "",
        })
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
                return make_ping_error(f"ping-http-{resp.status}",
                                       f"Ping returned non-JSON body and HTTP {resp.status}")
            return make_ping_error("http-not-json")
        return GlobalBridgeState.deserialize(migrate_state_data(raw_pong))
