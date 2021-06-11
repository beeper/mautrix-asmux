# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
import logging
import asyncio
import json

from yarl import URL
from aiohttp import ClientError, ClientTimeout, ContentTypeError
import aiohttp

from ..database import AppService
from .as_proxy import Pong, Events


class AppServiceHTTPHandler:
    log: logging.Logger = logging.getLogger("mau.api.as_http")
    http: aiohttp.ClientSession
    mxid_suffix: str

    def __init__(self, mxid_suffix: str, http: aiohttp.ClientSession) -> None:
        self.mxid_suffix = mxid_suffix
        self.http = http

    async def post_events(self, appservice: AppService, events: Events) -> bool:
        attempt = 0
        url = URL(appservice.address) / "_matrix/app/v1/transactions" / events.txn_id
        err_prefix = (f"Failed to send transaction {events.txn_id} "
                      f"({len(events.pdu)}p/{len(events.edu)}e) to {url}")
        retries = 10 if len(events.pdu) > 0 else 2
        backoff = 1
        last_error = ""
        while attempt < retries:
            attempt += 1
            self.log.debug(f"Sending transaction {events.txn_id} to {appservice.name} "
                           f"via HTTP, attempt #{attempt}")
            try:
                resp = await self.http.put(url.with_query({"access_token": appservice.hs_token}),
                                           json={"events": events.pdu, "ephemeral": events.edu})
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
                    return True
            await asyncio.sleep(backoff)
            backoff *= 1.5
        last_error = f" (last error: {last_error})" if last_error else ""
        self.log.warning(f"Gave up trying to send {events.txn_id} to {appservice.name}"
                         + last_error)
        return False

    async def ping(self, appservice: AppService) -> Pong:
        url = (URL(appservice.address) / "_matrix/app/com.beeper.bridge_state").with_query({
            "user_id": f"@{appservice.owner}{self.mxid_suffix}",
        })
        headers = {"Authorization": f"Bearer {appservice.hs_token}"}
        try:
            resp = await self.http.post(url, headers=headers, timeout=ClientTimeout(total=45))
        except asyncio.TimeoutError:
            return {"ok": False, "error_source": "asmux", "error": "io-timeout",
                    "message": "Timeout while waiting for ping response"}
        except ClientError as e:
            return {"ok": False, "error_source": "asmux", "error": "http-connection-error",
                    "message": f"HTTP client error while pinging: {e}"}
        except Exception as e:
            self.log.exception(f"Error pinging {appservice.name}")
            return {"ok": False, "error_source": "asmux", "error": "http-fatal-error",
                    "message": f"Fatal error while pinging: {e}"}
        try:
            return await resp.json()
        except (json.JSONDecodeError, ContentTypeError):
            if resp.status >= 300:
                return {"ok": False, "error_source": "asmux", "error": f"ping-http-{resp.status}",
                        "message": f"Ping returned non-JSON body and HTTP {resp.status}"}
            return {"ok": False, "error_source": "asmux", "error": "http-not-json",
                    "message": f"Non-JSON ping response"}
