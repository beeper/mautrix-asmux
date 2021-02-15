# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional
import logging
import asyncio

import aiohttp
from yarl import URL

log = logging.getLogger("mau.posthog")
token: Optional[str] = None
host: str = "app.posthog.com"
http: aiohttp.ClientSession


async def track(event: str, user_id: str, **properties: str) -> None:
    if not token:
        return
    try:
        await http.post(URL.build(scheme="https", host=host, path="/capture/"), json={
            "api_key": token,
            "event": event,
            "properties": {
                **properties,
                "distinct_id": user_id,
            },
        })
        log.debug(f"Tracked {event} from {user_id}")
    except Exception:
        log.exception(f"Failed to track {event} from {user_id}")


def init(input_token: str, input_host: str, session: Optional[aiohttp.ClientSession] = None
         ) -> None:
    global token, host, http
    token = input_token
    host = input_host
    http = session or aiohttp.ClientSession(loop=asyncio.get_event_loop())
    log.info("Posthog tracking is enabled")
