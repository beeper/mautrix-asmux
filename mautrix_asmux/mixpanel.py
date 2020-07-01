# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional
import logging
import asyncio
import base64
import json

import aiohttp
from yarl import URL

log = logging.getLogger("mau.mixpanel")
token: Optional[str] = None
http: aiohttp.ClientSession


async def track(event: str, user_id: str, user_agent: str = "", **properties: str) -> None:
    if not token:
        return
    try:
        await http.post(URL("https://api.mixpanel.com/track/").with_query({
            "data": base64.b64encode(json.dumps({
                "event": event,
                "properties": {
                    **properties,
                    "token": token,
                    "distinct_id": user_id,
                }
            }).encode("utf-8")).decode("utf-8"),
        }), headers={
            "User-Agent": user_agent
        } if user_agent else {})
        log.debug(f"Tracked {event} from {user_id}")
    except Exception:
        log.exception(f"Failed to track {event} from {user_id}")


def init(input_token: str, session: Optional[aiohttp.ClientSession] = None) -> None:
    global token, http
    token = input_token
    http = session or aiohttp.ClientSession(loop=asyncio.get_event_loop())
    log.info("Mixpanel tracking is enabled")
