# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, TYPE_CHECKING
import logging
import asyncio
import time

import aiohttp
from yarl import URL

if TYPE_CHECKING:
    from mautrix.types import JSON
    from .database import AppService
    from .api.as_proxy import Events

log = logging.getLogger("mau.posthog")
token: Optional[str] = None
host: str = "app.posthog.com"
http: aiohttp.ClientSession
mxid_suffix: str


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


def _get_tracking_event_type(appservice: 'AppService', event: 'JSON') -> Optional[str]:
    limit = int(time.time() * 1000) - 5 * 60 * 1000
    if event.get("origin_server_ts", limit) < limit:
        return None  # message is too old
    elif event.get("type", None) not in ("m.room.message", "m.room.encrypted"):
        return None  # not a message
    elif event.get("sender", None) != f"@{appservice.owner}{mxid_suffix}":
        return None  # message isn't from the user
    content = event.get("content")
    if not isinstance(content, dict):
        content = {}
    relates_to = event.get("m.relates_to")
    if not isinstance(relates_to, dict):
        relates_to = {}
    if relates_to.get("rel_type", None) == "m.replace":
        return None  # message is an edit
    for bridge in ("telegram", "whatsapp", "facebook", "hangouts", "amp", "twitter", "signal",
                   "instagram", "imessage"):
        if content.get(f"net.maunium.{bridge}.puppet", False):
            return "Outgoing remote event"
    if content.get("source", None) in ("slack", "discord"):
        return "Outgoing remote event"
    return "Outgoing Matrix event"


async def track_events(appservice: 'AppService', events: 'Events') -> None:
    for event in events.pdu:
        event_type = _get_tracking_event_type(appservice, event)
        if event_type:
            await track(event_type, event["sender"],
                        bridge_type=appservice.prefix, bridge_id=str(appservice.id))


def init(input_token: str, input_host: str, input_mxid_suffix: str,
         session: Optional[aiohttp.ClientSession] = None) -> None:
    global token, host, http, mxid_suffix
    token = input_token
    host = input_host
    mxid_suffix = input_mxid_suffix
    http = session or aiohttp.ClientSession(loop=asyncio.get_event_loop())
    log.info("Posthog tracking is enabled")
