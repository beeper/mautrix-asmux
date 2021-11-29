# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, Dict, TYPE_CHECKING
from collections import defaultdict
import logging
import asyncio
import time

import aiohttp
from yarl import URL

from .util import is_double_puppeted

if TYPE_CHECKING:
    from mautrix.types import JSON
    from .database import AppService
    from .api.as_proxy import Events

log = logging.getLogger("mau.segment")
token: Optional[str] = None
host: str = "api.segment.io"
http: aiohttp.ClientSession
mxid_suffix: str

per_user_counter: Dict[str, int] = defaultdict(lambda: 0)
per_user_event_counter: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(lambda: 0))


async def track(event: str, user_id: str, **properties: str) -> None:
    if not token:
        return
    try:
        per_user_counter[user_id] += 1
        per_user_event_counter[user_id][event] += 1
        remote_count = per_user_event_counter[user_id]["Outgoing remote event"]
        matrix_count = per_user_event_counter[user_id]["Outgoing Matrix event"]
        properties["counter_user"] = str(per_user_counter[user_id])
        properties["counter_remote_event"] = str(remote_count)
        properties["counter_matrix_event"] = str(matrix_count)
        if matrix_count or remote_count:
            properties["counter_matrix_share"] = str(matrix_count / (matrix_count + remote_count))
        await http.post(URL.build(scheme="https", host=host, path="/v1/track"), json={
            "userId": user_id,
            "event": event,
            "properties": properties,
        }, auth=aiohttp.BasicAuth(login=token, encoding="utf-8"))
        log.debug(f"Tracked {event} from {user_id} with {properties}")
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
    content = event.get("content") or {}
    relates_to = content.get("m.relates_to")
    if not isinstance(relates_to, dict):
        relates_to = {}
    if relates_to.get("rel_type", None) == "m.replace":
        return None  # message is an edit
    if is_double_puppeted(event):
        return "Outgoing remote event"
    return "Outgoing Matrix event"


def track_event(az: 'AppService', pdu: 'JSON') -> None:
    event_type = _get_tracking_event_type(az, pdu)
    if event_type:
        asyncio.create_task(track(event_type, pdu["sender"], network=az.prefix,
                                  bridge_type=az.prefix, bridge_id=str(az.id)))


def track_events(az: 'AppService', events: 'Events') -> None:
    for event in events.pdu:
        track_event(az, event)


def init(input_token: str, input_host: str, input_mxid_suffix: str, session: aiohttp.ClientSession
         ) -> None:
    global token, host, http, mxid_suffix
    token = input_token
    host = input_host
    mxid_suffix = input_mxid_suffix
    http = session
    log.info("Segment tracking is enabled")
