from typing import TYPE_CHECKING, Awaitable, Optional, Union
from logging import Logger

from multidict import CIMultiDict
from yarl import URL

from mautrix.types import JSON
from mautrix.util.logging import TraceLogger
from mautrix.util.message_send_checkpoint import CHECKPOINT_TYPES

if TYPE_CHECKING:
    from .database import AppService

BRIDGE_DOUBLE_PUPPET_INDICATORS = (
    "amp",
    "facebook",
    "googlechat",
    "hangouts",
    "imessage",
    "instagram",
    "signal",
    "telegram",
    "twitter",
    "whatsapp",
)

CHECKPOINT_TYPES_STR = frozenset(str(evt_type) for evt_type in CHECKPOINT_TYPES)


def is_double_puppeted(event: JSON) -> bool:
    content = event.get("content") or {}
    if content.get("fi.mau.double_puppet_source"):
        return True
    if content.get("com.beeper.linkedin.puppet", False):
        return True
    for bridge in BRIDGE_DOUBLE_PUPPET_INDICATORS:
        if content.get(f"net.maunium.{bridge}.puppet", False):
            return True
    if content.get("source", None) in ("slack", "discord"):
        return True
    return False


def should_forward_pdu(az: Optional["AppService"], event: JSON, mxid_suffix: str) -> bool:
    return (
        event.get("type") in CHECKPOINT_TYPES_STR
        and (not az or event.get("sender") == f"@{az.owner}{mxid_suffix}")
        and not is_double_puppeted(event)
    )


def copy_headers_no_host(headers: CIMultiDict[str]) -> CIMultiDict[str]:
    headers_no_host = headers.copy()
    try:
        del headers_no_host["Host"]
    except KeyError:
        pass
    return headers_no_host


async def log_task_exceptions(logger: Union[TraceLogger, Logger], awaitable: Awaitable):
    try:
        return await awaitable
    except Exception:
        logger.exception("Exception in task")
        raise


IGNORE_URL_PATH_PARTS = {
    "_matrix",
    "client",
    "v3",
    "r0",
}

ALLOWED_URL_PATH_PARTS = {
    # Rooms
    "rooms",
    "createRoom",
    "joined_rooms",
    "read_markers",
    "send",
    "batch_send",
    # Events
    "join",
    "receipt",
    "event",
    "invite",
    "redact",
    "state",
    # Key management
    "keys",
    "query",
    "sendToDevice",
    # Unstable
    "unstable",
    "org.matrix.msc2716",
    "org.matrix.msc2432",
    "fi.mau.msc2246",
    # Media
    "media",
    "upload",
    # Account/profile
    "presence",
    "pushrules",
    "profile",
    "avatar_url",
    "displayname",
    "account",
    "whoami",
    "register",
    "login",
    "logout",
}


def get_metric_endpoint_for_url(url: URL) -> str:
    endpoint = [""]
    for part in url.path.split("/"):
        if part in IGNORE_URL_PATH_PARTS:
            continue

        if part in ALLOWED_URL_PATH_PARTS:
            endpoint.append(part)
        elif not endpoint or endpoint[-1] != "...":
            endpoint.append("...")

    return "/".join(endpoint)


__all__ = [
    "is_double_puppeted",
    "should_forward_pdu",
    "copy_headers_no_host",
    "get_metric_endpoint_for_url",
]
