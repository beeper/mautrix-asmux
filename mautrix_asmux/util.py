from typing import TYPE_CHECKING, Awaitable, Optional, Union
import logging
import re

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

logger = logging.getLogger("mau.util")


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


async def log_task_exceptions(logger: Union[TraceLogger, logging.Logger], awaitable: Awaitable):
    try:
        return await awaitable
    except Exception:
        logger.exception("Exception in task")
        raise


def _client_regex(path: str):
    return re.compile(rf"^/_matrix/client/(?:r0|v3|unstable){path}")


def _media_regex(path: str):
    return re.compile(rf"^/_matrix/media{path}")


# NOTE: the order of these affects performance, most popular routes first
REGEX_TO_ENDPOINT = {
    _client_regex(r"/rooms/[^/]+/([a-z_]+)"): "/rooms/.../%s",
    _client_regex(
        r"/(sync|account/whoami|joined_rooms|register|createRoom|login|keys/(?:query|upload|claim))"
    ): "/%s",
    _client_regex(r"/(sendToDevice|pushrules)/.+"): "/%s/...",
    _client_regex(r"/profile/[^/]+/([a-z_]+)"): "/profile/.../%s",
    _media_regex(
        r"/unstable/fi.mau.msc2246/upload/.+"
    ): "/media/unstable/fi.mau.msc2246/upload/...",
    _media_regex(r"/unstable/fi.mau.msc2246/create"): "/media/unstable/fi.mau.msc2246/create",
    _media_regex(r"/(?:r0|v3)/(upload|config)"): "/media/v3/%s",
    _media_regex(r"/(?:r0|v3)/download/.+"): "/media/v3/download/...",
    _client_regex(r"/profile/[^/]+"): "/profile/...",
    _client_regex(r"/user/[^/]+/account_data/.+"): "/user/.../account_data/...",
    _client_regex(r"/user/[^/]+/rooms/![^/]+/tags"): "/user/.../rooms/.../tags",
    _client_regex(r"/user/[^/]+/filter"): "/user/.../filter",
    _client_regex(r"/join/.+"): "/join/...",
    _client_regex(
        r"/org.matrix.msc2716/rooms/[^/]+/batch_send"
    ): "/org.matrix.msc2716/rooms/.../batch_send",
    _client_regex(r"/presence/[^/]+/([a-z]+)"): "/presence/.../%s",
    _client_regex(r"/directory/room/.+"): "/directory/room/...",
    re.compile(r"/_matrix/client/versions"): "/versions",
}.items()


def get_metric_endpoint_for_url(url: URL) -> Optional[str]:
    url_path = url.path
    for regex, endpoint in REGEX_TO_ENDPOINT:
        match = regex.match(url_path)
        if match:
            return endpoint % match.groups()
    logger.warning("Path not converted to metric endpoint: %s", url_path)
    return None


__all__ = [
    "is_double_puppeted",
    "should_forward_pdu",
    "copy_headers_no_host",
    "get_metric_endpoint_for_url",
]
