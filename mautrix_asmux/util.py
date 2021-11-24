from typing import TYPE_CHECKING

from mautrix.types import JSON
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
    content = event.get("content")
    if content.get("com.beeper.linkedin.puppet", False):
        return True
    for bridge in BRIDGE_DOUBLE_PUPPET_INDICATORS:
        if content.get(f"net.maunium.{bridge}.puppet", False):
            return True
    if content.get("source", None) in ("slack", "discord"):
        return True
    return False


def should_send_checkpoint(az: 'AppService', event: JSON, mxid_suffix: str) -> bool:
    return (
        event.get("type") in CHECKPOINT_TYPES_STR
        and event.get("sender") == f"@{az.owner}{mxid_suffix}"
        and not is_double_puppeted(event)
    )


__all__ = ["is_double_puppeted", "should_send_checkpoint"]
