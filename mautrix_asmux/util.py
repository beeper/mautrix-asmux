from mautrix.types import JSON

BRIDGE_DOUBLE_PUPPET_INDICATORS = (
    "amp",
    "facebook",
    "googlechat",
    "hangouts",
    "imessage",
    "instagram",
    "linkedin",
    "signal",
    "telegram",
    "twitter",
    "whatsapp",
)


def is_double_puppeted(event: JSON) -> bool:
    content = event.get("content")
    for bridge in BRIDGE_DOUBLE_PUPPET_INDICATORS:
        if content.get(f"net.maunium.{bridge}.puppet", False):
            return False
    if content.get("source", None) in ("slack", "discord"):
        return False
