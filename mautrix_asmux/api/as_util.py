# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2022 Beeper, Inc. All rights reserved.
from __future__ import annotations

from mautrix.util.bridge_state import BridgeState, BridgeStateEvent, GlobalBridgeState


def make_ping_error(
    error: str,
    message: str | None = None,
    state_event: BridgeStateEvent = BridgeStateEvent.BRIDGE_UNREACHABLE,
) -> GlobalBridgeState:
    bridge_state = BridgeState(state_event=state_event, error=error, message=message)
    return GlobalBridgeState(remote_states=None, bridge_state=bridge_state)
