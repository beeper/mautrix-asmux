# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2022 Beeper, Inc. All rights reserved.
from __future__ import annotations

from typing import Any, List
import time

from attr import dataclass
import attr

from mautrix.types import JSON, DeviceLists, DeviceOTKCount, UserID
from mautrix.util.bridge_state import BridgeState, BridgeStateEvent, GlobalBridgeState
from mautrix.util.opt_prometheus import Counter

from ..database import AppService
from ..segment import track_events
from ..util import is_double_puppeted

SUCCESSFUL_EVENTS = Counter(
    "asmux_successful_events",
    "Number of PDUs that were successfully sent to the target appservice",
    labelnames=["owner", "bridge", "type"],
)
FAILED_EVENTS = Counter(
    "asmux_failed_events",
    "Number of PDUs that were successfully sent to the target appservice",
    labelnames=["owner", "bridge", "type"],
)


def send_metrics(az: AppService, txn: Events, metric: Counter) -> None:
    for type_ in txn.types:
        metric.labels(owner=az.owner, bridge=az.prefix, type=type_).inc()


def send_successful_metrics(az: AppService, txn: Events) -> None:
    return send_metrics(az, txn, SUCCESSFUL_EVENTS)


def send_failed_metrics(az: AppService, txn: Events) -> None:
    track_events(az, txn)
    return send_metrics(az, txn, FAILED_EVENTS)


def make_ping_error(
    error: str,
    message: str | None = None,
    state_event: BridgeStateEvent = BridgeStateEvent.BRIDGE_UNREACHABLE,
) -> GlobalBridgeState:
    bridge_state = BridgeState(state_event=state_event, error=error, message=message)
    return GlobalBridgeState(remote_states=None, bridge_state=bridge_state)


@dataclass
class Events:
    txn_id: str
    pdu: list[JSON] = attr.ib(factory=lambda: [])
    edu: list[JSON] = attr.ib(factory=lambda: [])
    types: list[str] = attr.ib(factory=lambda: [])
    otk_count: dict[UserID, DeviceOTKCount] = attr.ib(factory=lambda: {})
    device_lists: DeviceLists = attr.ib(factory=lambda: DeviceLists(changed=[], left=[]))

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "Events":
        data["pdu"] = data.pop("events", [])
        data["edu"] = data.pop("ephemeral", [])

        otk_count = data.pop("device_one_time_keys_count", {})
        if otk_count:
            data["otk_count"] = {
                uid: DeviceOTKCount.deserialize(count) for uid, count in otk_count.items()
            }

        device_lists = data.get("device_lists")
        if device_lists:
            data["device_lists"] = DeviceLists.deserialize(device_lists)

        if "txn_id" not in data:
            data["txn_id"] = ""

        return cls(**data)

    def serialize(self) -> dict[str, Any]:
        output = {
            "txn_id": self.txn_id,
            "events": self.pdu,
        }
        if self.edu:
            output["ephemeral"] = self.edu
        if self.otk_count:
            output["device_one_time_keys_count"] = {  # type: ignore
                user_id: otk.serialize() for user_id, otk in self.otk_count.items()
            }
        if self.device_lists.changed or self.device_lists.left:
            output["device_lists"] = self.device_lists.serialize()
        return output

    @property
    def is_empty(self) -> bool:
        return (
            not self.pdu
            and not self.edu
            and not self.otk_count
            and not self.device_lists.changed
            and not self.device_lists.left
        )

    def pop_expired_pdu(self, owner: str, max_age: int) -> List[JSON]:
        now = int(time.time() * 1000)
        filtered = []
        new_pdu = []
        for evt in self.pdu:
            if evt.get("sender") != owner or is_double_puppeted(evt):
                continue
            ts = evt.get("origin_server_ts", {})
            if ts + max_age < now:
                filtered.append(evt)
            else:
                new_pdu.append(evt)
        self.pdu = new_pdu
        return filtered
