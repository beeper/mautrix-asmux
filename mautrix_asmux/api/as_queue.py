# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import TYPE_CHECKING, AsyncIterator, List, Optional
from contextlib import asynccontextmanager
import asyncio

from mautrix.types import JSON, DeviceLists

from ..database import AppService
from .as_proxy import Events

if TYPE_CHECKING:
    from .as_websocket import AppServiceWebsocketHandler


class QueueWaiterOverridden(Exception):
    def __init__(self) -> None:
        super().__init__("Another task started waiting for the next transaction")


next_consumer_id = 0


class AppServiceQueue:
    az: AppService
    ws: "AppServiceWebsocketHandler"
    owner: str
    max_pdu_age_ms: int
    loop: asyncio.AbstractEventLoop
    _next_txn: Optional[Events]
    _current_txn: Optional[Events]
    _txn_waiter: Optional[asyncio.Future]
    _consumer_id: Optional[int]
    _cleanup_task: Optional[asyncio.Task]

    def __init__(self, az: AppService, ws: "AppServiceWebsocketHandler") -> None:
        self.az = az
        self.ws = ws
        self.owner = f"@{self.az.owner}{ws.mxid_suffix}"
        self.loop = asyncio.get_running_loop()
        self.max_pdu_age_ms = 3 * 60 * 1000
        self.cleanup_interval = 15
        self._next_txn = None
        self._current_txn = None
        self._txn_waiter = None
        self._consumer_id = None
        self._cleanup_task = None

    @asynccontextmanager
    async def next(self) -> AsyncIterator[Events]:
        if self._current_txn is None:
            if self._txn_waiter is not None and not self._txn_waiter.done():
                self._txn_waiter.set_exception(QueueWaiterOverridden())
            self._txn_waiter = self.loop.create_future()
            await self._txn_waiter

        assert self._current_txn is not None
        yield self._current_txn

        self._current_txn = self._next_txn
        self._next_txn = None

    def push(self, txn: Events) -> None:
        if self._current_txn is None:
            self._current_txn = txn
            if self._txn_waiter is not None and not self._txn_waiter.done():
                self._txn_waiter.set_result(None)
                self._txn_waiter = None
        elif self._next_txn is None:
            self._next_txn = txn
        else:
            self._next_txn.txn_id = f"{self._next_txn.txn_id},{txn.txn_id}"
            self._next_txn.types += txn.types
            self._next_txn.pdu += txn.pdu
            self._next_txn.edu += txn.edu
            _append_device_list(self._next_txn.device_lists, txn.device_lists)
            self._next_txn.otk_count |= txn.otk_count

        # Restart the cleanup task if it's not running and there's nothing consuming the queue.
        if (self._cleanup_task is None or self._cleanup_task.done()) and self._consumer_id is None:
            self._cleanup_task = asyncio.create_task(self._do_cleanup())

    def start_consuming(self) -> int:
        global next_consumer_id
        next_consumer_id += 1
        self._consumer_id = next_consumer_id
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            self._cleanup_task = None
        return self._consumer_id

    def stop_consuming(self, consumer_id: int) -> None:
        if self._consumer_id == consumer_id:
            self._consumer_id = None
            self._cleanup_task = asyncio.create_task(self._do_cleanup())

    async def _do_cleanup(self) -> None:
        while True:
            await asyncio.sleep(self.cleanup_interval)
            expired = self.pop_expired_pdu()
            if expired:
                asyncio.create_task(self.ws.report_expired_pdu(self.az, expired))
            if self.is_empty:
                # Stop looping if there's no events, we'll restart the cleanup task in push().
                self._cleanup_task = None
                break

    @property
    def is_empty(self) -> bool:
        return self._next_txn is None and self._current_txn is None

    @property
    def contains_pdus(self) -> bool:
        return bool(
            (self._current_txn and self._current_txn.pdu)
            or (self._next_txn and self._next_txn.pdu)
        )

    def pop_expired_pdu(self) -> List[JSON]:
        expired = []
        if self._current_txn is not None:
            expired = self._current_txn.pop_expired_pdu(self.owner, self.max_pdu_age_ms)
        if self._next_txn is not None:
            expired += self._next_txn.pop_expired_pdu(self.owner, self.max_pdu_age_ms)

        if self._current_txn is not None and self._current_txn.is_empty:
            self._current_txn = self._next_txn
            self._next_txn = None
        if self._next_txn is not None and self._next_txn.is_empty:
            self._next_txn = None
        return expired


def _append_device_list(l1: DeviceLists, l2: DeviceLists) -> None:
    l1.changed = list(set(l1.changed) | set(l2.changed))
    l1.left = list(set(l1.left) | set(l2.left))
