# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, Iterable
from contextlib import asynccontextmanager
import asyncio

from mautrix.types import DeviceLists

from ..database import AppService
from .as_proxy import Events


class QueueWaiterOverridden(Exception):
    def __init__(self) -> None:
        super().__init__("Another task started waiting for the next transaction")


class AppServiceQueue:
    az: AppService
    loop: asyncio.AbstractEventLoop
    _next_txn: Optional[Events]
    _current_txn: Optional[Events]
    _txn_waiter: Optional[asyncio.Future]

    def __init__(self, az: AppService) -> None:
        self.az = az
        self.loop = asyncio.get_running_loop()
        self._next_txn = None
        self._current_txn = None
        self._txn_waiter = None

    @asynccontextmanager
    async def next(self) -> Iterable[Events]:
        if self._current_txn is None:
            if self._txn_waiter is not None and not self._txn_waiter.done():
                self._txn_waiter.set_exception(QueueWaiterOverridden())
            self._txn_waiter = self.loop.create_future()
            await self._txn_waiter

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


def _append_device_list(l1: DeviceLists, l2: DeviceLists) -> None:
    l1.changed = list(set(l1.changed) | set(l2.changed))
    l1.left = list(set(l1.left) | set(l2.left))
