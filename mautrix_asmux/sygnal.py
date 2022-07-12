# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Optional
from contextlib import asynccontextmanager
import time

from attr import dataclass
import aiohttp

from mautrix.api import HTTPAPI
from mautrix.types import SerializableAttrs, field

if TYPE_CHECKING:
    from mautrix_asmux.database.table.appservice import AppService


@dataclass
class PushKey(SerializableAttrs):
    _sess: ClassVar[Optional[aiohttp.ClientSession]] = None

    url: str
    app_id: str
    pushkey: str
    pushkey_ts: int = field(factory=lambda: int(time.time() * 1000))
    data: dict[str, Any] = field(factory=lambda: {})

    @asynccontextmanager
    async def push(self, az: AppService, **data: Any) -> AsyncIterator[aiohttp.ClientResponse]:
        if self._sess is None:
            self.__class__._sess = aiohttp.ClientSession(
                headers={
                    "User-Agent": HTTPAPI.default_ua,
                }
            )

        device = self.serialize()
        device["user_id"] = az.owner

        payload = {
            "notification": {
                **data,
                "devices": [device],
            }
        }

        assert self._sess is not None
        async with self._sess.post(self.url, json=payload) as resp:
            yield resp
