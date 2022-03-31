# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Any, AsyncIterator, ClassVar, Optional
from contextlib import asynccontextmanager
import time

from attr import dataclass
import aiohttp

from mautrix.api import HTTPAPI
from mautrix.types import SerializableAttrs, field


@dataclass
class PushKey(SerializableAttrs):
    _sess: ClassVar[Optional[aiohttp.ClientSession]] = None

    url: str
    app_id: str
    pushkey: str
    pushkey_ts: int = field(factory=lambda: int(time.time() * 1000))
    data: dict[str, Any] = field(factory=lambda: {})

    @asynccontextmanager
    async def push(self, **data: Any) -> AsyncIterator[aiohttp.ClientResponse]:
        if self._sess is None:
            self.__class__._sess = aiohttp.ClientSession(
                headers={
                    "User-Agent": HTTPAPI.default_ua,
                }
            )

        payload = {
            "notification": {
                **data,
                "devices": [self.serialize()],
            }
        }

        async with self._sess.post(self.url, json=payload) as resp:
            yield resp
