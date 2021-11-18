# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Any, ClassVar, Optional, AsyncIterator
from contextlib import asynccontextmanager

import aiohttp
from attr import dataclass

from mautrix.api import HTTPAPI
from mautrix.types import SerializableAttrs


@dataclass
class PushKey(SerializableAttrs):
    _sess: ClassVar[Optional[aiohttp.ClientSession]] = None

    url: str
    app_id: str
    pushkey: str
    pushkey_ts: int
    data: dict[str, Any]

    @asynccontextmanager
    async def push(self, **data: Any) -> AsyncIterator[aiohttp.ClientResponse]:
        if self._sess is None:
            self.__class__._sess = aiohttp.ClientSession(headers={
                "User-Agent": HTTPAPI.default_ua,
            })

        payload = {
            "notification": {
                **data,
                "devices": [self.serialize()],
            }
        }

        async with self._sess.post(self.url, json=payload) as resp:
            yield resp
