# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import AsyncIterator, Callable
from contextlib import asynccontextmanager
import asyncio
import json
import logging

from aioredis import Redis

from ..database import AppService
from ..util import log_task_exceptions
from .as_proxy import Events

MAX_PDU_AGE_MS = 3 * 60 * 1000

logger = logging.getLogger("mau.api.as_queue")


class AppServiceQueue:
    """
    A Redis based queue used to buffer AS transactions to be sent via websockets.
    """

    az: AppService
    log: logging.Logger

    def __init__(
        self,
        redis: Redis,
        mxid_suffix: str,
        az: AppService,
        report_expired_pdu: Callable,
    ) -> None:
        self.redis = redis
        self.az = az
        self.queue_name = f"bridge-txns-{az.id}"
        self.owner_mxid = f"@{az.owner}{mxid_suffix}"
        self.report_expired_pdu = report_expired_pdu
        self.log = logger.getChild(az.name)

    @asynccontextmanager
    async def next(self) -> AsyncIterator[Events]:
        """
        Get and yield events from a Redis stream, removing them after successful processing.
        We use a stream here becacuse this allows us to do a blocking get without popping
        the message until after processing.
        """

        self.log.debug(f"Waiting for next txn in stream: {self.queue_name}")

        while True:
            streams_response = await self.redis.xread({self.queue_name: 0}, count=1, block=30000)
            if not streams_response:
                continue
            stream_id, raw_txn = streams_response[0][1][0]  # res -> queue(name, data) -> data[0]
            txn = Events.deserialize(json.loads(raw_txn[b"txn"]))
            expired = txn.pop_expired_pdu(self.owner_mxid, MAX_PDU_AGE_MS)
            if expired:
                self.log.warning(
                    f"Got {len(expired)} expired PDUs in stream: {self.queue_name}/{txn.txn_id}",
                )
                asyncio.create_task(
                    log_task_exceptions(self.log, self.report_expired_pdu(self.az, expired)),
                )
            if txn.is_empty:
                await self.redis.xdel(self.queue_name, stream_id)
            else:
                break

        yield txn

        # Now that we have successfully processed the txn, delete it from the stream
        await self.redis.xdel(self.queue_name, stream_id)

    async def push(self, txn: Events) -> None:
        """
        Push event transaction to the queue for this appservice.
        """

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xadd(self.queue_name, {"txn": json.dumps(txn.serialize())})
            pipe.expire(self.queue_name, 86400 * 7)  # 7 days just in case
            await pipe.execute()

    async def contains_pdus(self):
        """
        Loop through all pending txns for this AS and return true if any contain PDUs.
        """

        self.log.debug(f"Checking stream for PDUs: {self.queue_name}")

        raw_txns = await self.redis.xrange(self.queue_name)
        for _, raw_txn in raw_txns:
            txn = Events.deserialize(json.loads(raw_txn[b"txn"]))
            # Note: we remove the expired PDUs here for the purpose of indicating whether
            # the queue contains them. We don't actually write this back to Redis at all,
            # this is handled upon retrieval in next() above.
            txn.pop_expired_pdu(self.owner_mxid, MAX_PDU_AGE_MS)
            if txn.pdu:
                return True
        return False
