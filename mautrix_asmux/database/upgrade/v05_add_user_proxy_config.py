# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from mautrix.util.async_db import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add proxy config to user table")  # type: ignore
async def upgrade_v5(conn: Connection) -> None:
    await conn.execute('ALTER TABLE "user" ADD COLUMN proxy_config jsonb;')
