# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add proxy config to user table")
async def upgrade_v5(conn: Connection) -> None:
    await conn.execute('ALTER TABLE "user" ADD COLUMN proxy_config jsonb;')
