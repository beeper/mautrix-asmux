# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add deleted column for rooms")
async def upgrade_v10(conn: Connection) -> None:
    await conn.execute("ALTER TABLE room ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT false")
