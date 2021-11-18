# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add push_key column for appservices")
async def upgrade_v11(conn: Connection) -> None:
    await conn.execute("ALTER TABLE appservice ADD COLUMN push_key jsonb")
