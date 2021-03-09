# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Remove manager URL from users")
async def upgrade_v8(conn: Connection) -> None:
    await conn.execute('ALTER TABLE "user" DROP COLUMN manager_url')
