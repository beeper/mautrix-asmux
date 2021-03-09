# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Replace VARCHAR(255) with TEXT")
async def upgrade_v7(conn: Connection) -> None:
    await conn.execute("ALTER TABLE room ALTER COLUMN id TYPE TEXT")
    await conn.execute("ALTER TABLE appservice ALTER COLUMN address TYPE TEXT")
    await conn.execute("ALTER TABLE appservice ALTER COLUMN hs_token TYPE TEXT")
    await conn.execute("ALTER TABLE appservice ALTER COLUMN as_token TYPE TEXT")
    await conn.execute('ALTER TABLE "user" ALTER COLUMN login_token TYPE TEXT')
    await conn.execute('ALTER TABLE "user" ALTER COLUMN api_token TYPE TEXT')
    await conn.execute('ALTER TABLE "user" ALTER COLUMN manager_url TYPE TEXT')
