# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add storage for bridge config fetch tokens")
async def upgrade_v9(conn: Connection) -> None:
    await conn.execute("ALTER TABLE appservice ADD COLUMN config_password_hash bytea")
    await conn.execute("ALTER TABLE appservice ADD COLUMN config_password_expiry BIGINT")
