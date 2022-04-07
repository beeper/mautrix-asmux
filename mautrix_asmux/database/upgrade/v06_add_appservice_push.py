# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from mautrix.util.async_db import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add push boolean to appservice table")
async def upgrade_v6(conn: Connection) -> None:
    await conn.execute("ALTER TABLE appservice ADD COLUMN push BOOLEAN DEFAULT true")
