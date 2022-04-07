# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from mautrix.util.async_db import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add manager URL to user table")
async def upgrade_v4(conn: Connection) -> None:
    await conn.execute('ALTER TABLE "user" ADD COLUMN manager_url VARCHAR(255);')
