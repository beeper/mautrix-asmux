# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from mautrix.util.async_db import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Remove manager URL from users")  # type: ignore
async def upgrade_v8(conn: Connection) -> None:
    await conn.execute('ALTER TABLE "user" DROP COLUMN manager_url')
