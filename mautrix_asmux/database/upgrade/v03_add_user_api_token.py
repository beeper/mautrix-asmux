# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from mautrix.util.async_db import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add user-specific asmux API access tokens")
async def upgrade_v3(conn: Connection) -> None:
    await conn.execute(
        'ALTER TABLE "user" ADD COLUMN api_token VARCHAR(255) NOT NULL '
        "DEFAULT substring(sha256(random()::text::bytea)::text, 3);"
    )
