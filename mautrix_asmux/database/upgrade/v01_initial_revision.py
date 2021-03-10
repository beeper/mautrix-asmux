# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Initial revision")
async def upgrade_v1(conn: Connection) -> None:
    await conn.execute("""CREATE TABLE "user"(
        id          VARCHAR(32) PRIMARY KEY,
        login_token VARCHAR(255) NOT NULL
    )""")
    await conn.execute("""CREATE TABLE appservice (
        id     UUID         PRIMARY KEY,
        owner  VARCHAR(32)  NOT NULL REFERENCES "user"(id),
        prefix VARCHAR(32)  NOT NULL,

        bot      VARCHAR(32)  NOT NULL,
        address  VARCHAR(255) NOT NULL,
        hs_token VARCHAR(255) NOT NULL,
        as_token VARCHAR(255) NOT NULL,

        UNIQUE (owner, prefix)
    )""")
    await conn.execute("""CREATE TABLE room (
        id    VARCHAR(255) PRIMARY KEY,
        owner UUID         REFERENCES appservice(id)
    )""")
