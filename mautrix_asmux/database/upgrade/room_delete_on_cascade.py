# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from asyncpg import Connection

from .upgrade_table import upgrade_table


@upgrade_table.register(description="Add ON CASCADE DELETE for rooms")
async def upgrade_v2(conn: Connection) -> None:
    row = await conn.fetchrow("SELECT constraint_name FROM information_schema.table_constraints "
                              "WHERE table_name='room' AND constraint_type='FOREIGN KEY'")
    if not row:
        # To fix this error manually, create or modify the owner column's foreign key constraint
        # to be ON DELETE CASCADE, then set the version in the version table to 2.
        raise ValueError("Could not find foreign key constraint on room table")
    constraint_name = row["constraint_name"]
    await conn.execute("ALTER TABLE room ALTER COLUMN owner SET NOT NULL;")
    await conn.execute(f"""ALTER TABLE room
        DROP CONSTRAINT {constraint_name},
        ADD CONSTRAINT {constraint_name}
            FOREIGN KEY (owner)
            REFERENCES appservice(id)
            ON DELETE CASCADE
    """)
