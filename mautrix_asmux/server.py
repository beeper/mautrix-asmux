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
from typing import Optional
import logging
import asyncio

from aiohttp import web

from mautrix.appservice import AppServiceServerMixin


class MuxServer(AppServiceServerMixin):
    log: logging.Logger = logging.getLogger("mau.server")
    app: web.Application
    runner: web.AppRunner
    loop: asyncio.AbstractEventLoop

    host: str
    port: int

    def __init__(self, host: str, port: int, loop: Optional[asyncio.AbstractEventLoop] = None
                 ) -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.loop = loop or asyncio.get_event_loop()
        self.app = web.Application()
        self.register_routes(self.app)
        self.runner = web.AppRunner(self.app)

    async def start(self) -> None:
        self.log.debug("Starting web server")
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def stop(self) -> None:
        self.log.debug("Stopping web server")
        await self.runner.cleanup()
