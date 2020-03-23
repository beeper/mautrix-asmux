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

import aiohttp
from aiohttp import web
from yarl import URL

from .config import Config
from .api import ClientProxy, AppServiceProxy, ManagementAPI


class MuxServer:
    log: logging.Logger = logging.getLogger("mau.server")
    app: web.Application
    runner: web.AppRunner
    loop: asyncio.AbstractEventLoop
    http: aiohttp.ClientSession

    host: str
    port: int

    def __init__(self, config: Config, http: Optional[aiohttp.ClientSession] = None,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        super().__init__()
        self.host = config["mux.hostname"]
        self.port = config["mux.port"]
        mxid_prefix = "@" + config["appservice.namespace.prefix"]
        mxid_suffix = ":" + config["homeserver.domain"]

        self.loop = loop or asyncio.get_event_loop()
        self.http = http or aiohttp.ClientSession(loop=self.loop)

        self.cs_proxy = ClientProxy(mxid_prefix=mxid_prefix, mxid_suffix=mxid_suffix,
                                    hs_address=URL(config["homeserver.address"]),
                                    as_token=config["appservice.as_token"], http=self.http)
        self.as_proxy = AppServiceProxy(mxid_prefix=mxid_prefix, mxid_suffix=mxid_suffix,
                                        hs_token=config["appservice.hs_token"], http=self.http,
                                        loop=self.loop)
        self.management_api = ManagementAPI(config=config, http=self.http)

        self.app = web.Application()
        self.as_proxy.register_routes(self.app)
        self.app.add_subapp("/_matrix/asmux", self.management_api.app)
        self.app.add_subapp("/_matrix", self.cs_proxy.app)
        self.runner = web.AppRunner(self.app)

    async def start(self) -> None:
        self.log.debug("Starting web server")
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def stop(self) -> None:
        self.log.debug("Stopping web server")
        await self.runner.cleanup()
