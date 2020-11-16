# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
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
                                    as_token=config["appservice.as_token"], http=self.http,
                                    login_shared_secret=config["homeserver.login_shared_secret"])
        self.as_proxy = AppServiceProxy(mxid_prefix=mxid_prefix, mxid_suffix=mxid_suffix,
                                        hs_token=config["appservice.hs_token"], http=self.http,
                                        loop=self.loop)
        self.management_api = ManagementAPI(config=config, http=self.http, server=self)

        self.app = web.Application()
        self.as_proxy.register_routes(self.app)
        self.app.add_subapp("/_matrix/asmux/public", self.management_api.public_app)
        self.app.add_subapp("/_matrix/asmux/mxauth", self.management_api.mxauth_app)
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
