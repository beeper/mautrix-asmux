# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
import asyncio
import logging

from aiohttp import web
from yarl import URL
import aiohttp

# Annoyingly there are no stubbed types for asyncio redis yet
from redis.asyncio import Redis  # type: ignore

from .api import (
    AppServiceHTTPHandler,
    AppServiceProxy,
    AppServiceWebsocketHandler,
    ClientProxy,
    ManagementAPI,
)
from .config import Config
from .redis import RedisCacheHandler

class MuxServer:
    log: logging.Logger = logging.getLogger("mau.server")
    app: web.Application
    runner: web.AppRunner
    http: aiohttp.ClientSession

    as_proxy: AppServiceProxy
    as_websocket: AppServiceWebsocketHandler
    as_http: AppServiceHTTPHandler
    cs_proxy: ClientProxy
    management_api: ManagementAPI

    host: str
    port: int

    def __init__(self, config: Config, http: aiohttp.ClientSession) -> None:
        super().__init__()
        self.host = config["mux.hostname"]
        self.port = config["mux.port"]
        mxid_prefix = "@" + config["appservice.namespace.prefix"]
        mxid_suffix = ":" + config["homeserver.domain"]

        self.http = http

        self.redis = Redis.from_url(config["mux.redis"])
        self.redis_cache_handler = RedisCacheHandler(self.redis)

        checkpoint_url = config["mux.message_send_checkpoint_endpoint"]
        self.as_proxy = AppServiceProxy(
            server=self,
            mxid_prefix=mxid_prefix,
            mxid_suffix=mxid_suffix,
            hs_token=config["appservice.hs_token"],
            checkpoint_url=checkpoint_url,
            http=self.http,
        )
        self.as_http = AppServiceHTTPHandler(
            mxid_suffix=mxid_suffix, http=self.http, checkpoint_url=checkpoint_url
        )
        self.as_websocket = AppServiceWebsocketHandler(
            config=config, mxid_prefix=mxid_prefix, mxid_suffix=mxid_suffix
        )
        self.cs_proxy = ClientProxy(
            server=self,
            mxid_prefix=mxid_prefix,
            mxid_suffix=mxid_suffix,
            hs_address=URL(config["homeserver.address"]),
            as_token=config["appservice.as_token"],
            http=self.http,
            login_shared_secret=config["homeserver.login_shared_secret"],
        )
        self.management_api = ManagementAPI(
            config=config,
            http=self.http,
            server=self,
            redis_cache_handler=self.redis_cache_handler,
        )

        self.app = web.Application()
        self.as_proxy.register_routes(self.app)
        self.app.router.add_route(
            "PUT",
            "/_matrix/app/unstable/fi.mau.syncproxy/error/{transaction_id}",
            self.as_proxy.handle_syncproxy_error,
        )
        self.app.add_subapp("/_matrix/asmux/mxauth", self.management_api.mxauth_app)
        self.app.add_subapp("/_matrix/asmux/public", self.management_api.public_app)
        self.app.add_subapp("/_matrix/asmux/websocket", self.management_api.websocket_app)
        self.app.add_subapp("/_matrix/asmux", self.management_api.app)
        self.app.add_subapp("/_matrix", self.cs_proxy.app)
        self.runner = web.AppRunner(self.app)

    async def start(self) -> None:
        await self.redis.ping()
        self.log.debug("Starting web server")
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

    async def stop(self) -> None:
        asyncio.create_task(self.as_websocket.stop())
        asyncio.create_task(self.http.close())
        self.log.debug("Stopping web server")
        await self.runner.shutdown()
        await self.runner.cleanup()
        self.log.debug("Stopped web server")
