# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
import sys

from aiohttp import ClientSession, DummyCookieJar, TCPConnector

from mautrix.api import HTTPAPI
from mautrix.util.async_db import Database
from mautrix.util.program import Program

from . import __version__
from .config import Config
from .database import Base, upgrade_table
from .segment import init as init_segment
from .sentry import init as init_sentry
from .server import MuxServer


class AppServiceMux(Program):
    module = "mautrix_asmux"
    name = "mautrix-asmux"
    version = __version__
    command = "python -m mautrix-asmux"
    description = "A Matrix application service proxy and multiplexer"

    config_class = Config

    config: Config
    server: MuxServer
    client: ClientSession
    database: Database

    def prepare_arg_parser(self) -> None:
        super().prepare_arg_parser()
        self.parser.add_argument(
            "-g",
            "--generate-registration",
            action="store_true",
            help="generate registration and quit",
        )
        self.parser.add_argument(
            "-r",
            "--registration",
            type=str,
            default="registration.yaml",
            metavar="<path>",
            help="the path to save the generated registration to (not needed "
            "for running mautrix-asmux)",
        )

    def preinit(self) -> None:
        super().preinit()
        if self.args.generate_registration:
            self.generate_registration()
            sys.exit(0)

    def generate_registration(self) -> None:
        self.config.generate_registration()
        self.config.save()
        print(f"Registration generated and saved to {self.config.registration_path}")

    def prepare(self) -> None:
        super().prepare()
        self.database = Database.create(
            url=self.config["mux.database"], upgrade_table=upgrade_table
        )
        Base.db = self.database
        HTTPAPI.default_ua = f"{self.name}/{self.version} {HTTPAPI.default_ua}"
        self.client = self.loop.run_until_complete(self._create_client())
        self.server = MuxServer(self.config, http=self.client)

        if self.config["segment.token"]:
            init_segment(
                self.config["segment.token"],
                self.config["segment.host"],
                ":" + self.config["homeserver.domain"],
                self.client,
            )

        if self.config["sentry.enabled"]:
            init_sentry(self.config["homeserver.domain"], self.config["sentry_dsn"])

    async def _create_client(self) -> ClientSession:
        conn = TCPConnector(limit=0)
        return ClientSession(loop=self.loop, connector=conn, cookie_jar=DummyCookieJar())

    def prepare_config(self) -> None:
        self.config = self.config_class(
            self.args.config, self.args.registration, self.args.base_config
        )
        if self.args.generate_registration:
            self.config._check_tokens = False
        self.load_and_update_config()

    async def start(self) -> None:
        await self.database.start()
        await self.server.start()
        await super().start()

    async def stop(self) -> None:
        await self.server.stop()
        await self.database.stop()
        await super().stop()


AppServiceMux().run()
