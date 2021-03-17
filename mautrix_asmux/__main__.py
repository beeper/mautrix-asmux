# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
import sys

from aiohttp import ClientSession, TCPConnector, DummyCookieJar

from mautrix.util.program import Program
from mautrix.util.async_db import Database

from . import __version__
from .config import Config
from .server import MuxServer
from .database import Base, upgrade_table
from .posthog import init as init_posthog


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
        self.parser.add_argument("-g", "--generate-registration", action="store_true",
                                 help="generate registration and quit")
        self.parser.add_argument("-r", "--registration", type=str, default="registration.yaml",
                                 metavar="<path>",
                                 help="the path to save the generated registration to (not needed "
                                      "for running mautrix-asmux)")

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
        self.database = Database(url=self.config["mux.database"], upgrade_table=upgrade_table)
        Base.db = self.database
        self.client = self.loop.run_until_complete(self._create_client())
        self.server = MuxServer(self.config, http=self.client)
        if self.config["posthog.token"]:
            init_posthog(self.config["posthog.token"], self.config["posthog.host"],
                         ":" + self.config["homeserver.domain"], self.client)

    async def _create_client(self) -> ClientSession:
        conn = TCPConnector(limit=0)
        return ClientSession(loop=self.loop, connector=conn, cookie_jar=DummyCookieJar())

    def prepare_config(self) -> None:
        self.config = self.config_class(self.args.config, self.args.registration,
                                        self.args.base_config)
        if self.args.generate_registration:
            self.config._check_tokens = False
        self.load_and_update_config()

    async def start(self) -> None:
        await self.database.start()
        await self.server.start()
        await super().start()

    async def stop(self) -> None:
        await self.server.stop()
        await super().stop()


AppServiceMux().run()
