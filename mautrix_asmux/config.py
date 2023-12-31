# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Dict, List, Optional
import random
import re
import string

from mautrix.util.config import (
    BaseFileConfig,
    BaseValidatableConfig,
    ConfigUpdateHelper,
    ForbiddenDefault,
    yaml,
)


class Config(BaseFileConfig, BaseValidatableConfig):
    registration_path: str
    _registration: Optional[Dict]
    _check_tokens: bool

    def __init__(self, path: str, registration_path: str, base_path: str) -> None:
        super().__init__(path, base_path)
        self.registration_path = registration_path
        self._registration = None
        self._check_tokens = True

    def save(self) -> None:
        super().save()
        if self._registration and self.registration_path:
            with open(self.registration_path, "w") as stream:
                yaml.dump(self._registration, stream)

    @staticmethod
    def _new_token() -> str:
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=64))

    @property
    def forbidden_defaults(self) -> List[ForbiddenDefault]:
        return [ForbiddenDefault("homeserver.domain", "example.com"),] + (
            [
                ForbiddenDefault(
                    "appservice.as_token",
                    "This value is generated when generating the registration",
                    "Did you forget to generate the registration?",
                ),
                ForbiddenDefault(
                    "appservice.hs_token",
                    "This value is generated when generating the registration",
                    "Did you forget to generate the registration?",
                ),
            ]
            if self._check_tokens
            else []
        )

    def do_update(self, helper: ConfigUpdateHelper) -> None:
        copy, copy_dict, base = helper

        copy("homeserver.address")
        copy("homeserver.domain")
        copy("homeserver.login_shared_secret")

        copy("appservice.address")

        copy("appservice.id")
        copy("appservice.bot_username")
        copy("appservice.bot_displayname")
        copy("appservice.bot_avatar")

        copy("appservice.as_token")
        copy("appservice.hs_token")

        copy("segment.token")
        copy("segment.host")

        copy("sentry.enabled")
        copy("sentry.dsn")

        copy("metrics.enabled")
        copy("metrics.port")

        copy("mux.hostname")
        copy("mux.port")
        copy("mux.database")
        copy("mux.public_address")
        if self.get("mux.shared_secret", "generate") == "generate":
            base["mux.shared_secret"] = self._new_token()
        else:
            copy("mux.shared_secret")

        copy_dict("mux.bridge_config_template_files")
        if "mux.status_endpoint" in self:
            status = self["mux.status_endpoint"]
            base["mux.remote_status_endpoint"] = status
            base["mux.bridge_status_endpoint"] = status.replace("/state", "/bridge_state")
        else:
            copy("mux.remote_status_endpoint")
            copy("mux.bridge_status_endpoint")
        copy("mux.message_send_checkpoint_endpoint")
        copy("mux.sync_proxy.url")
        copy("mux.sync_proxy.token")
        copy("mux.sync_proxy.asmux_address")

        copy("mux.redis")

        copy("logging")

    def generate_registration(self) -> None:
        prefix = re.escape(self["appservice.namespace.prefix"])
        exclusive = self["appservice.namespace.exclusive"]
        server_name = re.escape(self["homeserver.domain"])

        self["appservice.as_token"] = self._new_token()
        self["appservice.hs_token"] = self._new_token()

        self._registration = {
            "id": self["appservice.id"],
            "as_token": self["appservice.as_token"],
            "hs_token": self["appservice.hs_token"],
            "namespaces": {
                "users": [
                    {
                        "regex": f"@{prefix}.+:{server_name}",
                        "exclusive": exclusive,
                    }
                ],
                "aliases": [
                    {
                        "regex": f"#{prefix}.+:{server_name}",
                        "exclusive": exclusive,
                    }
                ],
            },
            "url": self["appservice.address"],
            "sender_localpart": self["appservice.bot_username"],
            "rate_limited": False,
            "push_ephemeral": True,
            "de.sorunome.msc2409.push_ephemeral": True,
        }
