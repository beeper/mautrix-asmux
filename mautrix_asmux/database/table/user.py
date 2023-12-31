# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from __future__ import annotations

from typing import ClassVar, TypedDict, cast
import json
import logging
import secrets

from attr import asdict, dataclass
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    PrivateFormat,
    PublicFormat,
)

from mautrix.util.async_db import Connection

from .base import Base


class SSHKey(TypedDict):
    publicKey: str
    privateKey: str
    passphrase: str


class ProxySSHConfig(SSHKey, TypedDict):
    host: str
    port: int
    username: str
    hostKeySha256: str
    forwardPort: int
    forwardHost: str


class ProxySOCKSConfig(TypedDict):
    username: str
    password: str


class ProxyConfig(TypedDict):
    ssh: ProxySSHConfig
    socks: ProxySOCKSConfig


unset = object()


@dataclass
class User(Base):
    log = logging.getLogger("mau.db.user")
    id: str
    api_token: str
    login_token: str
    proxy_config: ProxyConfig | None

    cache_by_id: ClassVar[dict[str, User]] = {}
    cache_by_api_token: ClassVar[dict[str, User]] = {}

    @classmethod
    def empty_cache(cls) -> None:
        cls.cache_by_id = {}
        cls.cache_by_api_token = {}

    def __attrs_post_init__(self) -> None:
        if self.proxy_config and isinstance(self.proxy_config, str):
            self.proxy_config = json.loads(self.proxy_config)

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]
        del self.cache_by_api_token[self.api_token]

    def _add_to_cache(self) -> "User":
        self.cache_by_id[self.id] = self
        self.cache_by_api_token[self.api_token] = self
        return self

    def to_dict(self) -> dict:
        data = asdict(self)
        data.pop("proxy_config")
        return data

    def proxy_config_response(self, include_private_key: bool = False) -> ProxyConfig | None:
        if not self.proxy_config:
            return None
        proxy_cfg = {
            "ssh": {**self.proxy_config["ssh"]},
            "socks": {**self.proxy_config["socks"]},
        }
        if not include_private_key:
            del proxy_cfg["ssh"]["privateKey"]
            del proxy_cfg["ssh"]["passphrase"]
        return cast(ProxyConfig, proxy_cfg)

    @property
    def proxy_config_json(self) -> str | None:
        if self.proxy_config:
            return json.dumps(self.proxy_config)
        return None

    def generate_socks_config(self) -> ProxySOCKSConfig:
        return {
            "username": f"nova-{self.id}-proxy",
            "password": secrets.token_urlsafe(48),
        }

    def generate_ssh_key(self) -> SSHKey:
        key = Ed25519PrivateKey.generate()
        passphrase = secrets.token_urlsafe(48)
        privkey = key.private_bytes(
            Encoding.PEM,
            PrivateFormat.OpenSSH,
            BestAvailableEncryption(passphrase.encode("utf-8")),
        )
        pubkey = key.public_key().public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
        return {
            "publicKey": pubkey.decode("utf-8"),
            "privateKey": privkey.decode("utf-8"),
            "passphrase": passphrase,
        }

    @classmethod
    async def get(cls, user_id: str, *, conn: Connection | None = None) -> User | None:
        try:
            return cls.cache_by_id[user_id]
        except KeyError:
            pass
        row = await (conn or cls.db).fetchrow(
            "SELECT id, api_token, login_token, proxy_config " 'FROM "user" WHERE id=$1', user_id
        )
        return User(**cast(dict, row))._add_to_cache() if row else None

    @classmethod
    async def find_by_api_token(
        cls, api_token: str, *, conn: Connection | None = None
    ) -> User | None:
        try:
            return cls.cache_by_api_token[api_token]
        except KeyError:
            pass
        row = await (conn or cls.db).fetchrow(
            "SELECT id, api_token, login_token, proxy_config " 'FROM "user" WHERE api_token=$1',
            api_token,
        )
        return User(**cast(dict, row))._add_to_cache() if row else None

    @classmethod
    async def get_or_create(cls, id: str) -> "User":
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        async with cls.db.acquire() as conn, conn.transaction():
            user = await cls.get(id, conn=conn)
            if not user:
                user = User(
                    id=id,
                    api_token=secrets.token_urlsafe(48),
                    login_token=secrets.token_urlsafe(48),
                    proxy_config=None,
                )
                await user.insert(conn=conn)
            return user

    async def insert(self, *, conn: Connection | None = None) -> None:
        q = (
            'INSERT INTO "user" (id, api_token, login_token, proxy_config) '
            "VALUES ($1, $2, $3, $4)"
        )
        await (conn or self.db).execute(
            q,
            self.id,
            self.api_token,
            self.login_token,
            json.dumps(self.proxy_config) if self.proxy_config else None,
        )
        self._add_to_cache()
        self.log.info(f"Created user {self.id}")

    async def edit(
        self,
        proxy_config: ProxyConfig | None = cast(None, unset),
        *,
        conn: Connection | None = None,
    ) -> None:
        if proxy_config is unset:
            proxy_config = self.proxy_config
        proxy_config_str = json.dumps(proxy_config) if proxy_config else None
        q = 'UPDATE "user" SET proxy_config=$1 WHERE id=$2'
        await (conn or self.db).execute(q, proxy_config_str, self.id)
        self.proxy_config = proxy_config

    async def delete(self, *, conn: Connection | None = None) -> None:
        self._delete_from_cache()
        await (conn or self.db).execute('DELETE FROM "user" WHERE id=$1', self.id)
