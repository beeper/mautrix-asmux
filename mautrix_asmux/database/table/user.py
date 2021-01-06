# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2020 Nova Technology Corporation, Ltd. All rights reserved.
from typing import Optional, Dict, ClassVar, TypedDict
import logging
import random
import string
import json

from cryptography.hazmat.primitives.serialization import (Encoding, PrivateFormat, PublicFormat,
                                                          BestAvailableEncryption)
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from attr import dataclass, asdict
import asyncpg

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
    manager_url: Optional[str]
    proxy_config: Optional[ProxyConfig]

    cache_by_id: ClassVar[Dict[str, 'User']] = {}
    cache_by_api_token: ClassVar[Dict[str, 'User']] = {}

    def __attrs_post_init__(self) -> None:
        self.cache_by_id[self.id] = self
        self.cache_by_api_token[self.api_token] = self
        if self.proxy_config and isinstance(self.proxy_config, str):
            self.proxy_config = json.loads(self.proxy_config)

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]
        del self.cache_by_api_token[self.api_token]

    def to_dict(self) -> dict:
        data = asdict(self)
        data.pop("proxy_config")
        return data

    def proxy_config_response(self, include_private_key: bool = False) -> ProxyConfig:
        proxy_cfg = {
            "ssh": {**self.proxy_config["ssh"]},
            "socks": {**self.proxy_config["socks"]},
        }
        if not include_private_key:
            del proxy_cfg["ssh"]["privateKey"]
            del proxy_cfg["ssh"]["passphrase"]
        return proxy_cfg

    @property
    def proxy_config_json(self) -> Optional[str]:
        if self.proxy_config:
            return json.dumps(self.proxy_config)
        return None

    def generate_socks_config(self) -> ProxySOCKSConfig:
        return {
            "username": f"nova-{self.id}-proxy",
            "password": self._random(50),
        }

    def generate_ssh_key(self) -> SSHKey:
        key = Ed25519PrivateKey.generate()
        passphrase = self._random(50)
        privkey = key.private_bytes(Encoding.PEM, PrivateFormat.OpenSSH,
                                    BestAvailableEncryption(passphrase.encode("utf-8")))
        pubkey = key.public_key().public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
        return {
            "publicKey": pubkey.decode("utf-8"),
            "privateKey": privkey.decode("utf-8"),
            "passphrase": passphrase,
        }

    @classmethod
    async def get(cls, id: str, *, conn: Optional[asyncpg.Connection] = None
                  ) -> Optional['User']:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT id, api_token, login_token, manager_url, proxy_config '
                                  'FROM "user" WHERE id=$1', id)
        return User(**row) if row else None

    @classmethod
    async def find_by_api_token(cls, api_token: str, *, conn: Optional[asyncpg.Connection] = None
                                ) -> Optional['User']:
        try:
            return cls.cache_by_api_token[api_token]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT id, api_token, login_token, manager_url, proxy_config '
                                  'FROM "user" WHERE api_token=$1', api_token)
        return User(**row) if row else None

    @staticmethod
    def _random(length: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    async def get_or_create(cls, id: str) -> 'User':
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        async with cls.db.acquire() as conn, conn.transaction():
            user = await cls.get(id, conn=conn)
            if not user:
                user = User(id=id, api_token=cls._random(64), login_token=cls._random(64),
                            manager_url=None, proxy_config=None)
                await user.insert(conn=conn)
            return user

    async def insert(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        self.log.info(f"Creating new user {self.id}")
        conn = conn or self.db
        q = ('INSERT INTO "user" (id, api_token, login_token, manager_url, proxy_config) '
             'VALUES ($1, $2, $3, $4)')
        await conn.execute(q, self.id, self.api_token, self.login_token, self.manager_url,
                           json.dumps(self.proxy_config) if self.proxy_config else None)

    async def edit(self, manager_url: str = unset, proxy_config: Optional[ProxyConfig] = unset, *,
                   conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        if manager_url is unset:
            manager_url = self.manager_url
        if proxy_config is unset:
            proxy_config = self.proxy_config
        proxy_config_str = json.dumps(proxy_config) if proxy_config else None
        q = 'UPDATE "user" SET manager_url=$1, proxy_config=$2 WHERE id=$3'
        await conn.execute(q, manager_url, proxy_config_str, self.id)
        self.proxy_config = proxy_config
        self.manager_url = manager_url

    async def delete(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        self._delete_from_cache()
        await conn.execute('DELETE FROM "user" WHERE id=$1', self.id)
