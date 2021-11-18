# mautrix-asmux - A Matrix application service proxy and multiplexer
# Copyright (C) 2021 Beeper, Inc. All rights reserved.
from typing import Optional, List, Iterable, Dict, Tuple, ClassVar
from uuid import UUID, uuid4
import secrets
import hashlib
import base64
import time
import hmac
import math

from attr import dataclass
import asyncpg

from ...sygnal import PushKey
from .base import Base
from .user import User


@dataclass
class AppService(Base):
    id: UUID
    owner: str
    prefix: str

    bot: str
    address: str
    hs_token: str
    as_token: str
    push: bool
    config_password_hash: Optional[bytes] = None
    config_password_expiry: Optional[int] = None
    push_key: Optional[PushKey] = None

    login_token: Optional[str] = None
    created_: bool = False

    cache_by_id: ClassVar[Dict[UUID, 'AppService']] = {}
    cache_by_owner: ClassVar[Dict[Tuple[str, str], 'AppService']] = {}

    def __attrs_post_init__(self) -> None:
        if self.push_key and isinstance(self.push_key, str):
            self.push_key = PushKey.parse_json(self.push_key)

    @property
    def name(self) -> str:
        return f"{self.owner}/{self.prefix}"

    @property
    def real_as_token(self) -> str:
        return f"{self.id}-{self.as_token}"

    def _add_to_cache(self) -> 'AppService':
        self.cache_by_id[self.id] = self
        self.cache_by_owner[(self.owner, self.prefix)] = self
        return self

    def _delete_from_cache(self) -> None:
        del self.cache_by_id[self.id]
        del self.cache_by_owner[(self.owner, self.prefix)]

    @classmethod
    async def get(cls, id: UUID, *, conn: Optional[asyncpg.Connection] = None
                  ) -> Optional['AppService']:
        try:
            return cls.cache_by_id[id]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT appservice.id, owner, prefix, bot, address, '
                                  '       hs_token, as_token, push, "user".login_token, '
                                  '       config_password_hash, config_password_expiry '
                                  'FROM appservice JOIN "user" ON "user".id=appservice.owner '
                                  'WHERE appservice.id=$1::uuid', id)
        return AppService(**row)._add_to_cache() if row else None

    @classmethod
    async def find(cls, owner: str, prefix: str, *, conn: Optional[asyncpg.Connection] = None
                   ) -> Optional['AppService']:
        try:
            return cls.cache_by_owner[(owner, prefix)]
        except KeyError:
            pass
        conn = conn or cls.db
        row = await conn.fetchrow('SELECT appservice.id, owner, prefix, bot, address, hs_token, '
                                  '       as_token, push, "user".login_token, '
                                  '       config_password_hash, config_password_expiry '
                                  'FROM appservice JOIN "user" ON "user".id=appservice.owner '
                                  'WHERE owner=$1 AND prefix=$2 ',
                                  owner, prefix)
        return AppService(**row)._add_to_cache() if row else None

    @classmethod
    async def find_or_create(cls, user: User, prefix: str, *, bot: str = "bot", address: str = "",
                             push: bool = True) -> 'AppService':
        try:
            return cls.cache_by_owner[(user.id, prefix)]
        except KeyError:
            pass
        async with cls.db.acquire() as conn, conn.transaction():
            az = await cls.find(user.id, prefix, conn=conn)
            if not az:
                uuid = uuid4()
                hs_token = secrets.token_urlsafe(48)
                # The input AS token also contains the UUID, so we want this to be a bit shorter
                as_token = secrets.token_urlsafe(20)
                az = AppService(id=uuid, owner=user.id, prefix=prefix, bot=bot, address=address,
                                hs_token=hs_token, as_token=as_token, push=push, push_key=None,
                                login_token=user.login_token)
                az.created_ = True
                await az.insert(conn=conn)
            return az

    @classmethod
    async def get_many(cls, ids: List[UUID], *, conn: Optional[asyncpg.Connection] = None
                       ) -> Iterable['AppService']:
        conn = conn or cls.db
        rows = await conn.fetch('SELECT appservice.id, owner, prefix, bot, address, hs_token,'
                                '       as_token, push, "user".login_token, '
                                '       config_password_hash, config_password_expiry, push_key '
                                'FROM appservice JOIN "user" ON "user".id=appservice.owner '
                                'WHERE appservice.id = ANY($1::uuid[])', ids)
        return (AppService(**row)._add_to_cache() for row in rows)

    async def insert(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        await conn.execute("INSERT INTO appservice "
                           "(id, owner, prefix, bot, address, hs_token, as_token, push) "
                           "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                           self.id, self.owner, self.prefix, self.bot, self.address,
                           self.hs_token, self.as_token, self.push)
        self._add_to_cache()

    async def set_address(self, address: str, *,
                          conn: Optional[asyncpg.Connection] = None) -> None:
        if address is None or self.address == address:
            return
        self.address = address
        conn = conn or self.db
        await conn.execute("UPDATE appservice SET address=$2 WHERE id=$1", self.id, self.address)

    async def set_push(self, push: bool) -> None:
        if push is None or push == self.push:
            return
        self.push = push
        await self.db.execute("UPDATE appservice SET push=$2 WHERE id=$1", self.id, self.push)

    async def generate_password(self, lifetime: Optional[int] = None) -> str:
        token = secrets.token_bytes()
        self.config_password_hash = hashlib.sha256(token).digest()
        self.config_password_expiry = None if lifetime is None else (int(time.time()) + lifetime)
        await self.db.execute("UPDATE appservice SET config_password_hash=$2, "
                              "                      config_password_expiry=$3 "
                              "WHERE id=$1",
                              self.id, self.config_password_hash, self.config_password_expiry)
        # We use case-insensitive base32 instead of base64 due to
        # https://gitlab.com/beeper/brooklyn/-/issues/7
        return base64.b32encode(token).decode("utf-8").rstrip("=")

    def check_password(self, password: str) -> bool:
        pad_length = math.ceil(len(password) / 8) * 8 - len(password)
        padded_password = password.upper() + "=" * pad_length
        hashed_password = hashlib.sha256(base64.b32decode(padded_password)).digest()
        correct = hmac.compare_digest(hashed_password, self.config_password_hash)
        expired = (self.config_password_expiry < int(time.time())
                   if self.config_password_expiry is not None
                   else False)
        return correct and not expired

    async def set_push_key(self, push_key: Optional[PushKey]) -> None:
        self.push_key = push_key
        await self.db.execute("UPDATE appservice SET push_key=$2 WHERE id=$1",
                              self.id, self.push_key.serialize() if self.push_key else None)

    async def delete(self, *, conn: Optional[asyncpg.Connection] = None) -> None:
        conn = conn or self.db
        self._delete_from_cache()
        await conn.execute("DELETE FROM appservice WHERE id=$1", self.id)
