from __future__ import annotations

from uuid import UUID
import asyncio
import logging

from aioredis import Redis

from mautrix.types import RoomID
from mautrix_asmux.database.table import AppService, Room, User

APPSERVICE_CACHE_CHANNEL = "appservice-cache-invalidation"
ROOM_CACHE_CHANNEL = "room-cache-invalidation"
USER_CACHE_CHANNEL = "user-cache-invalidation"


class RedisCacheHandler:
    log: logging.Logger = logging.getLogger("mau.redis")

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def setup(self):
        self.log.info("Setting up Redis cache invalidation subscriptions")

        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)

        # NOTE: aioredis 2.0.1 doesn't support async callback functons and
        # we cannot currently switch to redis-py (where aioredis has been
        # merged) due to conflicts with aiohttp.
        # See: https://linear.app/beeper/issue/BRI-2811
        await self.pubsub.subscribe(
            APPSERVICE_CACHE_CHANNEL,
            ROOM_CACHE_CHANNEL,
            USER_CACHE_CHANNEL,
        )
        self.channel_handlers = {
            APPSERVICE_CACHE_CHANNEL: self.handle_invalidate_az,
            ROOM_CACHE_CHANNEL: self.handle_invalidate_room,
            USER_CACHE_CHANNEL: self.handle_invalidate_user,
        }

        asyncio.create_task(self.read_pubsub_messages())

    # Listen for and handle invalidation messages

    async def read_pubsub_messages(self):
        message: dict[str, bytes]

        while True:
            try:
                async for message in self.pubsub.listen():
                    channel = message["channel"].decode()
                    handler = self.channel_handlers.get(channel)
                    if handler:
                        await handler(message["data"].decode())
                    else:
                        self.log.warning(f"Unexpected redis pubsub message: {message}")
            except Exception as e:
                self.log.critical(f"Redis failure, throwing caches: {e}")
                AppService.empty_cache()
                Room.empty_cache()
                User.empty_cache()
            await asyncio.sleep(1)

    async def handle_invalidate_az(self, message: str):
        az = await AppService.get(UUID(message))
        if az:
            self.log.debug(f"Invalidating cached appservice: {az.name}")
            az._delete_from_cache()

    async def handle_invalidate_room(self, message: str):
        room = await Room.get(RoomID(message))
        if room:
            self.log.debug(f"Invalidating cached room: {room.id}")
            room._delete_from_cache()

    async def handle_invalidate_user(self, message: str):
        user = await User.get(message)
        if user:
            self.log.debug(f"Invalidating cached user: {user.id}")
            user._delete_from_cache()

    # Publish invalidation messages

    async def invalidate_az(self, az: AppService) -> None:
        self.log.debug(f"Sending invalidate cached AZ: {az.name}")
        await self.redis.publish(APPSERVICE_CACHE_CHANNEL, str(az.id))

    async def invalidate_room(self, room: Room) -> None:
        self.log.debug(f"Sending invalidate cached AZ: {room.id}")
        await self.redis.publish(ROOM_CACHE_CHANNEL, str(room.id))

    async def invalidate_user(self, user: User) -> None:
        self.log.debug(f"Sending invalidate cached AZ: {user.id}")
        await self.redis.publish(USER_CACHE_CHANNEL, user.id)
