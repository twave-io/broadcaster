import asyncio
import typing
from urllib.parse import urlparse

from nats.aio.client import Client as NATS

from .base import BroadcastBackend
from .._base import Event


class NATSBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        host = parsed_url.hostname or "localhost"
        port = parsed_url.port or 4222
        self._nc = NATS()
        self._url = f'{host}:{port}'
        self._channel_ssids = {}

    async def connect(self) -> None:
        self._listen_queue = asyncio.Queue()
        await self._nc.connect(self._url)

    async def disconnect(self) -> None:
        await self._nc.drain()

    async def subscribe(self, channel: str) -> None:
        ssid = await self._nc.subscribe(channel, cb=self.message_handler)
        self._channel_ssids[channel] = ssid

    async def unsubscribe(self, channel: str) -> None:
        ssid = self._channel_ssids.get(channel)
        if ssid is not None:
            await self._nc.unsubscribe(ssid)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._nc.publish(channel, message.encode())

    async def message_handler(self, message) -> None:
        subject = message.subject
        data = message.data.decode()
        event = Event(channel=subject, message=data)
        self._listen_queue.put_nowait(event)

    async def next_published(self) -> Event:
        return await self._listen_queue.get()
