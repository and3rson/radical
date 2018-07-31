from typing import Awaitable

import asyncio
from urllib.parse import urlparse, parse_qsl

import aioredis
from aioredis.pubsub import Receiver
from aioredis.abc import AbcChannel

from radical.log import logger
from radical import exceptions

QUEUE_PREFIX = 'radical:'


class RedisTransport(object):
    def __init__(self, transport_url, queue_name, loop):
        self.transport_url = transport_url
        self.queue_name = queue_name
        self.loop = loop
        self.conn = None
        config = dict(parse_qsl(urlparse(transport_url).query))
        self.request_timeout = int(config.get('request_timeout', 10))

    async def start(self):
        self.pool = await aioredis.create_redis_pool(
            self.transport_url,
            loop=self.loop,
            minsize=4  # At lest 2: one for BLPOP, other for PUBLISH
        )
        logger.debug('Redis transport started')

    async def stop(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def get_next_request(self):
        source_name = QUEUE_PREFIX + self.queue_name
        try:
            result = await self.pool.execute(
                'blpop',
                source_name,
                1
            )
            if result is not None:
                logger.debug('BLPOP %s', source_name)
                return result[1]
        except Exception as error:
            logger.error(f'ERROR: {repr(error)}, retrying in 1 second')
            await asyncio.sleep(1)

    async def reply_to(self, request_id, message):
        source_name = QUEUE_PREFIX + request_id
        logger.debug('PUBLISH %s', source_name)
        await self.pool.execute(
            'publish',
            source_name,
            message
        )

    async def send_to(self, queue_name, message):
        source_name = QUEUE_PREFIX + queue_name
        logger.debug('LPUSH %s', source_name)
        await self.pool.execute('lpush', source_name, message)

    async def get_response(self, request_id) -> Awaitable:
        channel_name = QUEUE_PREFIX + request_id
        logger.debug('SUBSCRIBE %s', channel_name)
        channel = (await self.pool.subscribe(channel_name))[0]
        async def get_message():
            wait_coro = asyncio.ensure_future(channel.wait_message())
            timeout_coro = asyncio.ensure_future(asyncio.sleep(self.request_timeout))
            done = set()
            try:
                done, running = await asyncio.wait([
                    wait_coro, timeout_coro
                ], return_when=asyncio.FIRST_COMPLETED)
            except Exception as error:  # pragma: no cover
                logger.error('Error while waiting for message:', error)
                raise exceptions.RadicalException(f'Error while waiting for message: {str(error)}')
            else:
                if timeout_coro in done:
                    raise exceptions.TimeoutException('Timeout while waiting for response.')
                message = await channel.get()
                return message
            finally:
                timeout_coro.cancel()
                try:
                    await self.pool.unsubscribe(channel_name)
                except Exception as error:  # pragma: no cover
                    raise exceptions.TransportException(str(error))
                channel.close()
        return get_message()
