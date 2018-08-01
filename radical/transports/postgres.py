from typing import Awaitable

import asyncio
from urllib.parse import urlparse, parse_qsl
from hashlib import md5
from struct import unpack

import aiopg

from radical.log import logger
from radical import exceptions

QUEUE_PREFIX = 'radical_'


class PostgresTransport(object):
    def __init__(self, transport_url, queue_name, loop):
        self.transport_url = transport_url
        self.queue_name = queue_name
        self.loop = loop
        self.conn = None
        self.lock_id = self._calculate_lock_id()
        self.urlinfo = urlparse(transport_url)
        config = dict(parse_qsl(self.urlinfo.query))
        self.request_timeout = int(config.get('request_timeout', 10))
        self.closed = asyncio.Event()

        self.request_table = QUEUE_PREFIX + self.queue_name

    def _calculate_lock_id(self):
        hash_digest = md5(self.queue_name.encode('utf-8')).digest()[:8]
        return unpack('l', hash_digest)[0]

    async def start(self):
        self.closed.clear()
        self.pool = await aiopg.create_pool(
            host=self.urlinfo.hostname,
            port=self.urlinfo.port or 5432,
            database=self.urlinfo.path[1:],
            user=self.urlinfo.username,
            password=self.urlinfo.password
        )
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await self._lock(cursor)
                await cursor.execute('SELECT COUNT(*) FROM pg_tables WHERE tablename = %s', [self.request_table])
                count, = await cursor.fetchone()
                if not count:
                    logger.debug('Creating queue table.')
                    await cursor.execute(f'CREATE TABLE {self.request_table}(id serial, data bytea)')
                await self._unlock(cursor)
        logger.debug('Postgres transport started')

    async def _lock(self, cursor):
        await cursor.execute(f'SELECT pg_advisory_lock({self.lock_id})')

    async def _unlock(self, cursor):
        await cursor.execute(f'SELECT pg_advisory_unlock({self.lock_id})')

    async def stop(self):
        self.pool.close()

    async def get_next_request(self):
        result = None
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await self._lock(cursor)
                    await cursor.execute(f'SELECT * FROM {self.request_table} LIMIT 1')
                    row = await cursor.fetchone()
                    if row:
                        logger.debug('Received new request in queue table.')
                        id_, data = row
                        await cursor.execute(f'DELETE FROM {self.request_table} WHERE id = {id_}')
                        return bytes(data)
                finally:
                    await self._unlock(cursor)
        await asyncio.sleep(1)
        return result

    async def reply_to(self, request_id, message):
        source_name = QUEUE_PREFIX + request_id
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                logger.debug(f'NOTIFY {source_name}')
                await cursor.execute(f'NOTIFY {source_name}, %s', [message])

    async def send_to(self, queue_name, message):
        source_name = QUEUE_PREFIX + queue_name
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                logger.debug(f'Sending request to {queue_name}')
                try:
                    await self._lock(cursor)
                    await cursor.execute(f'INSERT INTO {source_name}(data) VALUES(%s)', [message])
                finally:
                    await self._unlock(cursor)

    async def get_response(self, request_id) -> Awaitable:
        channel_name = QUEUE_PREFIX + request_id
        async def get_message():
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    logger.debug(f'Waiting for response to {request_id}')
                    try:
                        await cursor.execute(f'LISTEN {channel_name}')
                        msg = await asyncio.wait_for(conn.notifies.get(), self.request_timeout)
                        return msg.payload
                    except asyncio.TimeoutError:
                        raise exceptions.TimeoutException('Timeout while waiting for response.')
                    finally:
                        await cursor.execute(f'UNLISTEN {channel_name}')
        return get_message()
