import sys
import asyncio
import logging
from unittest import TestCase

import aioredis
import pytest

from radical.worker import Worker
from radical.client import Client
from radical.decorators import method
from radical.exceptions import RadicalException, TimeoutException

pytestmark = pytest.mark.asyncio

logging.basicConfig(format='%(asctime)s %(levelname)8s %(message)s', level=logging.DEBUG)


def create_pair(event_loop, request_timeout=5, **kwargs):
    worker = Worker(f'redis://redis:6379/0', 'test', loop=event_loop, **kwargs)
    client = Client(f'redis://redis:6379/0?request_timeout={request_timeout}', 'test', loop=event_loop, **kwargs)
    worker.register_method(
        'test.add',
        lambda a, b: a + b
    )

    async def wait(delay, result=42):
        return await asyncio.sleep(delay, result=result)

    worker.register_method(
        'test.wait',
        wait
    )

    return worker, client


async def test_call(event_loop):
    worker, client = create_pair(event_loop)
    await worker.start()
    await client.start()
    assert (await client.call('test', 'test.add', 1300, 37)) is None
    await worker.stop()
    await client.stop()


async def test_call_wait(event_loop):
    worker, client = create_pair(event_loop)
    await worker.start()
    await client.start()
    assert (await client.call_wait('test', 'test.add', 1300, 37)) == 1337
    await worker.stop()
    await client.stop()


async def test_call_wait_async(event_loop):
    worker, client = create_pair(event_loop)
    await worker.start()
    await client.start()
    assert (await client.call_wait('test', 'test.wait', 1, result=42)) == 42
    await worker.stop()
    await client.stop()


async def test_call_wait_error(event_loop):
    worker, client = create_pair(event_loop)
    await worker.start()
    await client.start()
    raised = False
    try:
        await client.call_wait('test', 'test.add', 1, '2')
    except RadicalException:
        raised = True
    assert raised, 'RadicalException was not raised.'
    await worker.stop()
    await client.stop()


async def test_json_serializer(event_loop):
    worker, client = create_pair(
        event_loop,
        serializer='radical.serialization.json:JSONSerializer'
    )
    await worker.start()
    await client.start()
    assert (await client.call_wait('test', 'test.add', 1300, 37)) == 1337
    await worker.stop()
    await client.stop()


async def test_bad_request(event_loop):
    worker, _ = create_pair(event_loop)
    await worker.start()
    conn = await aioredis.create_redis('redis://redis:6379/0')
    await conn.execute('lpush', 'radical:test', 'dafuq!')
    conn.close()
    await conn.wait_closed()
    # assert (await client.call_wait('test', 'test.add', 1300, 37)) == 1337
    await worker.stop()


async def test_bad_request_json(event_loop):
    worker, _ = create_pair(
        event_loop,
        serializer='radical.serialization.json:JSONSerializer'
    )
    await worker.start()
    conn = await aioredis.create_redis('redis://redis:6379/0')
    await conn.execute('lpush', 'radical:test', 'dafuq!')
    conn.close()
    await conn.wait_closed()
    # assert (await client.call_wait('test', 'test.add', 1300, 37)) == 1337
    await worker.stop()


async def test_timeout(event_loop):
    worker, client = create_pair(event_loop, request_timeout=2)
    await worker.start()
    await client.start()
    try:
        assert (await client.call_wait('unexistent', 'foo.bar'))
    except TimeoutException:
        raised = True
    assert raised, 'TimeoutException was not raised.'
    await worker.stop()
    await client.stop()


async def test_inspect(event_loop):
    worker, client = create_pair(event_loop, request_timeout=2)
    await worker.start()
    await client.start()
    actual = await client.call_wait('test', '_inspect')
    expected = ['test.add', 'test.wait', '_inspect']
    assert set(actual) == set(expected)
    await worker.stop()
    await client.stop()


async def test_discovery(event_loop):
    class RadicalFakePackage(object):
        class FakeModule(object):
            @method
            def foo(self):  # pragma: no cover
                pass

            def bar(self):  # pragma: no cover
                pass

    try:
        sys.modules['RadicalFakePackage'] = RadicalFakePackage
        sys.modules['RadicalFakePackage.FakeModule'] = RadicalFakePackage.FakeModule
        worker, _ = create_pair(event_loop)
        worker.discover([
            'RadicalFakePackage.FakeModule'
        ])
        assert 'RadicalFakePackage.FakeModule.foo' in worker.methods
        assert 'RadicalFakePackage.FakeModule.bar' not in worker.methods
    finally:
        del sys.modules['RadicalFakePackage']
        del sys.modules['RadicalFakePackage.FakeModule']
