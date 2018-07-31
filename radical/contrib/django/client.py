import asyncio

from django.conf import settings

from radical.client import Client


def _create_session():
    loop = asyncio.new_event_loop()
    client = Client(
        settings.RADICAL_CONFIG['TRANSPORT_URL'],
        queue_name=settings.RADICAL_CONFIG.get('QUEUE_NAME'),
        transport=settings.RADICAL_CONFIG.get('TRANSPORT'),
        serializer=settings.RADICAL_CONFIG.get('SERIALIZER'),
        loop=loop
    )
    return loop, client


def _destroy_session(client):
    client.loop.run_until_complete(client.stop())
    client.loop.stop()
    client.loop.close()


def call(queue_name, method, *args, **kwargs):
    loop, client = _create_session()
    try:
        loop.run_until_complete(client.start())
        coroutine = client.call(queue_name, method, *args, **kwargs)
        loop.run_until_complete(coroutine)
    finally:
        _destroy_session(client)


def call_wait(queue_name, method, *args, **kwargs):
    loop, client = _create_session()
    try:
        loop.run_until_complete(client.start())
        coroutine = client.call_wait(queue_name, method, *args, **kwargs)
        return loop.run_until_complete(coroutine)
    finally:
        _destroy_session(client)
