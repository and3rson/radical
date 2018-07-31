import asyncio
import importlib

from radical.serialization.base import (
    RadicalRequest, RadicalResponse, ProtocolError, BaseSerializer
)


class Peer(object):
    def __init__(
            self,
            transport_url: str,
            queue_name: str = None,
            transport: str = None,
            serializer: str = None,
            loop=None
    ):
        if queue_name is None:  # pragma: no cover
            queue_name = 'default'
        if transport is None:
            transport = 'radical.transports.redis:RedisTransport'
        if serializer is None:
            serializer = 'radical.serialization.pickle:PickleSerializer'
        if loop is None:  # pragma: no cover
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.transport_url = transport_url
        self.transport = self._create_transport(transport, transport_url, queue_name)
        self.serializer = self._create_serializer(serializer)
        self.queue_name = queue_name

    def _create_transport(self, transport: str, transport_url: str, queue_name: str):
        module_name, _, class_name = transport.rpartition(':')
        transport_module = importlib.import_module(module_name)
        transport_class = getattr(transport_module, class_name)
        return transport_class(transport_url, queue_name, self.loop)

    def _create_serializer(self, serializer: str) -> BaseSerializer:
        module_name, _, class_name = serializer.rpartition(':')
        serializer_module = importlib.import_module(module_name)
        serializer_class = getattr(serializer_module, class_name)
        return serializer_class()

