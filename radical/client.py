import uuid

from radical.peer import Peer
from radical.serialization.base import RadicalRequest, Signature
from radical.log import logger
from radical.exceptions import RadicalException


class Client(Peer):
    def __init__(
            self,
            transport_url: str,
            queue_name: str = None,
            transport: str = None,
            serializer: str = None,
            loop=None
    ):
        super().__init__(
            transport_url, queue_name, transport, serializer, loop
        )

    async def call(self, queue_name, method, *args, **kwargs):
        logger.debug('Calling %s from %s (nowait mode)', method, queue_name)
        radical_request = RadicalRequest(signature=Signature(
            method=method,
            args=args,
            kwargs=kwargs
        ), reply_to=None)
        data = self.serializer.encode_request(radical_request)
        await self.transport.send_to(queue_name, data)

    async def call_wait(self, queue_name, method, *args, **kwargs):
        logger.debug('Calling %s from %s (wait mode)', method, queue_name)
        message_id = uuid.uuid1().hex
        response_future = await self.transport.get_response(message_id)
        radical_request = RadicalRequest(signature=Signature(
            method=method,
            args=args,
            kwargs=kwargs
        ), reply_to=message_id)
        data = self.serializer.encode_request(radical_request)
        await self.transport.send_to(queue_name, data)
        response = await response_future
        # TODO: Add timeout
        radical_response = self.serializer.decode_response(response)
        if radical_response.error:
            raise RadicalException(radical_response.error)
        return radical_response.result

    async def start(self):
        await self.transport.start()

    async def stop(self):
        await self.transport.stop()
