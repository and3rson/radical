import sys
sys.path.append('.')

from typing import Union, Callable, Optional, NoReturn, Awaitable, List
import logging
import asyncio
import importlib
import signal
import traceback
import argparse
from urllib.parse import urlparse

from radical.peer import Peer
from radical.serialization.base import (
    RadicalRequest, RadicalResponse, NoRequest,
    ProtocolError, BaseSerializer
)
from radical.log import logger
from radical import meta


class Worker(Peer):
    def __init__(
            self,
            transport_url: str,
            queue_name: str = None,
            transport: str = None,
            serializer: str = None,
            loop=None
    ):
        super().__init__(transport_url, queue_name, transport, serializer, loop)
        self.methods = {
            '_inspect': lambda: list(self.methods.keys())
        }
        self.futures = []
        self.terminated = True
        self._main = None

    def discover(self, arg: Union[list, tuple, str]) -> List[str]:
        methods = []
        if isinstance(arg, (list, tuple)):
            for module_name in arg:
                methods.extend(self.discover(module_name))
        else:
            module_name = arg
            module_obj = importlib.import_module(module_name)
            for key, value in vars(module_obj).items():
                if hasattr(value, '__radical__'):
                    cannonical_name = '.'.join([module_name, key])
                    self.register_method(cannonical_name, value)
                    methods.append(cannonical_name)
        return methods

    def register_method(self, cannonical_name: str, method: Callable) -> NoReturn:
        assert cannonical_name not in self.methods, \
            f'Cannonical name {cannonical_name} already registered.'
        self.methods[cannonical_name] = method

    async def start(self) -> Awaitable:
        urlinfo = urlparse(self.transport_url)
        sys.stderr.write(meta.FLAIR % (
            f'Version   : {meta.VERSION_COLOR}{meta.VERSION}{meta.R}',
            f'URL       : {meta.PROP_COLOR}{urlinfo.scheme}://***@{urlinfo.hostname}{meta.R}',
            f'Queue     : {meta.PROP_COLOR}{self.queue_name}{meta.R}',
            f'Transport : {meta.PROP_COLOR}{self.transport.__class__.__name__}{meta.R}',
            f'Serializer: {meta.PROP_COLOR}{self.serializer.__class__.__name__}{meta.R}',
        ))
        sys.stderr.write(f'\n')
        if len(self.methods):
            sys.stderr.write('Offering the following methods:\n\n')
            for method in self.methods.keys():
                sys.stderr.write(f'{meta.SVC_COLOR}  - {method}\n{meta.R}')
        else:
            sys.stderr.write(f'{meta.ERROR_COLOR}  ! No methods found.\n{meta.R}')
        sys.stderr.write('\n')

        await self.transport.start()
        self._main = asyncio.ensure_future(self._run())
        return self._main

    async def _run(self) -> NoReturn:
        logger.info('Radical RPC server is ready.')
        self.terminated = False
        self.futures.append(asyncio.ensure_future(self._accept(), loop=self.loop))
        while len(self.futures):
            done, running = await asyncio.wait(
                self.futures,
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop
            )
            while done:
                future = done.pop()
                self.futures.remove(future)
                obj = future.result()
                if isinstance(obj, RadicalRequest):
                    if not self.terminated:
                        self.futures.append(asyncio.ensure_future(self._accept(), loop=self.loop))
                    self.futures.append(
                        asyncio.ensure_future(self._process_request(obj), loop=self.loop)
                    )
                elif isinstance(obj, RadicalResponse):
                    self.futures.append(asyncio.ensure_future(self._process_response(obj), loop=self.loop))
                elif isinstance(obj, NoRequest):
                    if not self.terminated:
                        self.futures.append(asyncio.ensure_future(self._accept(), loop=self.loop))
        logger.info('Radical RPC server is terminating gracefully.')
        await self.transport.stop()

    async def _accept(self) -> RadicalRequest:
        request = await self.transport.get_next_request()
        if request is not None:
            try:
                radical_request = self.serializer.decode_request(request)
            except ProtocolError as error:
                logger.error(f'Deserialization failed: {error}')
            else:
                return radical_request
        return NoRequest()

    async def _process_request(self, radical_request: RadicalRequest) -> Optional[RadicalResponse]:
        logger.info(f'Received request {radical_request}')
        result, error = None, None
        try:
            signature = radical_request.signature
            method = self.methods[signature.method]
            if asyncio.iscoroutinefunction(method):
                result = await method(*signature.args, **signature.kwargs)
            else:
                result = method(*signature.args, **signature.kwargs)
        except Exception as method_error:
            # TODO: Include traceback
            error = str(method_error)
        if error:
            logger.error(f'WARNING: Error occured: {error}')
        return RadicalResponse(
            request=radical_request, result=result, error=error
        )

    async def _process_response(self, radical_response: RadicalResponse) -> NoReturn:
        if radical_response.request.reply_to:
            logger.info(f'Sending response {radical_response}')
            data = self.serializer.encode_response(radical_response)
            await self.transport.reply_to(
                radical_response.request.reply_to,
                data
            )
        else:
            logger.info(f'Discarding response {radical_response}')

    def _exception_handler(self, loop, data) -> NoReturn:
        logger.error(f'Exception in loop {loop}: {data}')

    def run_until_complete(self) -> NoReturn:
        self.loop.set_exception_handler(self._exception_handler)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.loop.add_signal_handler(
                sig,
                lambda: asyncio.ensure_future(self.stop(), loop=self.loop)
            )
        try:
            coro = self.loop.run_until_complete(self.start())
            self.loop.run_until_complete(coro)
        except Exception as error:
            logger.exception(str(error))
            asyncio.ensure_future(self.stop(), loop=self.loop)
            self.loop.stop()

    async def stop(self):
        logger.info('Attempting graceful shutdown.')
        logger.info('Do NOT kill the process right now or you will lose data!')
        self.terminated = True
        logger.info('Waiting for all tasks to complete...')
        await self._main


def main():  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', default='redis://127.0.0.1:6379/0', dest='transport_url')
    parser.add_argument('-q', '--queue', default='default', dest='queue_name')
    parser.add_argument('-t', '--transport', default='radical.transports.redis:RedisTransport')
    parser.add_argument('-s', '--serializer', default='radical.serialization.pickle:PickleSerializer')
    parser.add_argument('-l', '--level', default='INFO', help='Logging level.')
    parser.add_argument('module', nargs='+', help='Module with methods. Can be specified multiple times.')
    args = dict(vars(parser.parse_args()))
    modules = args.pop('module')
    level = getattr(logging, args.pop('level').upper())
    logging.basicConfig(level=level, format='%(asctime)s [%(levelname)-8s] (%(module)s) %(message)s')
    worker = Worker(**args)
    methods = worker.discover(modules)
    worker.run_until_complete()


if __name__ == '__main__':  # pragma: no cover
    main()
