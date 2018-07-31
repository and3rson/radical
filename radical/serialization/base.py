from collections import namedtuple


class ProtocolError(Exception):
    pass


RadicalRequest = namedtuple('RadicalRequest', ('signature', 'reply_to'))
Signature = namedtuple('Signature', ('method', 'args', 'kwargs'))
RadicalResponse = namedtuple('RadicalResponse', ('request', 'result', 'error'))
NoRequest = namedtuple('NoRequest', ())


class BaseSerializer(object):
    def encode_request(self, request: RadicalRequest) -> dict:
        return dict(
            method=request.signature.method,
            args=request.signature.args,
            kwargs=request.signature.kwargs,
            reply_to=request.reply_to
        )

    def decode_response(self, data: dict) -> RadicalResponse:
        return RadicalResponse(
            request=None,
            result=data.get('result'),
            error=data.get('error')
        )

    def decode_request(self, data: dict) -> RadicalRequest:
        if not isinstance(data, dict):  # pragma: no cover
            raise ProtocolError('request is not a dictionary.')
        if not isinstance(data.get('method'), str):  # pragma: no cover
            raise ProtocolError('".method" is missing or invalid.')
        if not isinstance(data.get('args', []), (list, tuple)):  # pragma: no cover
            raise ProtocolError('".args" is missing or invalid.')
        if not isinstance(data.get('kwargs', {}), dict):  # pragma: no cover
            raise ProtocolError('".kwargs" is missing or invalid.')
        return RadicalRequest(
            signature=Signature(
                method=data.get('method'),
                args=data.get('args', []),
                kwargs=data.get('kwargs', {})
            ),
            reply_to=data.get('reply_to', '')
        )

    def encode_response(self, response: RadicalResponse) -> dict:
        return dict(
            result=response.result,
            error=response.error
        )
