import json

from radical.serialization.base import (
    BaseSerializer, ProtocolError, RadicalRequest, RadicalResponse
)


class JSONSerializer(BaseSerializer):
    def encode_request(self, request: RadicalRequest) -> bytes:
        return json.dumps(super().encode_request(request))

    def decode_response(self, data: bytes) -> RadicalResponse:
        return super().decode_response(json.loads(data))

    def decode_request(self, data: bytes) -> RadicalRequest:
        try:
            data = json.loads(data)
        except Exception as error:
            raise ProtocolError(str(error))
        return super().decode_request(data)

    def encode_response(self, data: RadicalResponse) -> bytes:
        return json.dumps(super().encode_response(data))
