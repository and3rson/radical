import pickle

from radical.serialization.base import (
    ProtocolError, BaseSerializer, RadicalRequest, RadicalResponse
)


class PickleSerializer(BaseSerializer):
    def encode_request(self, request: RadicalRequest) -> bytes:
        return pickle.dumps(super().encode_request(request))

    def decode_response(self, data: bytes) -> RadicalResponse:
        return super().decode_response(pickle.loads(data))

    def decode_request(self, data: bytes) -> RadicalRequest:
        try:
            data = pickle.loads(data)
        except Exception as error:
            raise ProtocolError(str(error))
        return super().decode_request(data)

    def encode_response(self, data: RadicalResponse) -> bytes:
        return pickle.dumps(super().encode_response(data))
