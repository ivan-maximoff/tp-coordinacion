import uuid

from common import message_protocol

internal = message_protocol.internal


class MessageHandler:

    def __init__(self):
        # The gateway copies this handler to other processes, so the id is set
        # here to make every copy share it.
        self.client_id = str(uuid.uuid4())
        self.records_sent = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.records_sent += 1
        return internal.serialize_message(
            self.client_id, internal.MsgType.DATA, [fruit, amount]
        )

    def serialize_eof_message(self, message):
        return internal.serialize_message(
            self.client_id, internal.MsgType.EOF, self.records_sent
        )

    def deserialize_result_message(self, message):
        fields = internal.deserialize(message)
        if fields[internal.CLIENT_ID] != self.client_id:
            return None
        return fields[internal.PAYLOAD]
