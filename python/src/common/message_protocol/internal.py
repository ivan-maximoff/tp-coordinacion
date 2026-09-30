import json

CLIENT_ID = "client_id"
TYPE = "type"
PAYLOAD = "payload"


class MsgType:
    DATA = "data"
    EOF = "eof"


class EofField:
    """Payload keys of the EOF a Sum sends to every Aggregation."""

    PROCESSED = "processed"
    TOTAL = "total"
    LATE = "late"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def serialize_message(client_id, msg_type, payload):
    """Serializes an internal message tagged with the client it belongs to."""
    return serialize({CLIENT_ID: client_id, TYPE: msg_type, PAYLOAD: payload})
