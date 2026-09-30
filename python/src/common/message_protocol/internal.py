import json

CLIENT_ID = "client_id"
TYPE = "type"
PAYLOAD = "payload"


class MsgType:
    DATA = "data"
    EOF = "eof"


class EofField:
    """Claves del payload del EOF que cada Sum envía a todos los Aggregation."""

    PROCESSED = "processed"
    TOTAL = "total"
    LATE = "late"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def serialize_message(client_id, msg_type, payload):
    """Serializa un mensaje interno etiquetado con el cliente al que pertenece."""
    return serialize({CLIENT_ID: client_id, TYPE: msg_type, PAYLOAD: payload})
