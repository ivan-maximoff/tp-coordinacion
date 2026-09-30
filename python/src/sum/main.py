import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

internal = message_protocol.internal

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = os.environ.get(
    key="SUM_CONTROL_EXCHANGE",
    default="SUM_CONTROL_EXCHANGE"
)
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
LATE_RECORD_COUNT = 1


def create_aggregation_exchanges():
    exchanges = []
    for i in range(AGGREGATION_AMOUNT):
        routing_key = f"{AGGREGATION_PREFIX}_{i}"
        exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [routing_key]
        )
        exchanges.append(exchange)
    return exchanges


def create_control_exchange(sum_id):
    return middleware.MessageMiddlewareExchangeRabbitMQ(
        MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{sum_id}"]
    )


class SumFilter:
    """
    Solo un Sum recibe el EOF de cada cliente: lo reenvía a todos por control
    y cada uno hace flush informando cuántos registros procesó. Los registros
    que llegan después del flush se envían sueltos como tardíos.
    """

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # Pika no permite compartir conexiones entre hilos: cada hilo publica
        # por las suyas. Datos y EOF van por la misma, así llegan en orden.
        self.data_output_exchanges = create_aggregation_exchanges()
        self.flush_output_exchanges = create_aggregation_exchanges()

        self.control_exchanges = [create_control_exchange(i) for i in range(SUM_AMOUNT)]
        self.control_queue = create_control_exchange(ID)

        self.lock = threading.Lock()
        self.amount_by_fruit = {}
        self.processed_records = {}
        self.finished = {}
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, _signum, _frame):
        logging.info("SIGTERM received. Shutting down SumFilter gracefully...")
        self.input_queue.stop_consuming()
        self.control_queue.stop_consuming_threadsafe()

    def _process_data(self, client_id, fruit, amount):
        logging.debug(f"Process data for ({fruit}, {amount})")
        record = fruit_item.FruitItem(fruit, int(amount))
        with self.lock:
            total = self.finished.get(client_id)
            if total is None:
                client_fruits = self.amount_by_fruit.setdefault(client_id, {})
                client_fruits[fruit] = client_fruits.get(
                    fruit, fruit_item.FruitItem(fruit, 0)
                ) + record
                self.processed_records[client_id] = (
                    self.processed_records.get(client_id, 0) + 1
                )
                return

        logging.info(f"Forwarding late record of client {client_id}")
        self._send_fruits(self.data_output_exchanges, client_id, [record])
        self._send_eof(
            self.data_output_exchanges, client_id, LATE_RECORD_COUNT, total, late=True
        )

    def _get_aggregator_index(self, fruit):
        """
        Asigna cada fruta al mismo agregador siempre, permitiendo el procesamiento distribuido.
        """
        return zlib.crc32(fruit.encode()) % AGGREGATION_AMOUNT

    def _send_fruits(self, exchanges, client_id, fruit_items):
        batches = [[] for _ in range(AGGREGATION_AMOUNT)]
        for item in fruit_items:
            idx = self._get_aggregator_index(item.fruit)
            batches[idx].append([item.fruit, item.amount])

        for exchange, batch in zip(exchanges, batches):
            if batch:
                exchange.send(
                    internal.serialize_message(client_id, internal.MsgType.DATA, batch)
                )

    def _send_eof(self, exchanges, client_id, processed, total, late):
        payload = {
            internal.EofField.PROCESSED: processed,
            internal.EofField.TOTAL: total,
            internal.EofField.LATE: late,
        }
        message = internal.serialize_message(client_id, internal.MsgType.EOF, payload)
        for exchange in exchanges:
            exchange.send(message)

    def _process_eof(self, client_id, total):
        with self.lock:
            client_fruits = self.amount_by_fruit.pop(client_id, {})
            processed = self.processed_records.pop(client_id, 0)
            self.finished[client_id] = total

        logging.info(
            f"Flushing {len(client_fruits)} fruits ({processed}/{total} records) "
            f"of client {client_id} to Aggregators"
        )
        self._send_fruits(self.flush_output_exchanges, client_id, client_fruits.values())
        self._send_eof(
            self.flush_output_exchanges, client_id, processed, total, late=False
        )

    def _listen_control_messages(self):
        """Escucha la cola de control de este Sum y hace el flush del cliente notificado."""
        def on_control_message(msg, ack, nack):
            fields = internal.deserialize(msg)
            self._process_eof(fields[internal.CLIENT_ID], fields[internal.PAYLOAD])
            ack()

        try:
            self.control_queue.start_consuming(on_control_message)
        except Exception as e:
            logging.error(f"Error in control thread: {e}")

    def process_data_messsage(self, message, ack, nack):
        fields = internal.deserialize(message)
        client_id = fields[internal.CLIENT_ID]
        if fields[internal.TYPE] == internal.MsgType.DATA:
            self._process_data(client_id, *fields[internal.PAYLOAD])
        else:
            logging.info("EOF received from Gateway. Notifying peers via control exchange...")
            eof = internal.serialize_message(
                client_id, internal.MsgType.EOF, fields[internal.PAYLOAD]
            )
            for control_exchange in self.control_exchanges:
                control_exchange.send(eof)
        ack()

    def _close(self):
        resources = [
            self.input_queue,
            self.control_queue,
            *self.control_exchanges,
            *self.data_output_exchanges,
            *self.flush_output_exchanges,
        ]
        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing connection: {e}")

    def start(self):
        control_thread = threading.Thread(target=self._listen_control_messages)
        control_thread.start()
        
        logging.info("SumFilter started. Listening for data...")
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.control_queue.stop_consuming_threadsafe()
            control_thread.join()
            self._close()
            logging.info("Graceful shutdown complete.")


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
