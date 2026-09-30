import os
import logging
import heapq
import signal

from common import middleware, message_protocol, fruit_item

internal = message_protocol.internal

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class ClientAggregation:
    """
    Estado parcial de un cliente. Está completo cuando todos los Sum hicieron
    flush y los registros que informan como procesados suman el total que envió
    el cliente: pueden llegar registros tardíos después del último flush.
    """

    def __init__(self):
        self.amount_by_fruit = {}
        self.processed = 0
        self.flushes = 0

    def add_fruit(self, fruit, amount):
        record = fruit_item.FruitItem(fruit, amount)
        self.amount_by_fruit[fruit] = (
            self.amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0)) + record
        )

    def register_eof(self, eof):
        self.processed += eof[internal.EofField.PROCESSED]
        if not eof[internal.EofField.LATE]:
            self.flushes += 1

    def is_complete(self, total):
        return self.flushes == SUM_AMOUNT and self.processed == total

    def top(self):
        return heapq.nlargest(TOP_SIZE, self.amount_by_fruit.values())


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.aggregation_by_client = {}
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, _signum, _frame):
        logging.info("SIGTERM received. Shutting down AggregationFilter...")
        self.input_exchange.stop_consuming()

    def _process_data(self, aggregation, fruits):
        for fruit, amount in fruits:
            aggregation.add_fruit(fruit, amount)

    def _process_eof(self, client_id, aggregation, eof):
        aggregation.register_eof(eof)
        total = eof[internal.EofField.TOTAL]
        logging.info(
            f"EOF of client {client_id}: {aggregation.flushes}/{SUM_AMOUNT} Sums, "
            f"{aggregation.processed}/{total} records"
        )
        if not aggregation.is_complete(total):
            return

        logging.info(f"Client {client_id} complete. Sending partial top to Joiner.")
        fruit_top = [[item.fruit, item.amount] for item in aggregation.top()]
        self.output_queue.send(
            internal.serialize_message(client_id, internal.MsgType.DATA, fruit_top)
        )
        del self.aggregation_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        fields = internal.deserialize(message)
        client_id = fields[internal.CLIENT_ID]
        aggregation = self.aggregation_by_client.setdefault(
            client_id, ClientAggregation()
        )
        if fields[internal.TYPE] == internal.MsgType.DATA:
            self._process_data(aggregation, fields[internal.PAYLOAD])
        else:
            self._process_eof(client_id, aggregation, fields[internal.PAYLOAD])
        ack()

    def start(self):
        logging.info("AggregationFilter: Starting consumption...")
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.input_exchange.close()
            self.output_queue.close()
            logging.info("Graceful shutdown complete.")


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
