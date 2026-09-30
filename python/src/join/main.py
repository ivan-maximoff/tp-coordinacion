import os
import logging
import heapq
import signal

from common import middleware, message_protocol, fruit_item

internal = message_protocol.internal

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # Los Sum reparten las frutas por hash, así que los tops parciales nunca
        # comparten una fruta y el top global es simplemente el mejor entre todos.
        self.candidates_by_client = {}
        self.partial_tops_by_client = {}
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, _signum, _frame):
        logging.info("SIGTERM received. Shutting down JoinFilter...")
        self.input_queue.stop_consuming()

    def process_messsage(self, message, ack, nack):
        fields = internal.deserialize(message)
        client_id = fields[internal.CLIENT_ID]
        candidates = self.candidates_by_client.setdefault(client_id, [])
        candidates.extend(
            fruit_item.FruitItem(fruit, amount)
            for fruit, amount in fields[internal.PAYLOAD]
        )
        received = self.partial_tops_by_client.get(client_id, 0) + 1
        self.partial_tops_by_client[client_id] = received
        logging.info(
            f"Partial tops of client {client_id}: {received}/{AGGREGATION_AMOUNT}"
        )

        if received == AGGREGATION_AMOUNT:
            self._send_final_top(client_id)
        ack()

    def _send_final_top(self, client_id):
        candidates = self.candidates_by_client.pop(client_id)
        del self.partial_tops_by_client[client_id]
        top = heapq.nlargest(TOP_SIZE, candidates)
        result = [[item.fruit, item.amount] for item in top]
        self.output_queue.send(
            internal.serialize_message(client_id, internal.MsgType.DATA, result)
        )
        logging.info(f"Sent final top of client {client_id} to Gateway")

    def start(self):
        logging.info("JoinFilter: Starting consumption...")
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.input_queue.close()
            self.output_queue.close()
            logging.info("Graceful shutdown complete.")


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
