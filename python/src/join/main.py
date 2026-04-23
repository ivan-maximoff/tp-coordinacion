import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.global_counts = {}
        self.eofs_received = 0

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        partial_top = message_protocol.internal.deserialize(message)

        for fruit, amount in partial_top:
            self.global_counts[fruit] = self.global_counts.get(fruit, 0) + amount
        self.eofs_received += 1
        logging.info(f"EOFs received: {self.eofs_received}/{AGGREGATION_AMOUNT}")

        if self.eofs_received == AGGREGATION_AMOUNT:
            self._send_final_top()
            self.eofs_received = 0 
            self.global_counts = {}
        ack()


    def _send_final_top(self):
        logging.info("Consolidating final top and sending to Gateway")
        
        final_list = [
            fruit_item.FruitItem(fruit, amount) for fruit, amount in self.global_counts.items()
        ]
        
        final_list.sort(reverse=True)
        top_n = final_list[:TOP_SIZE]
        
        result = [(item.fruit, item.amount) for item in top_n]
        
        self.output_queue.send(message_protocol.internal.serialize(result))
        logging.info("Sent final global top to Gateway")

    def start(self):
        logging.info("JoinFilter: Starting consumption...")
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
