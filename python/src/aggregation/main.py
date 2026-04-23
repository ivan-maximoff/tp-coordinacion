import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = []
        self.sums_finished = 0
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, _signum, _frame):
        logging.info("SIGTERM received. Shutting down AggregationFilter...")
        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        self.output_queue.close()

    def _process_data(self, fruit, amount):
        logging.debug(f"Updating count for {fruit}")
        for i in range(len(self.fruit_top)):
            if self.fruit_top[i].fruit == fruit:
                self.fruit_top[i] = self.fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                return
        bisect.insort(self.fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self):
        self.sums_finished += 1
        logging.info(f"Received EOF from Sum ({self.sums_finished}/{SUM_AMOUNT})")
        
        if self.sums_finished == SUM_AMOUNT:
            logging.info("All Sums finished. Sending partial top to Joiner.")
            fruit_chunk = list(self.fruit_top[-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            self.output_queue.send(message_protocol.internal.serialize(fruit_top))
            self.fruit_top = []
            self.sums_finished = 0

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        logging.debug(f"Received fields: {fields}")
        if len(fields) == 2:
            self._process_data(*fields)
        else:
            self._process_eof()
        ack()

    def start(self):
        logging.info("AggregationFilter: Starting consumption...")
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
