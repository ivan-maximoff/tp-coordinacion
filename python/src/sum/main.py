import os
import logging
import signal
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

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
CONTROL_EOF_MARKER = "__CONTROL_EOF__"

class SumFilter:
    def __init__(self):
        self.finished = False
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            routing_key = f"{AGGREGATION_PREFIX}_{i}"
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [routing_key]
            )
            self.data_output_exchanges.append(exchange)
            
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, []
        )
        
        self.amount_by_fruit = {}
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, _signum, _frame):
        logging.info("SIGTERM received. Shutting down SumFilter gracefully...")
        self.input_queue.stop_consuming()
        self.input_queue.close()
        for ex in self.data_output_exchanges:
            ex.close()
        self.control_exchange.close()
        logging.info("Connections closed. Exiting.")
        logging.info("Graceful shutdown complete.")

    def _process_data(self, fruit, amount):
        logging.debug(f"Process data for ({fruit}, {amount})")
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _get_aggregator_index(self, fruit):
        """
        Asigna cada fruta al mismo agregador siempre, permitiendo el procesamiento distribuido.
        String -> Bytes -> MD5 Hash -> Hex -> Integer -> Index.
        """
        fruit_bytes = fruit.encode()
        md5_hash = hashlib.md5(fruit_bytes).hexdigest()
        hash_as_int = int(md5_hash, 16)
        return hash_as_int % AGGREGATION_AMOUNT

    def _process_eof(self):
        if self.finished:
            return
        self.finished = True

        logging.info(f"Flushing {len(self.amount_by_fruit)} fruits to Aggregators")
        for item in self.amount_by_fruit.values():
            idx = self._get_aggregator_index(item.fruit)
            self.data_output_exchanges[idx].send(
                message_protocol.internal.serialize([item.fruit, item.amount])
            )

        logging.info(f"Broadcasting EOF message to all Aggregators")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([]))

    def _listen_control_messages(self):
        """Listen on control exchange and signal the main consumer thread."""
        try:
            local_control_middleware = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, []
            )
            local_signal_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

            def on_control_message(msg, ack, nack):
                local_signal_queue.send(
                    message_protocol.internal.serialize([CONTROL_EOF_MARKER])
                )
                ack()

            local_control_middleware.start_consuming(
                on_control_message
            )
        except Exception as e:
            logging.error(f"Error in control thread: {e}")

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields == [CONTROL_EOF_MARKER]:
            logging.info("EOF received from control exchange")
            self._process_eof()
        elif len(fields) == 2:
            self._process_data(*fields)
        else:
            logging.info("EOF received from Gateway. Notifying peers via control exchange...")
            self.control_exchange.send(message_protocol.internal.serialize([]))
            self._process_eof()
        ack()

    def start(self):
        control_thread = threading.Thread(target=self._listen_control_messages)
        control_thread.daemon = True
        control_thread.start()
        
        logging.info("SumFilter started. Listening for data...")
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
