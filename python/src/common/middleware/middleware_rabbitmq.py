import logging
import pika
from pika.exceptions import AMQPConnectionError, ConnectionClosedByBroker
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class _RabbitMQMiddleware:
    def __init__(self, host):
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
        except AMQPConnectionError:
            logging.error(f"Error conectando a RabbitMQ en {host}")
            raise

    def close(self):
        try:
            if self._connection.is_open:
                self._connection.close()
        except Exception:
            raise MessageMiddlewareCloseError()

    def _publish(self, exchange, routing_key, message):
        try:
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message
            )
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, _, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            try:
                on_message_callback(body, ack, nack)
            except Exception:
                logging.exception("Unhandled error in message callback")
                nack()

        try:
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=internal_callback
            )
            self._channel.start_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except ConnectionClosedByBroker:
            logging.info("Connection closed by broker while consuming")

    def stop_consuming(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except Exception:
            pass

class MessageMiddlewareQueueRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        _RabbitMQMiddleware.__init__(self, host)
        self.queue_name = queue_name
        self._channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        self._publish(exchange='', routing_key=self.queue_name, message=message)

class MessageMiddlewareExchangeRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        """
        La lógica de inicialización deduce el comportamiento según las routing_keys:
        1. Tipo de Exchange: 'fanout' si no hay keys (broadcast) o 'direct' si las hay (sharding).
        2. Tipo de Cola: 
            Si hay una sola key, se usa una cola con nombre persistente. 
            Si no, se crea una cola anónima y exclusiva.
        """
        _RabbitMQMiddleware.__init__(self, host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        ex_type = 'fanout' if not routing_keys else 'direct'
        self._channel.exchange_declare(exchange=self.exchange_name, exchange_type=ex_type)

        if len(routing_keys) == 1:
            queue_name = routing_keys[0]
            exclusive = False
        else:
            queue_name = ''
            exclusive = True

        self.queue_name = self.declare_queue(
            queue_name=queue_name,
            exclusive=exclusive,
            auto_delete=False,
        )

        for routing_key in self.routing_keys:
            self.bind_queue(self.queue_name, routing_key)

    def declare_queue(self, queue_name, exclusive=False, auto_delete=False):
        result = self._channel.queue_declare(
            queue=queue_name,
            exclusive=exclusive,
            auto_delete=auto_delete,
        )
        return result.method.queue

    def bind_queue(self, queue_name, routing_key):
        self._channel.queue_bind(
            exchange=self.exchange_name,
            queue=queue_name,
            routing_key=routing_key,
        )

    def send(self, message):
        routing_keys = self.routing_keys if self.routing_keys else ['']
        for routing_key in routing_keys:
            self._publish(exchange=self.exchange_name, routing_key=routing_key, message=message)