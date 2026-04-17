import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queue_name = queue_name
        except pika.exceptions.AMQPConnectionError as e:
            raise RuntimeError(f"No se pudo conectar: {e}")

    def start_consuming(self, on_message_callback):
        def on_message(channel, method, properties, body):
            def ack():
                channel.basic_ack(method.delivery_tag)
            def nack():
                channel.basic_nack(method.delivery_tag)
            on_message_callback(body, ack, nack)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()
        self.close()

    def send(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message
        )

    def close(self):
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys

            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue

            for key in routing_keys:
                self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise RuntimeError(f"No se pudo conectar al broker: {e}")

    def start_consuming(self, on_message_callback):
        def on_message(channel, method, properties, body):
            def ack():
                channel.basic_ack(method.delivery_tag)
            def nack():
                channel.basic_nack(method.delivery_tag)
            on_message_callback(body, ack, nack)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()
        self.close()

    def send(self, message, routing_key=None):
        keys = [routing_key] if routing_key else self.routing_keys
        for key in keys:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=key,
                body=message
            )

    def close(self):
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()
