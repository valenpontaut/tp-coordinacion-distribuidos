import os
import logging
import threading
import signal
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.amount_by_fruit = {}
        self.lock = threading.Lock()
        self.control_consumer = None
        self.aggregation_exchange = None
        self.control_thread = None

    def _aggregator_idx(self, client_id, fruit):
        key = f"{fruit}_{client_id}".encode()
        return int(hashlib.md5(key).hexdigest(), 16) % AGGREGATION_AMOUNT

    def _process_data(self, client_id, fruit, amount):
        logging.info(
            f"Processing data: client={client_id} fruit={fruit} amount={amount}"
        )
        with self.lock:
            client_data = self.amount_by_fruit.setdefault(client_id, {})
            client_data[fruit] = client_data.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            client_id, fruit, amount = fields
            self._process_data(client_id, fruit, amount)
        else:
            client_id = fields[0]
            logging.info(f"EOF client {client_id}, broadcasting to control exchange")
            self.control_publisher.send(
                message_protocol.internal.serialize([client_id])
            )
        ack()

    def _run_control_consumer(self):
        self.control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        self.aggregation_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)],
        )

        def on_eof(message, ack, nack):
            client_id = message_protocol.internal.deserialize(message)[0]
            logging.info(
                f"EOF received for client {client_id}, flushing accumulated data"
            )
            with self.lock:
                client_data = self.amount_by_fruit.pop(client_id, {})
            logging.info(
                f"Sending {len(client_data)} fruit totals to aggregators for client {client_id}"
            )
            for fi in client_data.values():
                idx = self._aggregator_idx(client_id, fi.fruit)
                self.aggregation_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, fi.fruit, fi.amount]
                    ),
                    routing_key=f"{AGGREGATION_PREFIX}_{idx}",
                )
            client_data.clear()
            del client_data
            logging.info(f"Sending EOF to aggregators for client {client_id}")
            self.aggregation_exchange.send(
                message_protocol.internal.serialize([client_id])
            )
            ack()

        self.control_consumer.start_consuming(on_eof)
        self.aggregation_exchange.close()

    def start(self):
        self.control_thread = threading.Thread(
            target=self._run_control_consumer, daemon=True
        )
        self.control_thread.start()

        self.input_queue.start_consuming(self.process_data_message)
        if self.control_consumer is not None:
            self.control_consumer.stop_consuming()
        self.control_thread.join()
        self.control_publisher.close()

    def stop(self):
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Starting SumFilter ID={ID}")
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda s, f: sum_filter.stop())
    sum_filter.start()
    logging.info("SumFilter stopped")


if __name__ == "__main__":
    main()
