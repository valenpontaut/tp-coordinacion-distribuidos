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
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.dedicated_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_PREFIX}_{ID}"
        )
        self.amount_by_fruit = {}
        self.eof_received = {}
        self.dedicated_consumer = None
        self.dedicated_thread = None

    def _aggregator_idx(self, client_id, fruit):
        key = f"{fruit}_{client_id}".encode()
        return int(hashlib.md5(key).hexdigest(), 16) % AGGREGATION_AMOUNT

    def _flush(self, client_id):
        client_data = self.amount_by_fruit.pop(client_id, {})
        for fi in client_data.values():
            idx = self._aggregator_idx(client_id, fi.fruit)
            self.aggregation_exchange.send(
                message_protocol.internal.serialize([client_id, fi.fruit, fi.amount]),
                routing_key=f"{AGGREGATION_PREFIX}_{idx}",
            )
        client_data.clear()
        del client_data
        self.aggregation_exchange.send(
            message_protocol.internal.serialize([client_id])
        )

    def _forward_to_dedicated(self, message, ack, nack):
        self.dedicated_queue.send(message)
        ack()

    def _process_dedicated(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            client_id, fruit, amount = fields
            client_data = self.amount_by_fruit.setdefault(client_id, {})
            client_data[fruit] = client_data.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
        else:
            client_id = fields[0]
            if client_id not in self.eof_received:
                logging.info(f"First EOF for client {client_id}, propagating and flushing")
                self.eof_received[client_id] = True
                for i in range(SUM_AMOUNT):
                  if i != ID:
                      q = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"{SUM_PREFIX}_{i}")
                      q.send(message_protocol.internal.serialize([client_id]))
                      q.close()
                self._flush(client_id)
            else:
                logging.info(f"Ignoring duplicate EOF for client {client_id}")
        ack()

    def _run_dedicated_consumer(self):
        self.dedicated_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_PREFIX}_{ID}"
        )
        self.aggregation_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)],
        )
        self.dedicated_consumer.start_consuming(self._process_dedicated)
        self.aggregation_exchange.close()

    def start(self):
        self.dedicated_thread = threading.Thread(
            target=self._run_dedicated_consumer, daemon=True
        )
        self.dedicated_thread.start()

        self.input_queue.start_consuming(self._forward_to_dedicated)

        if self.dedicated_consumer is not None:
            self.dedicated_consumer.stop_consuming()
        self.dedicated_thread.join()
        self.dedicated_queue.close()

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