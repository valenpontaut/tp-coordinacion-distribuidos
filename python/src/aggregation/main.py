import os
import logging
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
        self.fruit_top = {}
        self.eof_count = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(
            f"Processing data: client={client_id} fruit={fruit} amount={amount}"
        )
        top = self.fruit_top.setdefault(client_id, {})
        top[fruit] = top.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        count = self.eof_count[client_id]
        logging.info(f"EOF received for client {client_id} ({count}/{SUM_AMOUNT})")
        if count < SUM_AMOUNT:
            return

        top = self.fruit_top.get(client_id, {})
        top_chunk = sorted(top.values())[-TOP_SIZE:]
        top_chunk.reverse()
        top_list = [[fi.fruit, fi.amount] for fi in top_chunk]
        logging.info(
            f"All EOFs received for client {client_id}, sending top {len(top_list)} fruits"
        )
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, top_list])
        )

        del self.eof_count[client_id]
        top.clear()
        self.fruit_top.pop(client_id, None)

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            client_id, fruit, amount = fields
            self._process_data(client_id, fruit, int(amount))
        else:
            client_id = fields[0]
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)
        self.output_queue.close()

    def stop(self):
        self.input_exchange.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Starting AggregationFilter ID={ID}")
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda s, f: aggregation_filter.stop())
    aggregation_filter.start()
    logging.info("AggregationFilter stopped")


if __name__ == "__main__":
    main()
