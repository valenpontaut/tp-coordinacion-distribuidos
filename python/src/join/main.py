import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

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
        self.partial_tops = {}

    def _merge_tops(self, tops):
        merged = {}
        for top in tops:
            for fi in top:
                merged[fi.fruit] = merged.get(fi.fruit, fruit_item.FruitItem(fi.fruit, 0)) + fi
        top_chunk = sorted(merged.values())[-TOP_SIZE:]
        top_chunk.reverse()
        return [[fi.fruit, fi.amount] for fi in top_chunk]

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        partial_top = [fruit_item.FruitItem(f, a) for f, a in fields[1]]

        self.partial_tops.setdefault(client_id, []).append(partial_top)

        if len(self.partial_tops[client_id]) == AGGREGATION_AMOUNT:
            final_top = self._merge_tops(self.partial_tops.pop(client_id))
            logging.info(f"Sending final top for client {client_id}")
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, final_top])
            )
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)
        self.output_queue.close()

    def stop(self):
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda s, f: join_filter.stop())
    join_filter.start()


if __name__ == "__main__":
    main()
