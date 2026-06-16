import logging
import time

logger = logging.getLogger(__name__)


class CDCConsumer:
    MAX_RETRIES = 3

    def __init__(self, kafka_consumer, es_writer, dlq_publisher):
        self.consumer = kafka_consumer
        self.es = es_writer
        self.dlq = dlq_publisher

    def run(self):
        logger.info("starting CDC consumer")
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=1000)
                for partition, records in messages.items():
                    for record in records:
                        self._process(record)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"consumer error: {e}")
                time.sleep(1)

    def _process(self, record):
        event = self._deserialize(record)
        if event is None:
            self.consumer.commit(record)
            return

        for attempt in range(self.MAX_RETRIES):
            try:
                if event['op'] == 'd':
                    self.es.delete(event['entity_id'], event['source_ts'])
                else:
                    self.es.write(event['entity_id'], event['payload'], event['source_ts'])
                self.consumer.commit(record)
                return
            except Exception as e:
                if attempt == self.MAX_RETRIES - 1:
                    logger.error(f"routing {event['entity_id']} to DLQ after {self.MAX_RETRIES} attempts")
                    self.dlq.publish({'entity_id': event['entity_id'], 'source_ts': event['source_ts']})
                    self.consumer.commit(record)
                else:
                    time.sleep(0.1 * (2 ** attempt))

    def _deserialize(self, record):
        try:
            return record.value
        except Exception as e:
            logger.error(f"deserialize failed: {e}")
            return None