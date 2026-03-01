"""
kafka/consumer.py
Consumes stock price messages from Kafka and logs them
Run: python kafka/consumer.py
"""
import json, logging
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-prices"
GROUP_ID = "equity-research-consumer"

def run():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC])
    log.info("Consumer started. Listening to topic: %s", TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Consumer error: %s", msg.error())
                continue

            data = json.loads(msg.value().decode("utf-8"))
            log.info(
                "%-6s | price=%-8.2f | change=%+.4f%% | vol=%d | ts=%s",
                data["ticker"], data["price"], data["change_pct"],
                data["volume"], data["timestamp"],
            )

    except KeyboardInterrupt:
        log.info("Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()
