"""
kafka/producer.py
Simulates real-time stock price feed publishing to Kafka topic: stock-prices
Run: python kafka/producer.py
"""
import json, time, random, logging
from datetime import datetime
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer")

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock-prices"

TICKERS = {
    "AAPL":  274.95, "MSFT":  404.71, "GOOGL": 312.64,
    "AMZN":  210.73, "NVDA":  194.27, "META":  650.55,
    "TSLA":  414.42, "BRK-B": 494.65, "UNH":   286.98, "JNJ": 245.11,
}

def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)
    else:
        log.info("Delivered: %s [partition=%d offset=%d]", msg.topic(), msg.partition(), msg.offset())

def run():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    log.info("Producer started. Publishing to topic: %s", TOPIC)

    prices = dict(TICKERS)

    try:
        while True:
            for ticker, base_price in prices.items():
                change_pct = random.uniform(-0.005, 0.005)
                prices[ticker] = round(prices[ticker] * (1 + change_pct), 2)

                message = {
                    "ticker":     ticker,
                    "price":      prices[ticker],
                    "change_pct": round(change_pct * 100, 4),
                    "volume":     random.randint(10000, 500000),
                    "timestamp":  datetime.utcnow().isoformat() + "Z",
                    "source":     "kafka-producer",
                }

                producer.produce(
                    TOPIC,
                    key=ticker,
                    value=json.dumps(message).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)

            producer.flush()
            log.info("Batch published for %d tickers", len(prices))
            time.sleep(5)

    except KeyboardInterrupt:
        log.info("Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    run()
