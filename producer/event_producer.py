"""Kafka event producer with configurable throughput."""
import argparse
import json
import time
import uuid
import logging
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()


class EventProducer:
    """High-throughput Kafka event producer."""

    def __init__(self, brokers: str, topic: str, rate: int = 1000):
        self.topic = topic
        self.rate = rate  # events per second
        self.producer = Producer({
            "bootstrap.servers": brokers,
            "batch.size": 65536,
            "linger.ms": 5,
            "compression.type": "snappy",
            "acks": "all",
        })

    def generate_event(self) -> dict:
        """Generate a realistic event payload."""
        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": fake.uuid4(),
            "session_id": fake.uuid4(),
            "event_type": fake.random_element(["click", "view", "purchase", "scroll", "search"]),
            "page_url": fake.url(),
            "device_type": fake.random_element(["mobile", "desktop", "tablet"]),
            "country": fake.country_code(),
            "content_id": fake.uuid4(),
            "duration_ms": fake.random_int(100, 30000),
            "metadata": {
                "browser": fake.user_agent(),
                "ip": fake.ipv4(),
            },
        }

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for {msg.key()}: {err}")

    def produce(self, duration_seconds: int = 60):
        """Produce events at specified rate for given duration."""
        start = time.time()
        total_events = 0
        interval = 1.0 / self.rate

        logger.info(f"Starting producer | rate={self.rate}/sec | topic={self.topic}")

        while (time.time() - start) < duration_seconds:
            event = self.generate_event()
            self.producer.produce(
                self.topic,
                key=event["user_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=self._delivery_report,
            )
            total_events += 1

            if total_events % 1000 == 0:
                self.producer.poll(0)
                elapsed = time.time() - start
                actual_rate = total_events / elapsed
                logger.info(f"Produced {total_events:,} events | rate={actual_rate:.0f}/sec")

            time.sleep(interval)

        self.producer.flush()
        logger.info(f"Done. Total events produced: {total_events:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka event producer")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="raw-events")
    parser.add_argument("--rate", type=int, default=1000, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    args = parser.parse_args()

    producer = EventProducer(brokers=args.brokers, topic=args.topic, rate=args.rate)
    producer.produce(duration_seconds=args.duration)
