"""
Sample data producer for Tableflow demo.
Produces orders and customers to Confluent Cloud Kafka topics.

Usage:
  export CONFLUENT_BOOTSTRAP=<bootstrap-server>
  export CONFLUENT_API_KEY=<api-key>
  export CONFLUENT_API_SECRET=<api-secret>
  export SCHEMA_REGISTRY_URL=<sr-url>
  export SCHEMA_REGISTRY_API_KEY=<sr-key>
  export SCHEMA_REGISTRY_API_SECRET=<sr-secret>

  python demo/producer.py --topic orders --count 100
  python demo/producer.py --topic customers --count 50
"""
from __future__ import annotations

import argparse
import os
import random
import time
import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

PRODUCTS = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"]
CITIES = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Denver"]
NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]


def load_schema(schema_file: str) -> str:
    with open(schema_file) as f:
        return f.read()


def make_producer(topic: str, schema_file: str) -> SerializingProducer:
    sr_client = SchemaRegistryClient({
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
        ),
    })

    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=load_schema(schema_file),
    )

    return SerializingProducer({
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    })


def generate_order() -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1, 100):04d}",
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(9.99, 499.99), 2),
        "order_date": int(time.time() * 1000),
    }


def generate_customer() -> dict:
    return {
        "customer_id": f"CUST-{random.randint(1, 100):04d}",
        "name": random.choice(NAMES),
        "email": f"{random.choice(NAMES).lower()}@example.com",
        "city": random.choice(CITIES),
        "created_at": int(time.time() * 1000),
    }


def main():
    parser = argparse.ArgumentParser(description="Produce sample data to Confluent Cloud")
    parser.add_argument("--topic", required=True, choices=["orders", "customers"])
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between messages in seconds")
    args = parser.parse_args()

    schema_file = os.path.join(os.path.dirname(__file__), "schemas", f"{args.topic}.avsc")
    generator = generate_order if args.topic == "orders" else generate_customer

    producer = make_producer(args.topic, schema_file)

    print(f"Producing {args.count} records to '{args.topic}'...")
    for i in range(args.count):
        record = generator()
        key = record.get("order_id") or record.get("customer_id")
        producer.produce(topic=args.topic, key=key, value=record)
        if (i + 1) % 10 == 0:
            producer.flush()
            print(f"  Produced {i + 1}/{args.count}")
        time.sleep(args.delay)

    producer.flush()
    print(f"Done. Produced {args.count} records to '{args.topic}'.")


if __name__ == "__main__":
    main()
