#!/usr/bin/env python3
"""
Avro producer — creates topics, registers schemas, and produces records.

Usage:
    python scripts/produce.py

    Loads credentials from .env.sync in the project root.

Environment variables:
    CONFLUENT_CLUSTER_ID        - Kafka cluster ID
    KAFKA_BOOTSTRAP_SERVER      - Bootstrap server (e.g. pkc-xxx.region.aws.confluent.cloud:9092)
    KAFKA_API_KEY               - Kafka API key
    KAFKA_API_SECRET            - Kafka API secret
    SCHEMA_REGISTRY_URL         - Schema Registry endpoint
    SCHEMA_REGISTRY_API_KEY     - SR API key
    SCHEMA_REGISTRY_API_SECRET  - SR API secret
"""

import json
import os
import random
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ── Load .env.sync ──────────────────────────────────────────

_env_file = Path(__file__).resolve().parent.parent / ".env.sync"
if _env_file.is_file():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVER"]
KAFKA_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_SECRET = os.environ["KAFKA_API_SECRET"]
SR_URL = os.environ["SCHEMA_REGISTRY_URL"]
SR_KEY = os.environ["SCHEMA_REGISTRY_API_KEY"]
SR_SECRET = os.environ["SCHEMA_REGISTRY_API_SECRET"]

# ── Schemas ─────────────────────────────────────────────────

TOPICS = {
    "invoices": {
        "schema": {
            "type": "record",
            "name": "Invoice",
            "namespace": "com.example.billing",
            "fields": [
                {"name": "invoice_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "currency", "type": "string"},
                {"name": "due_date", "type": "string"},
                {"name": "status", "type": "string"},
            ],
        },
        "generate": lambda i: {
            "invoice_id": f"INV-{i:05d}",
            "customer_id": f"CUST-{random.randint(1, 200):04d}",
            "amount": round(random.uniform(50, 5000), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "due_date": f"2026-{random.randint(4,12):02d}-{random.randint(1,28):02d}",
            "status": random.choice(["PENDING", "PAID", "OVERDUE"]),
        },
    },
    "user_events": {
        "schema": {
            "type": "record",
            "name": "UserEvent",
            "namespace": "com.example.analytics",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "page", "type": "string"},
                {"name": "ip_address", "type": "string"},
                {"name": "timestamp_ms", "type": "long"},
            ],
        },
        "generate": lambda i: {
            "event_id": str(uuid.uuid4()),
            "user_id": f"USR-{random.randint(1, 500):05d}",
            "event_type": random.choice(["page_view", "click", "signup", "purchase", "logout"]),
            "page": random.choice(["/home", "/products", "/checkout", "/account", "/search"]),
            "ip_address": f"{random.randint(10,192)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "timestamp_ms": int(time.time() * 1000),
        },
    },
    "support_tickets": {
        "schema": {
            "type": "record",
            "name": "SupportTicket",
            "namespace": "com.example.support",
            "fields": [
                {"name": "ticket_id", "type": "string"},
                {"name": "customer_email", "type": "string"},
                {"name": "subject", "type": "string"},
                {"name": "priority", "type": "string"},
                {"name": "category", "type": "string"},
                {"name": "created_at", "type": "string"},
            ],
        },
        "generate": lambda i: {
            "ticket_id": f"TKT-{i:05d}",
            "customer_email": f"user{random.randint(1,200)}@example.com",
            "subject": random.choice([
                "Cannot login", "Billing issue", "Feature request",
                "Bug report", "Account locked", "Refund request",
            ]),
            "priority": random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
            "category": random.choice(["auth", "billing", "product", "account"]),
            "created_at": f"2026-04-{random.randint(1,6):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z",
        },
    },
    "inventory_updates": {
        "schema": {
            "type": "record",
            "name": "InventoryUpdate",
            "namespace": "com.example.warehouse",
            "fields": [
                {"name": "sku", "type": "string"},
                {"name": "warehouse_id", "type": "string"},
                {"name": "quantity_change", "type": "int"},
                {"name": "reason", "type": "string"},
                {"name": "updated_by", "type": "string"},
                {"name": "timestamp_ms", "type": "long"},
            ],
        },
        "generate": lambda i: {
            "sku": f"SKU-{random.randint(1000, 9999)}",
            "warehouse_id": random.choice(["WH-EAST", "WH-WEST", "WH-CENTRAL", "WH-EU"]),
            "quantity_change": random.randint(-50, 200),
            "reason": random.choice(["restock", "sold", "returned", "damaged", "transfer"]),
            "updated_by": f"operator-{random.randint(1, 20)}",
            "timestamp_ms": int(time.time() * 1000),
        },
    },
    "audit_logs": {
        "schema": {
            "type": "record",
            "name": "AuditLog",
            "namespace": "com.example.security",
            "fields": [
                {"name": "log_id", "type": "string"},
                {"name": "actor", "type": "string"},
                {"name": "action", "type": "string"},
                {"name": "resource", "type": "string"},
                {"name": "ip_address", "type": "string"},
                {"name": "result", "type": "string"},
                {"name": "timestamp_ms", "type": "long"},
            ],
        },
        "generate": lambda i: {
            "log_id": str(uuid.uuid4()),
            "actor": f"user-{random.randint(1, 100)}@corp.com",
            "action": random.choice(["LOGIN", "LOGOUT", "CREATE", "DELETE", "UPDATE", "EXPORT"]),
            "resource": random.choice(["database", "api_key", "user_account", "config", "report"]),
            "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "result": random.choice(["SUCCESS", "DENIED", "ERROR"]),
            "timestamp_ms": int(time.time() * 1000),
        },
    },
}

# ── Setup clients ───────────────────────────────────────────

sr_client = SchemaRegistryClient({
    "url": SR_URL,
    "basic.auth.user.info": f"{SR_KEY}:{SR_SECRET}",
})

producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": KAFKA_KEY,
    "sasl.password": KAFKA_SECRET,
}
producer = Producer(producer_conf)
string_serializer = StringSerializer("utf_8")

# ── Produce ─────────────────────────────────────────────────

RECORDS_PER_TOPIC = 100


def delivery_report(err, msg):
    if err:
        print(f"  FAILED: {err}")


for topic_name, topic_conf in TOPICS.items():
    schema_str = json.dumps(topic_conf["schema"])
    avro_serializer = AvroSerializer(sr_client, schema_str)

    print(f"\nProducing {RECORDS_PER_TOPIC} Avro records to '{topic_name}'...")
    for i in range(RECORDS_PER_TOPIC):
        record = topic_conf["generate"](i)
        try:
            producer.produce(
                topic=topic_name,
                key=string_serializer(str(i)),
                value=avro_serializer(
                    record, SerializationContext(topic_name, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
            if (i + 1) % 50 == 0:
                producer.flush()
        except Exception as e:
            print(f"  Error on record {i}: {e}")
            break

    producer.flush()
    print(f"  -> {RECORDS_PER_TOPIC} records produced")

print(f"\nDone: {len(TOPICS)} topics, {RECORDS_PER_TOPIC * len(TOPICS)} total records")
