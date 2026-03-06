#!/usr/bin/env python3
"""Delete Kafka topics on a dedicated PrivateLink cluster.

Connects via the Kafka protocol (port 9092) over PrivateLink.
Must be run from a host with PrivateLink access (e.g., bastion).

Usage:
    pip install confluent-kafka   # one-time
    python3 delete-topics.py [topic1 topic2 ...]

If no topics are specified, deletes: orders, customers, pageviews.

Environment variables (from .env.topics):
    KAFKA_REST_ENDPOINT  - Used to derive bootstrap server (same host, port 9092)
    KAFKA_API_KEY        - Kafka API key
    KAFKA_API_SECRET     - Kafka API secret
"""

import os
import sys

DEFAULT_TOPICS = ["orders", "customers", "pageviews"]


def main():
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:
        print("confluent-kafka not installed. Run: pip install confluent-kafka")
        sys.exit(1)

    rest_endpoint = os.environ.get("KAFKA_REST_ENDPOINT", "")
    api_key = os.environ.get("KAFKA_API_KEY", "")
    api_secret = os.environ.get("KAFKA_API_SECRET", "")

    if not all([rest_endpoint, api_key, api_secret]):
        print("Missing env vars. Source .env.topics first:")
        print("  set -a && source .env.topics && set +a")
        sys.exit(1)

    # Derive bootstrap from REST endpoint (same host, port 9092)
    bootstrap = rest_endpoint.replace("https://", "").split("/")[0]
    bootstrap = bootstrap.rsplit(":", 1)[0] + ":9092"

    topics = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_TOPICS

    print(f"Bootstrap: {bootstrap}")
    print(f"Topics to delete: {', '.join(topics)}")

    admin = AdminClient({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    })

    # Verify connectivity by listing topics
    try:
        metadata = admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
        print(f"Connected. Cluster has {len(existing)} topics.")
    except Exception as e:
        print(f"Failed to connect: {e}")
        print("Make sure you're running from a host with PrivateLink access (e.g., bastion).")
        sys.exit(1)

    to_delete = [t for t in topics if t in existing]
    skipped = [t for t in topics if t not in existing]

    if skipped:
        for t in skipped:
            print(f"  {t}: not found (already deleted)")

    if not to_delete:
        print("Nothing to delete.")
        return

    futures = admin.delete_topics(to_delete, operation_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"  {topic}: deleted")
        except Exception as e:
            print(f"  {topic}: failed — {e}")


if __name__ == "__main__":
    main()
