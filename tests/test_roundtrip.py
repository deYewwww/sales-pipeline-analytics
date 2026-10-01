"""
smoke test: Produce one event → Consume it back → Verify round-trip.

This proves:
  1. Confluent Cloud credentials work
  2. Producer can write to the topic
  3. Consumer can read from the topic
  4. JSON serialization/deserialization is intact

Usage:
    cd sales-pipeline-analytics
    python -m tests.test_roundtrip
"""

import json
import uuid
import sys
import time
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer, Consumer
from simulator.config import get_producer_config, get_consumer_config, TOPIC

MYT = timezone(timedelta(hours=8))  # Malaysia Timezone (UTC+8)

def test_roundtrip():
    # ----------------------------------------------------
    # 1. Produce a single event test with unique marker
    # ----------------------------------------------------
    marker = str(uuid.uuid4())
    test_event = {
        "event_id": marker,
        "deal_id": "test-deal-001",
        "event_type": "deal_created",
        "owner": "Test_User",
        "deal_name": "CCTV Installation - KL HQ",
        "deal_value_rm": 12500.00,
        "old_stage": None,
        "new_stage": "Enquiry",
        "timestamp": datetime.now(MYT).isoformat(),
        "metadata": {
            "source": "roundtrip_test",
            "vertical": "cctv"
        }
    }

    print("="*60)
    print("Kafka Roundtrip Smoke Test")
    print("="*60)
    print(f"\nTopic: {TOPIC}")
    print(f"Marker: {marker[:8]}...")

    # --- Produce --- 
    print("\n[1/3] Producing test event...")
    producer = Producer(get_producer_config())

    delivery_ok = False
    delivery_err = None

    def on_delivery(err, msg):
        nonlocal delivery_ok, delivery_err
        if err:
            delivery_err = err
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            delivery_ok = True
            print(f"    -> Delivered to partition {msg.partition()}, offset {msg.offset()}")

    producer.produce(
        TOPIC,
        key=test_event["deal_id"],
        value=json.dumps(test_event),
        callback=on_delivery
    )
    producer.flush(timeout=15)  # Wait for delivery

    if not delivery_ok:
        print(f"\n PRODUCED FAILED: {delivery_err}")
        print("  Check your .env credentials and that the topic exists.")
        sys.exit(1)
    print("  Event produced successfully.")

    # ----------------------------------------------------
    # 2. Consume and find our event
    # ----------------------------------------------------
    print("\n[2/3] Consuming events from topic (timeout 30s)...")

    consumer = Consumer(get_consumer_config(group_id=f"roundtrip-{marker[:8]}"))
    consumer.subscribe([TOPIC])

    found_event = None
    messages_consumed = 0

    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue  # No message yet
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            messages_consumed += 1
            payload = json.loads(msg.value().decode('utf-8'))

            if payload.get("event_id") == marker:
                found_event = payload
                break
    finally:
        consumer.close()

    # ----------------------------------------------------
    # 3. Verify
    # ----------------------------------------------------
    print(f"\n[3/3] Verification (Scanned {messages_consumed} messages)...")

    if found_event is None:
        print("\n  Roundtrip test FAILED: Could not find our test event.")
        print("  The event was produced but not consumed within 30s.")
        sys.exit(1)

    checks = [
        ("event_id",      found_event["event_id"] == marker),
        ("deal_id",       found_event["deal_id"] == "test-deal-001"),
        ("event_type",    found_event["event_type"] == "deal_created"),
        ("new_stage",     found_event["new_stage"] == "Enquiry"),
        ("deal_value_rm", found_event["deal_value_rm"] == 12500.00),
        ("vertical",      found_event["metadata"]["vertical"] == "cctv"),
    ]

    all_passed = True
    for field, ok in checks:
        status = "passed --->" if ok else "failed --->"
        print(f"{status} {field}")
        if not ok:
            all_passed = False
    print()
    if all_passed:
        print("  ══════════════════════════════════════")
        print("  ROUND-TRIP TEST PASSED")
        print("  ══════════════════════════════════════")
        print(f"\n  Your Confluent Cloud Kafka is working.")
        print(f"  Event traveled: Producer -> Kafka -> Consumer -> verified.")
    else:
        print("  ROUND-TRIP TEST FAILED — field mismatch")
        sys.exit(1)

if __name__ == "__main__":
    test_roundtrip()

    


