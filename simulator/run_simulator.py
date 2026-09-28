import uuid
import json
import argparse
from confluent_kafka import Producer
from simulator.config import get_producer_config, TOPIC 
from simulator.deal_generator import generate_deal_lifecycle

def delivery_callback(err, msg):
    '''Called once per message to confirm delivery or report failure.'''
    if err is not None:
        print(f" FAILED: {err}")

def main():
    # ---- Parse CLI arguments ----
    parser = argparse.ArgumentParser(description="SME Deal Event Simulator")
    parser.add_argument("--deals", type = int, default = 20, help = "Number of deals to simulate")
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f" SME Deal Event Simulator")
    print(f" Generating {args.deals} deals...")
    print(f"{'='*50}\n")

    # ---- Generate all deal lifecycle ----
    all_events = []
    for i in range(args.deals):
        deal_id = str(uuid.uuid4())
        events = generate_deal_lifecycle(deal_id)
        all_events.extend(events)
        print(f" Deal {i+1}/{args.deals}: {len(events)} events")

    # ---- Sort by timestamp ----
    all_events.sort(key = lambda e: e["timestamp"])

    print(f"\n Total events: {len(all_events)}")
    print(f" Producing to topic: {TOPIC}\n")

    # ---- Send to Kafka ----
    producer = Producer(get_producer_config())

    for event in all_events:
        producer.produce(
            TOPIC,
            key = event["deal_id"],
            value = json.dumps(event),
            callback = delivery_callback
        )
    remaining = producer.flush(timeout = 30)

    # ---- Summary ----
    delivered = len(all_events) - remaining
    print(f"\n{'='*50}")
    print(f" Delivered: {delivered}/{len(all_events)} events")
    if remaining > 0:
        print(f" Warning: {remaining} events still in queue...")
    else:
        print(f" All events delivered successfully")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()

