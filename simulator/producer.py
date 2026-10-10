import json
import uuid
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer
from simulator.config import get_producer_config, TOPIC

# Malaysia Timezone (UTC+8)
MYT = timezone(timedelta(hours=8))  

# Use in Flush() and in the error message 
FLUSH_TIMEOUT=10

# Function to create a test event
def create_test_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "deal_id": str(uuid.uuid4()),
        "event_type": "deal_created",
        "owner": "Aisyah",
        "deal_name": "CCTV Installation - PJ Branch",
        "deal_value_rm": 8400.00,
        "old_stage": None,
        "new_stage": "Enquiry",
        "timestamp": datetime.now(MYT).isoformat(),
        "metadata": {
            "source": "simulator_v1",
            "vertical": "cctv"
        }
    }


# Function to produce a message to Kafka
def produce_events(count: int = 1):
    producer = Producer(get_producer_config())
    failures = []

    # Callback function to handle delivery reports 
    def delivery_callback(err, msg):
        if err is not None:
            failure.append(err)
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            print(f"Delivered to {msg.topic()}: [partition {msg.partition()}] @ offset {msg.offset()}")

    for i in range(count):
        event=create_test_event()
        producer.produce(
            TOPIC,
            key=event["deal_id"],
            value=json.dumps(event),
            callback=delivery_callback
        )
        print(f"[{i+1}/{count}] Queued event_id = {event['event_id'][:8]}...")

    remaining = producer.flush(timeout=10)

    if remaining > 0 or failures:
        raise RuntimeError(f"{len(failures)} failed, {remaining} not comfirmed.")
    else:
        print(f"All {count} messages delivered successfully.")

if __name__ == "__main__":
    produce_events(1)
