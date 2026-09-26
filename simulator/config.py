'''
Confluent Cloud Kafka configuration.

Loads credentials from .env and builds the producer/consumer config dicts 
that confluent-kafka's Python client expects.

Why SASL_SSL? Confluent Cloud requires encrypted + authenticated for all connections. 
 - SALS mechanism: PLAIN (username=<API_KEY>/password=<API_SECRET>)
 - Security protocol: SASL_SSL (TLS + SASL auth)
'''

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")
TOPIC = os.getenv("KAFKA_TOPIC", "deal_events")

# Validate required environment variables
if not all([BOOTSTRAP_SERVERS, API_KEY, API_SECRET]):
    missing = [
        name for name, val in [
            ("CONFLUENT_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS),
            ("CONFLUENT_API_KEY", API_KEY),
            ("CONFLUENT_API_SECRET", API_SECRET),
        ]
        if not val
    ]
    raise EnvironmentError(
        f"Missing required env vars: {', '.join(missing)}. Copy .env.example to .env and fill in your credentials."
    )

# Kafka producer and consumer configuration
def get_producer_config() -> dict:
    return {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'acks': 'all',
    'enable.idempotence': True,
    'linger.ms': 5,
    'retries': 3,
}

def get_consumer_config(group_id: str = 'test-consumer-group') -> dict:
    return {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
}
