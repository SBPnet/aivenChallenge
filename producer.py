
"""Synthetic clickstream producer for Aiven for Apache Kafka®.

This module demonstrates how Aiven customers can bootstrap TLS credentials
directly from Terraform outputs and stream JSON payloads into a managed Kafka
topic. Use it as a template when wiring real-world data sources into the
platform.
"""
import json
import time
import random
import subprocess
import tempfile
import atexit
import os
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Set Number of Events to send (-1 for infinite)
NUM_EVENTS = -1

# Set Delay between events
DELAY_SECONDS = 1

# Set batch size in kb and linger time in ms
BATCH_SIZE = 16384
LINGER_MS = 10

# Get Terraform outputs
terraform_dir = os.path.join(os.path.dirname(__file__), 'terraform')
output = subprocess.run(
    ['terraform', 'output', '-json'],
    check=True,
    capture_output=True,
    cwd=terraform_dir,
)
output_json = json.loads(output.stdout)

try:
    KAFKA_BOOTSTRAP = output_json['challenge_kafka_bootstrap']['value']
    WEBSITE_EVENTS_TOPIC = output_json['website_events_topic']['value']
    KAFKA_ACCESS_CERT = output_json['challenge_kafka_access_cert']['value']
    KAFKA_ACCESS_KEY = output_json['challenge_kafka_access_key']['value']
    KAFKA_CA_CERT = output_json['challenge_kafka_ca_cert']['value']
except KeyError as err:
    raise KeyError(
        'Required Terraform output is missing. Ensure `terraform apply` has been run '
        f'in `{terraform_dir}` and that all outputs are defined. Missing key: {err}'
    ) from err

# Create list to store temp file paths
_temp_paths = []

# Write PEM content to temp file and store path
def _write_pem(content: str, suffix: str) -> str:
    temp_file = tempfile.NamedTemporaryFile('w', suffix=suffix, delete=False)
    temp_file.write(content)
    temp_file.flush()
    temp_file.close()
    _temp_paths.append(temp_file.name)
    return temp_file.name

# Cleanup temp files on exit
def _cleanup_temp_files() -> None:
    for path in _temp_paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

# Register cleanup function to run on exit
atexit.register(_cleanup_temp_files)

# Write PEM content to temp files
CERT_PATH = _write_pem(KAFKA_ACCESS_CERT, suffix='_cert.pem')
KEY_PATH = _write_pem(KAFKA_ACCESS_KEY, suffix='_key.pem')
CA_PATH = _write_pem(KAFKA_CA_CERT, suffix='_ca.pem')

# Initialize Faker
fake = Faker()



# Now initialize producer with provisioned servers
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    security_protocol='SSL',
    ssl_cafile=CA_PATH,
    ssl_certfile=CERT_PATH,
    ssl_keyfile=KEY_PATH,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks='all',
    batch_size=BATCH_SIZE,
    linger_ms=LINGER_MS  
)

def generate_event() -> dict[str, object]:
    """Generate a single clickstream event ready for Kafka serialization."""

    event_type = random.choice(['login', 'page_view', 'click', 'logout'])
    user_id = fake.uuid4()
    timestamp = datetime.now().isoformat()
    ip_address = fake.ipv4()
    
    if event_type == 'login':
        return {'event_type': 'login', 'user_id': user_id, 'timestamp': timestamp, 'ip': ip_address, 'device': random.choice(['mobile', 'desktop', 'tablet']), 'success': random.choice([True, False])}
    elif event_type == 'page_view':
        return {'event_type': 'page_view', 'user_id': user_id, 'timestamp': timestamp, 'page_url': fake.uri_path(), 'referrer': fake.uri() if random.random() > 0.5 else None, 'duration_seconds': random.randint(5, 300)}
    elif event_type == 'click':
        return {'event_type': 'click', 'user_id': user_id, 'timestamp': timestamp, 'element_id': fake.word(), 'page_url': fake.uri_path()}
    elif event_type == 'logout':
        return {'event_type': 'logout', 'user_id': user_id, 'timestamp': timestamp, 'session_duration': random.randint(60, 3600)}

def main() -> None:
    """Stream synthetic events into the configured Kafka topic for testing."""
    print(f"Starting simulation: Producing to topic {WEBSITE_EVENTS_TOPIC}")
    sent_count = 0
    try:
        while NUM_EVENTS == -1 or sent_count < NUM_EVENTS:
            event = generate_event()
            future = producer.send(WEBSITE_EVENTS_TOPIC, value=event)
            
            def on_success(metadata):
                print(f"Event sent: {event['event_type']} to partition {metadata.partition}")
            
            def on_error(exc):
                print(f"Error sending event: {exc}")
            
            future.add_callback(on_success)
            future.add_errback(on_error)
            
            sent_count += 1
            time.sleep(DELAY_SECONDS)
            
    except KeyboardInterrupt:
        print("Simulation interrupted.")
    finally:
        producer.flush()
        producer.close()
        print(f"Produced {sent_count} events.")
if __name__ == "__main__":
    main()