
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
import heapq
from dataclasses import dataclass
from itertools import count
from typing import Literal, Optional
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Set Number of Events to send (-1 for infinite)
NUM_EVENTS = int(os.getenv('NUM_EVENTS', '-1'))

# Set batch size in kb and linger time in ms
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '16384'))
LINGER_MS = int(os.getenv('LINGER_MS', '10'))
KAFKA_ACKS = os.getenv('KAFKA_ACKS', 'all')

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

# Device types of users
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']

# Target number of active users
TARGET_ACTIVE_USERS = int(os.getenv('TARGET_ACTIVE_USERS', '50'))

# Session duration in seconds
SESSION_DURATION_SECONDS = (
    int(os.getenv('SESSION_DURATION_MIN', '10')),
    int(os.getenv('SESSION_DURATION_MAX', '60')),
)
if SESSION_DURATION_SECONDS[0] >= SESSION_DURATION_SECONDS[1]:
    raise ValueError('SESSION_DURATION_MIN must be less than SESSION_DURATION_MAX')

# Action delay in seconds. Adjust to control the rate of events.
ACTION_DELAY_SECONDS = (
    float(os.getenv('ACTION_DELAY_MIN', '0.2')),
    float(os.getenv('ACTION_DELAY_MAX', '1.5')),
)
if ACTION_DELAY_SECONDS[0] <= 0 or ACTION_DELAY_SECONDS[0] >= ACTION_DELAY_SECONDS[1]:
    raise ValueError('ACTION_DELAY_MIN must be positive and less than ACTION_DELAY_MAX')

# Logout failure probability. Adjust to control the rate of failed logouts.
LOGOUT_FAILURE_PROB = float(os.getenv('LOGOUT_FAILURE_PROB', '0.1'))
if not 0 <= LOGOUT_FAILURE_PROB <= 1:
    raise ValueError('LOGOUT_FAILURE_PROB must be between 0 and 1')

# Session inactivity grace period in seconds. Adjust to control the rate of expired sessions.
SESSION_INACTIVITY_GRACE = float(os.getenv('SESSION_INACTIVITY_GRACE', '5'))
if SESSION_INACTIVITY_GRACE <= 0:
    raise ValueError('SESSION_INACTIVITY_GRACE must be positive')

# Random action delay in seconds
def _random_action_delay() -> float:
    return random.uniform(*ACTION_DELAY_SECONDS)

# Session class - use dataclass to store session state because it simplifies the code
@dataclass
class Session:
    user_id: str
    ip_address: str
    device: str
    start_time: float
    end_time: float
    next_event_at: float
    state: Literal['login', 'page_view', 'click', 'logout','completed','expired']
    last_page_url: Optional[str] = None
    release_at: Optional[float] = None


    # Emit event based on current state
    def emit_event(self, now: float) -> dict[str, object]:
        timestamp = datetime.now().isoformat()
        # Emit login event
        if self.state == 'login':
            self.state = 'page_view'
            self.next_event_at = now + _random_action_delay()
            return {
                'event_type': 'login',
                'user_id': self.user_id,
                'timestamp': timestamp,
                'ip': self.ip_address,
                'device': self.device,
                'success': True,
            }

        # Emit page view event
        if self.state == 'page_view':
            self.last_page_url = fake.uri_path()
            referrer = fake.uri() if random.random() > 0.5 else None
            self.state = 'click'
            self.next_event_at = now + _random_action_delay()
            return {
                'event_type': 'page_view',
                'user_id': self.user_id,
                'timestamp': timestamp,
                'page_url': self.last_page_url,
                'referrer': referrer,
                'duration_seconds': random.randint(5, 45),
            }

        # Emit click event
        if self.state == 'click':
            event = {
                'event_type': 'click',
                'user_id': self.user_id,
                'timestamp': timestamp,
                'element_id': fake.word(),
                'page_url': self.last_page_url or fake.uri_path(),
            }

            if now >= self.end_time:
                if random.random() < LOGOUT_FAILURE_PROB:
                    self.state = 'expired'
                    self.release_at = now + SESSION_INACTIVITY_GRACE
                    self.next_event_at = None
                else:
                    self.state = 'logout'
                    self.next_event_at = now + _random_action_delay()
            else:
                self.state = 'page_view'
                self.next_event_at = now + _random_action_delay()

            return event

        # Emit logout event
        if self.state == 'logout':
            self.state = 'completed'
            self.release_at = now
            self.next_event_at = None
            session_duration = max(0.0, now - self.start_time)
            return {
                'event_type': 'logout',
                'user_id': self.user_id,
                'timestamp': timestamp,
                'session_duration_seconds': session_duration,
            }
    
        raise RuntimeError(f'Unexpected session state {self.state}')

# Session manager
class SessionManager:
    def __init__(self, target_users: int) -> None:
        self.target_users = target_users
        self._queue: list[tuple[float, int, Session]] = []
        self._cooldown: list[tuple[float, int]] = []
        self._active_sessions: list[Session] = []
        self._counter = count()

        now = time.monotonic()
        for _ in range(self.target_users):
            self._register_session(self._build_session(now))

    # Time until next event
    def time_until_next_event(self) -> float:
        now = time.monotonic()
        self._release_cooldown_sessions(now)
        self._maybe_spawn_sessions(now)

        if not self._queue:
            return 0.1

        next_due, _, _ = self._queue[0]
        return max(0.0, next_due - now)

    # Pop next event
    def pop_next_event(self) -> Optional[dict[str, object]]:
        while True:
            now = time.monotonic()
            self._release_cooldown_sessions(now)
            self._maybe_spawn_sessions(now)

            if not self._queue:
                return None

            next_due, _, session = heapq.heappop(self._queue)
            now = time.monotonic()

            if next_due > now:
                heapq.heappush(self._queue, (next_due, next(self._counter), session))
                return None

            event = session.emit_event(now)

            if session.state in ('completed', 'expired'):
                try:
                    self._active_sessions.remove(session)
                except ValueError:
                    pass
                release_at = session.release_at if session.release_at is not None else now
                heapq.heappush(self._cooldown, (release_at, next(self._counter)))
            else:
                heapq.heappush(
                    self._queue,
                    (session.next_event_at, next(self._counter), session),
                )

            if event is not None:
                return event

    # Build session with random duration
    def _build_session(self, now: float) -> Session:
        duration = random.randint(*SESSION_DURATION_SECONDS)
        return Session(
            user_id=fake.uuid4(),
            ip_address=fake.ipv4(),
            device=random.choice(DEVICE_TYPES),
            start_time=now,
            end_time=now + duration,
            next_event_at=now,
            state='login',
        )

    # Register session to the queue
    def _register_session(self, session: Session) -> None:
        self._active_sessions.append(session)
        heapq.heappush(
            self._queue,
            (session.next_event_at, next(self._counter), session),
        )

    # Release cooldown sessions
    def _release_cooldown_sessions(self, now: float) -> None:
        while self._cooldown and self._cooldown[0][0] <= now:
            heapq.heappop(self._cooldown)

    # Maybe spawn sessions
    def _maybe_spawn_sessions(self, now: float) -> None:
        while len(self._active_sessions) + len(self._cooldown) < self.target_users:
            session = self._build_session(now)
            self._register_session(session)

# Now initialize producer with provisioned servers
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    security_protocol='SSL',
    ssl_cafile=CA_PATH,
    ssl_certfile=CERT_PATH,
    ssl_keyfile=KEY_PATH,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks=KAFKA_ACKS,
    batch_size=BATCH_SIZE,
    linger_ms=LINGER_MS,
)

# Main function
def main() -> None:
    """Stream synthetic events into the configured Kafka topic for testing."""
    print(f"Starting simulation: Producing to topic {WEBSITE_EVENTS_TOPIC}")
    sent_count = 0
    session_manager = SessionManager(TARGET_ACTIVE_USERS)
    try:
        while NUM_EVENTS == -1 or sent_count < NUM_EVENTS:
            wait_for = session_manager.time_until_next_event()
            if wait_for > 0:
                time.sleep(wait_for)

            event = session_manager.pop_next_event()
            if event is None:
                continue

            future = producer.send(WEBSITE_EVENTS_TOPIC, value=event)

            def on_success(metadata):
                print(f"Event sent: {event['event_type']} to partition {metadata.partition}")
            
            def on_error(exc):
                print(f"Error sending event: {exc}")
            
            future.add_callback(on_success)
            future.add_errback(on_error)
            
            sent_count += 1

    except KeyboardInterrupt:
        print("Simulation interrupted.")
    finally:
        producer.flush()
        producer.close()
if __name__ == "__main__":
    main()