## Stream consumer for Aiven-managed Kafka, PostgreSQL, and OpenSearch services.
##
## This module illustrates how to securely pull events from a Kafka topic, aggregate 
## them into session metrics, and persist the results to managed PostgreSQL and OpenSearch
## instances provisioned via Terraform.

import json
import subprocess
import tempfile
import atexit
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import Json
from opensearchpy import OpenSearch

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
    POSTGRES_URI = output_json['challenge_postgres_service_uri']['value']
    OPENSEARCH_URI = output_json['challenge_opensearch_service_uri']['value']
    KAFKA_ACCESS_CERT = output_json['challenge_kafka_access_cert']['value']
    KAFKA_ACCESS_KEY = output_json['challenge_kafka_access_key']['value']
    KAFKA_CA_CERT = output_json['challenge_kafka_ca_cert']['value']
except KeyError as err:
    raise KeyError(
        'Required Terraform output is missing. Ensure `terraform apply` has been run '
        f'in `{terraform_dir}` and that all outputs are defined. Missing key: {err}'
    ) from err

# Session timeout - the time between events before a session is considered expired
# This is set low for testing purposes
SESSION_TIMEOUT = timedelta(minutes=0.5)

_temp_paths = []
_active_sessions: Dict[str, Dict[str, Any]] = {}

# Helper function to write PEM material to a temp file
def _write_pem(content: str, suffix: str) -> str:
    """Persist PEM material to a temp file so TLS clients can reference it."""
    temp_file = tempfile.NamedTemporaryFile('w', suffix=suffix, delete=False)
    temp_file.write(content)
    temp_file.flush()
    temp_file.close()
    _temp_paths.append(temp_file.name)
    return temp_file.name

# Helper function to clean up temp files
def _cleanup_temp_files() -> None:
    """Remove any credential files created at runtime before the process exits."""
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

# Initialize Kafka consumer
consumer = KafkaConsumer(
    WEBSITE_EVENTS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    security_protocol='SSL',
    ssl_cafile=CA_PATH,
    ssl_certfile=CERT_PATH,
    ssl_keyfile=KEY_PATH,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='website-events-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize PostgreSQL connection
pg_conn: PGConnection = psycopg2.connect(POSTGRES_URI, sslmode='require')

# Initialize OpenSearch connection
opensearch_conn = OpenSearch(OPENSEARCH_URI)

# Ensure tables exist
def _ensure_table_exists() -> None:
    """Create relational tables expected by the consumer if they are missing."""
    cursor = pg_conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS website_events (
            id SERIAL PRIMARY KEY,
            event_type TEXT,
            user_id TEXT,
            timestamp TEXT,
            ip TEXT,
            device TEXT,
            success BOOLEAN
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS session_metrics (
            session_id UUID PRIMARY KEY,
            user_id TEXT NOT NULL,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            duration_seconds INTEGER NOT NULL,
            total_events INTEGER NOT NULL,
            page_views INTEGER NOT NULL,
            clicks INTEGER NOT NULL,
            successful_logins INTEGER NOT NULL,
            failed_logins INTEGER NOT NULL,
            logout_occurred BOOLEAN NOT NULL,
            last_event JSONB
        )
        """
    )
    # Commit changes
    pg_conn.commit()
    # Close cursor
    cursor.close()



# Ensure OpenSearch index exists
def _ensure_index_exists() -> None:
    """Create the OpenSearch index used for session metrics if it does not exist."""
    if not opensearch_conn.indices.exists(index='session-metrics'):
        opensearch_conn.indices.create(index='session-metrics')

# Index session metrics
def _index_session_metrics(session_state: Dict[str, Any], duration: int) -> None:
    """Write the finalized session document into the OpenSearch index."""
    metrics_doc = {
        'session_id': str(session_state['session_id']),
        'user_id': session_state['user_id'],
        'start_time': session_state['start_time'].isoformat(),
        'end_time': session_state['last_event_time'].isoformat(),
        'duration_seconds': duration,
        'total_events': session_state['total_events'],
        'page_views': session_state['page_views'],
        'clicks': session_state['clicks'],
        'successful_logins': session_state['successful_logins'],
        'failed_logins': session_state['failed_logins'],
        'logout_occurred': session_state['logout_occurred'],
        'last_event': session_state['last_event'],
    }
    opensearch_conn.index(index='session-metrics', id=metrics_doc['session_id'], body=metrics_doc)

# Parse event timestamp
def _parse_event_timestamp(event: Dict[str, Any]) -> Optional[datetime]:
    """Return a timezone-aware timestamp derived from the event payload."""
    raw_timestamp = event.get('timestamp')
    if not raw_timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(raw_timestamp)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

# Start new session
def _start_new_session(user_id: str, event_time: datetime) -> Dict[str, Any]:
    """Initialize an in-memory session tracker for the supplied user."""
    session_state = {
        'session_id': uuid.uuid4(),
        'user_id': user_id,
        'start_time': event_time,
        'last_event_time': event_time,
        'total_events': 0,
        'page_views': 0,
        'clicks': 0,
        'successful_logins': 0,
        'failed_logins': 0,
        'logout_occurred': False,
        'last_event': None,
    }
    _active_sessions[user_id] = session_state
    return session_state

# Finalize session when it expires or when the session is completed
def _finalize_session(user_id: str) -> None:
    """Persist accumulated session metrics for a user and clear cached state."""
    session_state = _active_sessions.pop(user_id, None)
    if not session_state:
        return

    duration = int((session_state['last_event_time'] - session_state['start_time']).total_seconds())
    cursor = pg_conn.cursor()
    cursor.execute(
        """
        INSERT INTO session_metrics (
            session_id,
            user_id,
            start_time,
            end_time,
            duration_seconds,
            total_events,
            page_views,
            clicks,
            successful_logins,
            failed_logins,
            logout_occurred,
            last_event
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        """,
        (
            str(session_state['session_id']),
            session_state['user_id'],
            session_state['start_time'],
            session_state['last_event_time'],
            duration,
            session_state['total_events'],
            session_state['page_views'],
            session_state['clicks'],
            session_state['successful_logins'],
            session_state['failed_logins'],
            session_state['logout_occurred'],
            Json(session_state['last_event']) if session_state['last_event'] is not None else None,
        ),
    )
    pg_conn.commit()
    cursor.close()

    _index_session_metrics(session_state, duration)


def _update_session_metrics(session_state: Dict[str, Any], event: Dict[str, Any], event_time: datetime) -> None:
    """Fold an event into the session aggregates that will be written downstream."""
    session_state['total_events'] += 1
    session_state['last_event_time'] = event_time
    event_type = event.get('event_type')
    if event_type == 'page_view':
        session_state['page_views'] += 1
    elif event_type == 'click':
        session_state['clicks'] += 1
    elif event_type == 'login':
        if event.get('success'):
            session_state['successful_logins'] += 1
        else:
            session_state['failed_logins'] += 1
    elif event_type == 'logout':
        session_state['logout_occurred'] = True

    session_state['last_event'] = event


def _handle_session(event: Dict[str, Any]) -> None:
    """Update user session state and finalize sessions when boundaries are hit."""
    user_id = event.get('user_id')
    if not user_id:
        return

    event_time = _parse_event_timestamp(event)
    if event_time is None:
        return

    # Finalize any sessions that have expired before handling this event
    _finalize_expired_sessions(event_time)

    session_state = _active_sessions.get(user_id)
    if session_state:
        if event_time - session_state['last_event_time'] > SESSION_TIMEOUT or event.get('event_type') == 'login':
            _finalize_session(user_id)
            session_state = None

    if session_state is None:
        session_state = _start_new_session(user_id, event_time)

    _update_session_metrics(session_state, event, event_time)

    if event.get('event_type') == 'logout':
        _finalize_session(user_id)


def _finalize_expired_sessions(reference_time: datetime) -> None:
    """Close any sessions that have exceeded the configured inactivity timeout."""
    expired_users = [
        uid
        for uid, state in _active_sessions.items()
        if reference_time - state['last_event_time'] > SESSION_TIMEOUT
    ]
    for uid in expired_users:
        _finalize_session(uid)


def _finalize_all_sessions() -> None:
    """Flush any remaining session state prior to shutting down the consumer."""
    remaining_users = list(_active_sessions.keys())
    for uid in remaining_users:
        _finalize_session(uid)

# Process events
def main() -> None:
    """Run the consumer loop, persisting raw events and derived metrics."""
    _ensure_table_exists()
    _ensure_index_exists()
    try:
        for message in consumer:
            event = message.value

            cursor = pg_conn.cursor()
            cursor.execute(
                "INSERT INTO website_events (event_type, user_id, timestamp, ip, device, success) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    event.get('event_type'),
                    event.get('user_id'),
                    event.get('timestamp'),
                    event.get('ip'),
                    event.get('device'),
                    event.get('success'),
                ),
            )
            pg_conn.commit()
            cursor.close()

            _handle_session(event)

            opensearch_conn.index(index='website-events', body=event)

            print(event)
    except KeyboardInterrupt:
        print('Stopping consumer...')
    finally:
        _finalize_all_sessions()
        consumer.close()
        pg_conn.close()
        opensearch_conn.close()


if __name__ == '__main__':
    main()

