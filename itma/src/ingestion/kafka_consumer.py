"""
ITMA - Kafka Consumer (kafka_consumer.py)
Reads traffic events from topic 'transport-telemetry', validates schemas,
and idempotently writes to PostgreSQL/TimescaleDB raw.raw_traffic.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka_consumer")


def get_db_connection(db_url: Optional[str] = None):
    if db_url:
        return psycopg2.connect(db_url)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "itma_user"),
        password=os.getenv("POSTGRES_PASSWORD", "itma_password"),
        dbname=os.getenv("POSTGRES_DB", "itma_db"),
    )


def validate_and_transform_event(payload: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
    """Validates required schema fields and formats record tuple."""
    segment_id = payload.get("segment_id")
    event_time_str = payload.get("event_time")
    speed_kmh = payload.get("speed_kmh")
    free_flow_speed = payload.get("free_flow_speed_kmh")

    if not segment_id or not event_time_str or speed_kmh is None or free_flow_speed is None:
        return None

    try:
        speed = float(speed_kmh)
        free_flow = float(free_flow_speed)
        is_syn = bool(payload.get("is_synthetic", True))
        return (
            str(segment_id).strip(),
            str(event_time_str).strip(),
            speed,
            free_flow,
            is_syn
        )
    except (ValueError, TypeError):
        return None


def write_batch_to_db(cursor, batch: List[Tuple[Any, ...]]) -> int:
    """Idempotently writes batch of traffic events into raw.raw_traffic."""
    if not batch:
        return 0

    sql = """
    INSERT INTO raw.raw_traffic (
        segment_id, event_time, speed_kmh, free_flow_speed_kmh, is_synthetic, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (segment_id, event_time) DO UPDATE SET
        speed_kmh = EXCLUDED.speed_kmh,
        free_flow_speed_kmh = EXCLUDED.free_flow_speed_kmh,
        is_synthetic = EXCLUDED.is_synthetic,
        ingested_at = NOW();
    """
    execute_batch(cursor, sql, batch, page_size=2000)
    return len(batch)


def consume_messages(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
    max_records: int = 10000,
    poll_timeout_ms: int = 5000,
    db_conn=None
) -> Dict[str, Any]:
    """
    Consumes messages from Kafka until timeout or max_records reached,
    then commits and persists into database.
    """
    if not bootstrap_servers:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    if not topic:
        topic = os.getenv("KAFKA_TOPIC", "transport-telemetry")
    if not group_id:
        group_id = os.getenv("KAFKA_CONSUMER_GROUP", "itma-traffic-consumer")

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=poll_timeout_ms
    )

    close_conn = False
    if db_conn is None:
        db_conn = get_db_connection()
        close_conn = True

    valid_records = []
    rejected_count = 0
    total_polled = 0

    try:
        logger.info("Polling messages from topic '%s'...", topic)
        for msg in consumer:
            total_polled += 1
            record = validate_and_transform_event(msg.value)
            if record:
                valid_records.append(record)
            else:
                rejected_count += 1

            if len(valid_records) >= max_records:
                break

        if valid_records:
            with db_conn.cursor() as cur:
                write_batch_to_db(cur, valid_records)
            db_conn.commit()
            consumer.commit()
            logger.info("Successfully ingested %d traffic events into raw.raw_traffic", len(valid_records))
        else:
            logger.info("No new traffic records found in topic '%s'", topic)

    except Exception as e:
        db_conn.rollback()
        logger.error("Error during traffic ingestion: %s", e)
        raise
    finally:
        consumer.close()
        if close_conn:
            db_conn.close()

    return {
        "polled": total_polled,
        "written": len(valid_records),
        "rejected": rejected_count
    }


def ingest_direct_events(events: List[Dict[str, Any]], db_conn=None) -> Dict[str, int]:
    """Directly ingests a list of telemetry events into raw.raw_traffic (used in Airflow/testing)."""
    close_conn = False
    if db_conn is None:
        db_conn = get_db_connection()
        close_conn = True

    valid_batch = []
    rejected = 0
    for ev in events:
        rec = validate_and_transform_event(ev)
        if rec:
            valid_batch.append(rec)
        else:
            rejected += 1

    written = 0
    try:
        if valid_batch:
            with db_conn.cursor() as cur:
                written = write_batch_to_db(cur, valid_batch)
            db_conn.commit()
    finally:
        if close_conn:
            db_conn.close()

    return {"written": written, "rejected": rejected}


if __name__ == "__main__":
    consume_messages()
