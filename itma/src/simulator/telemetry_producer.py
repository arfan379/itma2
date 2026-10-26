"""
ITMA - Telemetry Producer (telemetry_producer.py)
Generates traffic speed events per road segment and publishes to Kafka topic 'transport-telemetry'.
Produces peak-hour congestion patterns correlated with public transit routes.
"""

import os
import csv
import json
import yaml
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("telemetry_producer")

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DEFAULT_SEED_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "transform", "seeds", "route_segments.csv"
)


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["simulator"]


def load_unique_segments(seed_path: str = DEFAULT_SEED_PATH) -> List[str]:
    """Loads distinct segment IDs from route_segments.csv seed file."""
    if not os.path.exists(seed_path):
        logger.warning("Seed path not found: %s. Using default segments.", seed_path)
        return [
            "SEG-SUDIRMAN-01", "SEG-SUDIRMAN-02", "SEG-THAMRIN-01",
            "SEG-GAJAHMADA-01", "SEG-GATSU-01", "SEG-GATSU-02",
            "SEG-RASUNA-01", "SEG-MAMPANG-01", "SEG-DAANMOGOT-01"
        ]
    segments = set()
    with open(seed_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            seg = row.get("segment_id")
            if seg:
                segments.add(seg.strip())
    return sorted(list(segments))


def is_peak_hour(hour: int, peak_config: Dict[str, List[int]]) -> bool:
    morning = peak_config.get("morning", [6, 9])
    evening = peak_config.get("evening", [17, 20])
    return (morning[0] <= hour < morning[1]) or (evening[0] <= hour < evening[1])


def generate_traffic_events(
    service_date: str,
    segments: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Generates synthetic traffic telemetry records for all segments across 24 hours."""
    if config is None:
        config = load_config()
    if segments is None:
        segments = load_unique_segments()

    seed = int(hashlib.md5(f"traffic_{service_date}".encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)

    traffic_cfg = config.get("traffic", {})
    peak_cfg = config.get("peak_hours", {})
    base_free_flow = traffic_cfg.get("free_flow_speed_kmh", 50.0)
    peak_reduction = traffic_cfg.get("peak_speed_reduction_ratio", 0.55)
    off_peak_reduction = traffic_cfg.get("off_peak_speed_reduction_ratio", 0.15)
    interval_minutes = traffic_cfg.get("event_interval_minutes", 15)

    date_dt = datetime.strptime(service_date, "%Y-%m-%d")
    events = []

    # Operating time slices from 05:00 to 23:00 WIB
    for minute_offset in range(5 * 60, 23 * 60 + 1, interval_minutes):
        current_time = date_dt + timedelta(minutes=minute_offset)
        hour = current_time.hour
        in_peak = is_peak_hour(hour, peak_cfg)

        for seg in segments:
            # Segment intrinsic speed factor
            seg_factor = 1.0 + (int(hashlib.md5(seg.encode()).hexdigest()[:4], 16) % 15 - 7) / 100.0
            free_flow = max(30.0, float(base_free_flow * seg_factor))

            if in_peak:
                # Severe speed drop during peak hours (congestion score 7-10)
                reduction = rng.uniform(peak_reduction - 0.15, peak_reduction + 0.25)
            else:
                # Mild variations off-peak (congestion score 1-4)
                reduction = rng.uniform(0.05, off_peak_reduction + 0.10)

            reduction = max(0.05, min(0.90, reduction))
            actual_speed = round(free_flow * (1.0 - reduction) + rng.normal(0, 1.5), 1)
            actual_speed = max(5.0, min(free_flow, actual_speed))

            # Format ISO with WIB offset (+07:00)
            event_iso = current_time.strftime("%Y-%m-%d %H:%M:%S+07")

            events.append({
                "segment_id": seg,
                "event_time": event_iso,
                "speed_kmh": actual_speed,
                "free_flow_speed_kmh": round(free_flow, 1),
                "is_synthetic": True
            })

    return events


def produce_to_kafka(
    events: List[Dict[str, Any]],
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None
) -> int:
    """Publishes traffic telemetry events to Kafka."""
    if not bootstrap_servers:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    if not topic:
        topic = os.getenv("KAFKA_TOPIC", "transport-telemetry")

    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            retries=3
        )
        for ev in events:
            producer.send(topic, key=ev["segment_id"], value=ev)
        producer.flush()
        producer.close()
        logger.info("Successfully produced %d events to Kafka topic '%s'", len(events), topic)
        return len(events)
    except Exception as e:
        logger.error("Failed to publish to Kafka (%s): %s", bootstrap_servers, e)
        raise


def run_telemetry_simulation(
    service_date: Optional[str] = None,
    publish_kafka: bool = True
) -> Dict[str, Any]:
    """Generates telemetry events and optionally pushes to Kafka."""
    start_time = datetime.now()
    if not service_date:
        service_date = datetime.now().strftime("%Y-%m-%d")

    events = generate_traffic_events(service_date)
    produced = 0
    if publish_kafka:
        try:
            produced = produce_to_kafka(events)
        except Exception as e:
            logger.warning("Kafka production skipped or failed (%s). Continuing...", e)

    duration = (datetime.now() - start_time).total_seconds()
    metrics = {
        "service_date": service_date,
        "total_events": len(events),
        "produced_to_kafka": produced,
        "duration_seconds": duration
    }
    logger.info("Traffic simulation finished. Total events: %d (in %.2fs)", len(events), duration)
    return metrics


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ITMA Telemetry Producer")
    parser.add_argument("--date", help="Service date (YYYY-MM-DD)")
    parser.add_argument("--no-kafka", action="store_true", help="Skip sending to Kafka")
    args = parser.parse_args()

    run_telemetry_simulation(service_date=args.date, publish_kafka=not args.no_kafka)
