"""
ITMA - Delay Simulator (delay_simulator.py)
Generates synthetic actual_arrival_time for scheduled GTFS trips.
Features peak-hour degradation, incident spikes, reproducible seeds, and idempotent database loading.
"""

import os
import yaml
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("delay_simulator")

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yml")


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["simulator"]


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


def compute_date_seed(base_seed: int, service_date: str) -> int:
    """Computes a deterministic integer seed for a given service date."""
    hash_hex = hashlib.md5(f"{base_seed}_{service_date}".encode()).hexdigest()[:8]
    return int(hash_hex, 16)


def is_peak_hour(hour: int, peak_config: Dict[str, List[int]]) -> bool:
    """Checks if an hour belongs to morning or evening peak periods."""
    morning = peak_config.get("morning", [6, 9])
    evening = peak_config.get("evening", [17, 20])
    in_morning = morning[0] <= hour < morning[1]
    in_evening = evening[0] <= hour < evening[1]
    return in_morning or in_evening


def parse_gtfs_time_to_seconds(time_str: str) -> int:
    """Converts HH:MM:SS (which can exceed 24:00:00) into total seconds from midnight."""
    parts = time_str.split(":")
    hours = int(parts[0])
    minutes = int(parts[1]) if len(parts) > 1 else 0
    seconds = int(parts[2]) if len(parts) > 2 else 0
    return hours * 3600 + minutes * 60 + seconds


def fetch_gtfs_schedules_from_db(conn) -> List[Dict[str, Any]]:
    """Fetches stop_times joined with trips from raw GTFS tables."""
    sql = """
    SELECT
        st.trip_id,
        t.route_id,
        st.stop_id,
        st.arrival_time,
        st.stop_sequence
    FROM raw.raw_gtfs_stop_times st
    JOIN raw.raw_gtfs_trips t ON st.trip_id = t.trip_id
    ORDER BY st.trip_id, st.stop_sequence;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def generate_fallback_schedules(routes: List[str] = None) -> List[Dict[str, Any]]:
    """Generates synthetic GTFS schedule templates if raw GTFS is not yet populated."""
    if not routes:
        routes = [f"TJ-{i}" for i in range(1, 14)]
    schedules = []
    # Create 8 trips per route throughout the day
    departure_hours = [6, 7, 8, 12, 15, 17, 18, 20]
    for r_idx, route in enumerate(routes):
        for t_idx, d_hour in enumerate(departure_hours):
            trip_id = f"TRIP_{route}_{t_idx+1:02d}"
            # 5 stops per trip
            for seq in range(1, 6):
                stop_id = f"STOP_{route}_{seq:02d}"
                sched_seconds = d_hour * 3600 + seq * 600  # 10 min between stops
                h = sched_seconds // 3600
                m = (sched_seconds % 3600) // 60
                s = sched_seconds % 60
                schedules.append({
                    "trip_id": trip_id,
                    "route_id": route,
                    "stop_id": stop_id,
                    "arrival_time": f"{h:02d}:{m:02d}:{s:02d}",
                    "stop_sequence": seq
                })
    return schedules


def simulate_delays(
    schedules: List[Dict[str, Any]],
    service_date: str,
    config: Optional[Dict[str, Any]] = None
) -> List[Tuple[Any, ...]]:
    """
    Simulates actual arrival time for each schedule entry.
    Returns list of tuples ready for database insertion.
    """
    if config is None:
        config = load_config()

    seed = compute_date_seed(config.get("seed_base", 42), service_date)
    rng = np.random.default_rng(seed)

    delay_cfg = config.get("delay", {})
    peak_cfg = config.get("peak_hours", {})
    base_mean = delay_cfg.get("base_mean_minutes", 2.0)
    base_std = delay_cfg.get("base_std_minutes", 2.5)
    peak_mult = delay_cfg.get("peak_multiplier", 2.8)
    incident_prob = delay_cfg.get("incident_probability", 0.03)
    incident_range = delay_cfg.get("incident_delay_range", [15.0, 45.0])
    outlier_prob = delay_cfg.get("outlier_probability", 0.005)
    outlier_min = delay_cfg.get("outlier_min_minutes", -35.0)

    # Base date as datetime at 00:00:00 (WIB: UTC+7)
    date_dt = datetime.strptime(service_date, "%Y-%m-%d")

    records = []
    # Track accumulated delay per trip to maintain realistic causality across stops
    trip_accumulated_delay: Dict[str, float] = {}

    for item in schedules:
        trip_id = item["trip_id"]
        route_id = item["route_id"]
        stop_id = item["stop_id"]
        sched_time_str = item["arrival_time"]

        sched_seconds = parse_gtfs_time_to_seconds(sched_time_str)
        sched_hour = (sched_seconds // 3600) % 24

        is_peak = is_peak_hour(sched_hour, peak_cfg)

        # Baseline delay distribution
        if is_peak:
            mean = base_mean * peak_mult
            std = base_std * peak_mult * 0.8
        else:
            mean = base_mean
            std = base_std

        # Draw incremental delay for current segment
        incr_delay = rng.normal(loc=mean, scale=std)

        # Carry forward previous stops delay with slight decay
        prev_delay = trip_accumulated_delay.get(trip_id, 0.0)
        curr_delay = prev_delay * 0.7 + incr_delay

        # Incident injection
        if rng.random() < incident_prob:
            curr_delay += rng.uniform(incident_range[0], incident_range[1])

        # Negative outlier injection (< -30 mins)
        if rng.random() < outlier_prob:
            curr_delay = rng.uniform(outlier_min, -30.5)

        trip_accumulated_delay[trip_id] = curr_delay

        # Calculate actual timestamp
        sched_dt = date_dt + timedelta(seconds=sched_seconds)
        actual_dt = sched_dt + timedelta(minutes=curr_delay)
        # Format actual_dt as ISO string with Asia/Jakarta offset (+07:00)
        actual_iso = actual_dt.strftime("%Y-%m-%d %H:%M:%S+07")

        records.append((
            trip_id,
            route_id,
            stop_id,
            service_date,
            sched_time_str,
            actual_iso,
            True  # is_synthetic
        ))

    return records


def run_delay_simulation(
    service_date: Optional[str] = None,
    db_conn=None,
    config_path: str = DEFAULT_CONFIG_PATH
) -> Dict[str, Any]:
    """Runs delay simulation and loads into raw.raw_schedules."""
    start_time = datetime.now()
    if not service_date:
        service_date = datetime.now().strftime("%Y-%m-%d")

    config = load_config(config_path)

    close_conn = False
    if db_conn is None:
        try:
            db_conn = get_db_connection()
            close_conn = True
        except Exception as e:
            logger.warning("Could not connect to database (%s). Using offline mode.", e)
            db_conn = None

    if db_conn:
        schedules = fetch_gtfs_schedules_from_db(db_conn)
        if not schedules:
            logger.info("raw_gtfs_stop_times is empty, using fallback TransJakarta schedules.")
            schedules = generate_fallback_schedules()
    else:
        schedules = generate_fallback_schedules()

    simulated_records = simulate_delays(schedules, service_date, config)

    inserted = 0
    if db_conn:
        sql = """
        INSERT INTO raw.raw_schedules (
            trip_id, route_id, stop_id, service_date,
            scheduled_arrival_time, actual_arrival_time,
            is_synthetic, ingested_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (trip_id, stop_id, service_date) DO UPDATE SET
            route_id = EXCLUDED.route_id,
            scheduled_arrival_time = EXCLUDED.scheduled_arrival_time,
            actual_arrival_time = EXCLUDED.actual_arrival_time,
            is_synthetic = EXCLUDED.is_synthetic,
            ingested_at = NOW();
        """
        try:
            with db_conn.cursor() as cur:
                execute_batch(cur, sql, simulated_records, page_size=2000)
            db_conn.commit()
            inserted = len(simulated_records)
        except Exception as e:
            db_conn.rollback()
            logger.error("Failed to insert simulated schedules: %s", e)
            raise
        finally:
            if close_conn:
                db_conn.close()

    duration = (datetime.now() - start_time).total_seconds()
    metrics = {
        "service_date": service_date,
        "total_records": len(simulated_records),
        "inserted": inserted,
        "duration_seconds": duration
    }
    logger.info("Delay simulation for %s generated %d records in %.2fs", service_date, len(simulated_records), duration)
    return metrics


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ITMA Delay Simulator")
    parser.add_argument("--date", help="Service date (YYYY-MM-DD)")
    args = parser.parse_args()

    run_delay_simulation(service_date=args.date)
