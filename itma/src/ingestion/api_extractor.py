"""
ITMA - GTFS Extractor (api_extractor.py)
Downloads and loads TransJakarta GTFS feed into PostgreSQL Bronze tables.
Handles >24:00:00 time formats and provides idempotent batch upserts.
"""

import os
import io
import csv
import zipfile
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import requests
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("api_extractor")

REQUIRED_FILES = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]
DEFAULT_FEED_URL = os.getenv(
    "GTFS_FEED_URL",
    "https://gtfs.transjakarta.co.id/files/file_gtfs.zip"
)


def get_db_connection(db_url: Optional[str] = None):
    """Establishes connection to PostgreSQL/TimescaleDB."""
    if db_url:
        return psycopg2.connect(db_url)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "itma_user"),
        password=os.getenv("POSTGRES_PASSWORD", "itma_password"),
        dbname=os.getenv("POSTGRES_DB", "itma_db"),
    )


def download_gtfs_zip(url: str = DEFAULT_FEED_URL, timeout: int = 60) -> bytes:
    """Downloads GTFS zip content from remote source."""
    logger.info("Downloading GTFS feed from %s ...", url)
    headers = {"User-Agent": "ITMA-DataPipeline/1.0"}
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    logger.info("Downloaded %d bytes", len(response.content))
    return response.content


def validate_gtfs_archive(zf: zipfile.ZipFile) -> List[str]:
    """Validates that all mandatory GTFS text files exist in zip archive."""
    namelist = zf.namelist()
    # Normalize paths inside zip (some feeds put files in a subfolder)
    base_names = [os.path.basename(name) for name in namelist]
    missing = [f for f in REQUIRED_FILES if f not in base_names]
    if missing:
        raise ValueError(f"GTFS archive is missing required files: {missing}")
    return namelist


def extract_feed_version(zf: zipfile.ZipFile, raw_bytes: bytes) -> str:
    """Extracts feed version from feed_info.txt or hashes content."""
    for name in zf.namelist():
        if os.path.basename(name) == "feed_info.txt":
            with zf.open(name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    version = row.get("feed_version")
                    if version:
                        return version.strip()
    # Fallback to date + content hash
    today = datetime.now().strftime("%Y%m%d")
    content_hash = hashlib.md5(raw_bytes[:4096]).hexdigest()[:8]
    return f"feed_{today}_{content_hash}"


def sanitize_time_str(time_str: Optional[str]) -> str:
    """
    Validates and formats GTFS time string.
    GTFS times can be >24:00:00 (e.g., 25:10:00).
    Returns normalized 'HH:MM:SS' string.
    """
    if not time_str:
        return "00:00:00"
    cleaned = time_str.strip()
    parts = cleaned.split(":")
    if len(parts) == 3:
        # Check that parts are numeric
        try:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
            return f"{h:02d}:{m:02d}:{s:02d}"
        except ValueError:
            pass
    elif len(parts) == 2:
        try:
            h, m = int(parts[0]), int(parts[1])
            return f"{h:02d}:{m:02d}:00"
        except ValueError:
            pass
    return cleaned


def parse_csv_from_zip(zf: zipfile.ZipFile, target_filename: str) -> List[Dict[str, Any]]:
    """Finds target_filename inside zip (handling subfolders) and parses CSV."""
    for name in zf.namelist():
        if os.path.basename(name) == target_filename:
            with zf.open(name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                return [row for row in reader]
    return []


def upsert_routes(cursor, rows: List[Dict[str, Any]], feed_version: str) -> Tuple[int, int]:
    sql = """
    INSERT INTO raw.raw_gtfs_routes (
        route_id, route_short_name, route_long_name, route_type, feed_version, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (route_id) DO UPDATE SET
        route_short_name = EXCLUDED.route_short_name,
        route_long_name = EXCLUDED.route_long_name,
        route_type = EXCLUDED.route_type,
        feed_version = EXCLUDED.feed_version,
        ingested_at = NOW();
    """
    valid_data = []
    rejected = 0
    for r in rows:
        route_id = r.get("route_id")
        if not route_id:
            rejected += 1
            continue
        try:
            route_type = int(r.get("route_type", 3))
        except (ValueError, TypeError):
            route_type = 3
        valid_data.append((
            route_id.strip(),
            r.get("route_short_name", "").strip(),
            r.get("route_long_name", "").strip(),
            route_type,
            feed_version
        ))
    if valid_data:
        execute_batch(cursor, sql, valid_data, page_size=1000)
    return len(valid_data), rejected


def upsert_stops(cursor, rows: List[Dict[str, Any]], feed_version: str) -> Tuple[int, int]:
    sql = """
    INSERT INTO raw.raw_gtfs_stops (
        stop_id, stop_name, stop_lat, stop_lon, feed_version, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (stop_id) DO UPDATE SET
        stop_name = EXCLUDED.stop_name,
        stop_lat = EXCLUDED.stop_lat,
        stop_lon = EXCLUDED.stop_lon,
        feed_version = EXCLUDED.feed_version,
        ingested_at = NOW();
    """
    valid_data = []
    rejected = 0
    for r in rows:
        stop_id = r.get("stop_id")
        if not stop_id:
            rejected += 1
            continue
        try:
            lat = float(r.get("stop_lat", 0.0))
            lon = float(r.get("stop_lon", 0.0))
        except (ValueError, TypeError):
            lat, lon = 0.0, 0.0
        valid_data.append((
            stop_id.strip(),
            r.get("stop_name", "").strip(),
            lat,
            lon,
            feed_version
        ))
    if valid_data:
        execute_batch(cursor, sql, valid_data, page_size=1000)
    return len(valid_data), rejected


def upsert_trips(cursor, rows: List[Dict[str, Any]], feed_version: str) -> Tuple[int, int]:
    sql = """
    INSERT INTO raw.raw_gtfs_trips (
        trip_id, route_id, service_id, trip_headsign, direction_id, feed_version, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (trip_id) DO UPDATE SET
        route_id = EXCLUDED.route_id,
        service_id = EXCLUDED.service_id,
        trip_headsign = EXCLUDED.trip_headsign,
        direction_id = EXCLUDED.direction_id,
        feed_version = EXCLUDED.feed_version,
        ingested_at = NOW();
    """
    valid_data = []
    rejected = 0
    for r in rows:
        trip_id = r.get("trip_id")
        route_id = r.get("route_id")
        if not trip_id or not route_id:
            rejected += 1
            continue
        try:
            direction_id = int(r.get("direction_id", 0))
        except (ValueError, TypeError):
            direction_id = 0
        valid_data.append((
            trip_id.strip(),
            route_id.strip(),
            r.get("service_id", "").strip(),
            r.get("trip_headsign", "").strip(),
            direction_id,
            feed_version
        ))
    if valid_data:
        execute_batch(cursor, sql, valid_data, page_size=1000)
    return len(valid_data), rejected


def upsert_stop_times(cursor, rows: List[Dict[str, Any]], feed_version: str) -> Tuple[int, int]:
    sql = """
    INSERT INTO raw.raw_gtfs_stop_times (
        trip_id, stop_sequence, stop_id, arrival_time, departure_time, feed_version, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (trip_id, stop_sequence) DO UPDATE SET
        stop_id = EXCLUDED.stop_id,
        arrival_time = EXCLUDED.arrival_time,
        departure_time = EXCLUDED.departure_time,
        feed_version = EXCLUDED.feed_version,
        ingested_at = NOW();
    """
    valid_data = []
    rejected = 0
    for r in rows:
        trip_id = r.get("trip_id")
        stop_id = r.get("stop_id")
        seq_str = r.get("stop_sequence")
        if not trip_id or not stop_id or seq_str is None:
            rejected += 1
            continue
        try:
            stop_sequence = int(seq_str)
        except (ValueError, TypeError):
            rejected += 1
            continue

        arrival = sanitize_time_str(r.get("arrival_time"))
        departure = sanitize_time_str(r.get("departure_time") or arrival)

        valid_data.append((
            trip_id.strip(),
            stop_sequence,
            stop_id.strip(),
            arrival,
            departure,
            feed_version
        ))
    if valid_data:
        execute_batch(cursor, sql, valid_data, page_size=2000)
    return len(valid_data), rejected


def run_gtfs_ingestion(
    zip_bytes: Optional[bytes] = None,
    zip_path: Optional[str] = None,
    feed_url: str = DEFAULT_FEED_URL,
    db_conn=None
) -> Dict[str, Any]:
    """
    Main orchestration function for GTFS extraction and loading.
    Returns observability metrics.
    """
    start_time = datetime.now()

    if zip_bytes is None:
        if zip_path and os.path.exists(zip_path):
            with open(zip_path, "rb") as f:
                zip_bytes = f.read()
        else:
            zip_bytes = download_gtfs_zip(feed_url)

    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    validate_gtfs_archive(zf)
    feed_version = extract_feed_version(zf, zip_bytes)
    logger.info("Processing GTFS feed version: %s", feed_version)

    routes_data = parse_csv_from_zip(zf, "routes.txt")
    stops_data = parse_csv_from_zip(zf, "stops.txt")
    trips_data = parse_csv_from_zip(zf, "trips.txt")
    stop_times_data = parse_csv_from_zip(zf, "stop_times.txt")

    close_conn_at_end = False
    if db_conn is None:
        db_conn = get_db_connection()
        close_conn_at_end = True

    metrics = {
        "feed_version": feed_version,
        "routes": {"total": len(routes_data), "loaded": 0, "rejected": 0},
        "stops": {"total": len(stops_data), "loaded": 0, "rejected": 0},
        "trips": {"total": len(trips_data), "loaded": 0, "rejected": 0},
        "stop_times": {"total": len(stop_times_data), "loaded": 0, "rejected": 0},
        "duration_seconds": 0.0
    }

    try:
        with db_conn.cursor() as cur:
            # 1. Routes
            loaded, rej = upsert_routes(cur, routes_data, feed_version)
            metrics["routes"]["loaded"] = loaded
            metrics["routes"]["rejected"] = rej

            # 2. Stops
            loaded, rej = upsert_stops(cur, stops_data, feed_version)
            metrics["stops"]["loaded"] = loaded
            metrics["stops"]["rejected"] = rej

            # 3. Trips
            loaded, rej = upsert_trips(cur, trips_data, feed_version)
            metrics["trips"]["loaded"] = loaded
            metrics["trips"]["rejected"] = rej

            # 4. Stop Times
            loaded, rej = upsert_stop_times(cur, stop_times_data, feed_version)
            metrics["stop_times"]["loaded"] = loaded
            metrics["stop_times"]["rejected"] = rej

        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        logger.error("Error during GTFS database loading: %s", e)
        raise
    finally:
        if close_conn_at_end:
            db_conn.close()

    duration = (datetime.now() - start_time).total_seconds()
    metrics["duration_seconds"] = duration
    logger.info(
        "GTFS ingestion completed in %.2fs. Routes: %d, Stops: %d, Trips: %d, StopTimes: %d",
        duration,
        metrics["routes"]["loaded"],
        metrics["stops"]["loaded"],
        metrics["trips"]["loaded"],
        metrics["stop_times"]["loaded"],
    )
    return metrics


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ITMA GTFS Extractor")
    parser.add_argument("--zip-path", help="Path to local GTFS zip file")
    parser.add_argument("--url", default=DEFAULT_FEED_URL, help="Remote GTFS URL")
    args = parser.parse_args()

    run_gtfs_ingestion(zip_path=args.zip_path, feed_url=args.url)
