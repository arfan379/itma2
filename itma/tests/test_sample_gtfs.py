"""
Unit tests for the synthetic sample GTFS builder (tests/test_sample_gtfs.py)
"""

import io
import zipfile

from src.ingestion.api_extractor import parse_csv_from_zip, validate_gtfs_archive, extract_feed_version
from src.ingestion.sample_gtfs import build_sample_gtfs_zip, SAMPLE_FEED_VERSION


def _open(raw: bytes) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(raw))


def test_sample_gtfs_is_valid_archive():
    raw = build_sample_gtfs_zip()
    zf = _open(raw)
    validate_gtfs_archive(zf)
    assert extract_feed_version(zf, raw) == SAMPLE_FEED_VERSION


def test_sample_gtfs_row_counts():
    zf = _open(build_sample_gtfs_zip())
    assert len(parse_csv_from_zip(zf, "routes.txt")) == 13
    assert len(parse_csv_from_zip(zf, "trips.txt")) == 13 * 8
    assert len(parse_csv_from_zip(zf, "stop_times.txt")) == 13 * 8 * 5


def test_sample_gtfs_is_referentially_consistent():
    """Every trip must point to an existing route and every stop_time to an existing trip/stop."""
    zf = _open(build_sample_gtfs_zip())
    route_ids = {r["route_id"] for r in parse_csv_from_zip(zf, "routes.txt")}
    trips = parse_csv_from_zip(zf, "trips.txt")
    trip_ids = {t["trip_id"] for t in trips}
    stop_ids = {s["stop_id"] for s in parse_csv_from_zip(zf, "stops.txt")}

    assert all(t["route_id"] in route_ids for t in trips)
    for st in parse_csv_from_zip(zf, "stop_times.txt"):
        assert st["trip_id"] in trip_ids
        assert st["stop_id"] in stop_ids
