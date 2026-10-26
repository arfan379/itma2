"""
Unit tests for GTFS extraction and parsing (tests/test_gtfs_parser.py)
"""

import io
import zipfile
import pytest
from src.ingestion.api_extractor import (
    sanitize_time_str,
    validate_gtfs_archive,
    extract_feed_version,
    parse_csv_from_zip
)


def create_mock_gtfs_zip(include_all: bool = True, custom_stop_times: str = "") -> bytes:
    """Helper creating an in-memory zip archive mimicking GTFS structure."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        if include_all:
            zf.writestr(
                "routes.txt",
                "route_id,route_short_name,route_long_name,route_type\nTJ-1,1,Blok M - Kota,3\n"
            )
            zf.writestr("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nSTOP-01,Blok M,-6.243,106.798\n")
            zf.writestr(
                "trips.txt",
                "trip_id,route_id,service_id,trip_headsign,direction_id\nTRIP-1,TJ-1,DAILY,Kota,0\n"
            )
            st_content = custom_stop_times or (
                "trip_id,stop_sequence,stop_id,arrival_time,departure_time\n"
                "TRIP-1,1,STOP-01,08:30:00,08:30:00\n"
            )
            zf.writestr("stop_times.txt", st_content)
            zf.writestr("feed_info.txt", "feed_publisher_name,feed_version\nTransJakarta,TJ_V1_2026\n")
        else:
            # Missing stop_times.txt
            zf.writestr("routes.txt", "route_id,route_short_name,route_long_name,route_type\nTJ-1,1,Blok M - Kota,3\n")
            zf.writestr("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nSTOP-01,Blok M,-6.243,106.798\n")
    return buf.getvalue()


def test_sanitize_time_normal():
    assert sanitize_time_str("08:30:00") == "08:30:00"
    assert sanitize_time_str(" 8:05:00 ") == "08:05:00"
    assert sanitize_time_str("14:30") == "14:30:00"


def test_sanitize_time_past_midnight():
    """GTFS trips past midnight can have hours >= 24:00:00 (e.g. 25:10:00)."""
    assert sanitize_time_str("25:10:00") == "25:10:00"
    assert sanitize_time_str("26:45:30") == "26:45:30"


def test_validate_gtfs_archive_success():
    raw_zip = create_mock_gtfs_zip(include_all=True)
    zf = zipfile.ZipFile(io.BytesIO(raw_zip))
    namelist = validate_gtfs_archive(zf)
    assert len(namelist) >= 4


def test_validate_gtfs_archive_missing_required():
    raw_zip = create_mock_gtfs_zip(include_all=False)
    zf = zipfile.ZipFile(io.BytesIO(raw_zip))
    with pytest.raises(ValueError, match="missing required files"):
        validate_gtfs_archive(zf)


def test_extract_feed_version():
    raw_zip = create_mock_gtfs_zip(include_all=True)
    zf = zipfile.ZipFile(io.BytesIO(raw_zip))
    version = extract_feed_version(zf, raw_zip)
    assert version == "TJ_V1_2026"


def test_parse_csv_from_zip_handling_gtfs_times():
    custom_st = (
        "trip_id,stop_sequence,stop_id,arrival_time,departure_time\n"
        "TRIP-1,1,STOP-01,25:15:00,25:16:00\n"
        "TRIP-1,2,STOP-02,25:30:00,25:31:00\n"
    )
    raw_zip = create_mock_gtfs_zip(include_all=True, custom_stop_times=custom_st)
    zf = zipfile.ZipFile(io.BytesIO(raw_zip))
    rows = parse_csv_from_zip(zf, "stop_times.txt")
    assert len(rows) == 2
    assert rows[0]["arrival_time"] == "25:15:00"
    assert rows[1]["arrival_time"] == "25:30:00"
