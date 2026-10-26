"""
Unit tests for ITMA Data Simulators (tests/test_simulator.py)
"""

from src.simulator.delay_simulator import (
    compute_date_seed,
    is_peak_hour,
    parse_gtfs_time_to_seconds,
    simulate_delays,
    generate_fallback_schedules,
    load_config
)
from src.simulator.telemetry_producer import generate_traffic_events
from src.ingestion.kafka_consumer import validate_and_transform_event


def test_seed_reproducibility():
    """Identical service date and seed_base must yield identical seeds."""
    seed1 = compute_date_seed(42, "2026-10-26")
    seed2 = compute_date_seed(42, "2026-10-26")
    seed3 = compute_date_seed(42, "2026-10-27")
    assert seed1 == seed2
    assert seed1 != seed3


def test_is_peak_hour_classification():
    peak_cfg = {"morning": [6, 9], "evening": [17, 20]}
    # Morning peak (06:00 to 08:59)
    assert is_peak_hour(6, peak_cfg) is True
    assert is_peak_hour(7, peak_cfg) is True
    assert is_peak_hour(8, peak_cfg) is True
    assert is_peak_hour(9, peak_cfg) is False  # 09:00 is off-peak

    # Evening peak (17:00 to 19:59)
    assert is_peak_hour(16, peak_cfg) is False
    assert is_peak_hour(17, peak_cfg) is True
    assert is_peak_hour(18, peak_cfg) is True
    assert is_peak_hour(19, peak_cfg) is True
    assert is_peak_hour(20, peak_cfg) is False  # 20:00 is off-peak


def test_gtfs_time_to_seconds():
    assert parse_gtfs_time_to_seconds("08:30:00") == 8 * 3600 + 30 * 60
    assert parse_gtfs_time_to_seconds("25:10:00") == 25 * 3600 + 10 * 60


def test_simulate_delays_deterministic():
    """Running simulation twice with same service date must generate identical output."""
    schedules = generate_fallback_schedules(["TJ-1"])
    cfg = load_config()

    run1 = simulate_delays(schedules, "2026-10-26", cfg)
    run2 = simulate_delays(schedules, "2026-10-26", cfg)

    assert len(run1) == len(run2)
    # Check that actual arrival time matches across both runs
    for r1, r2 in zip(run1, run2):
        assert r1[0] == r2[0]  # trip_id
        assert r1[2] == r2[2]  # stop_id
        assert r1[5] == r2[5]  # actual_arrival_time


def test_simulate_delays_peak_vs_offpeak():
    """Rush hour trips should statistically experience longer delays."""
    schedules = [
        # Morning peak trip at 07:30
        {"trip_id": "T_PEAK", "route_id": "TJ-1", "stop_id": "S1", "arrival_time": "07:30:00", "stop_sequence": 1},
        # Off-peak trip at 12:30
        {"trip_id": "T_OFF", "route_id": "TJ-1", "stop_id": "S1", "arrival_time": "12:30:00", "stop_sequence": 1},
    ] * 200  # Repeat sample to test distribution

    cfg = load_config()
    results = simulate_delays(schedules, "2026-10-26", cfg)

    from datetime import datetime
    peak_delays = []
    off_delays = []
    for item in results:
        trip_id, _, _, _, sched_time, actual_iso, _ = item
        sched_dt = datetime.strptime(f"2026-10-26 {sched_time}", "%Y-%m-%d %H:%M:%S")
        actual_dt = datetime.strptime(actual_iso[:19], "%Y-%m-%d %H:%M:%S")
        delay_min = (actual_dt - sched_dt).total_seconds() / 60.0

        if trip_id == "T_PEAK":
            peak_delays.append(delay_min)
        else:
            off_delays.append(delay_min)

    assert sum(peak_delays) / len(peak_delays) > sum(off_delays) / len(off_delays)


def test_traffic_event_generation():
    events = generate_traffic_events("2026-10-26", segments=["SEG-SUDIRMAN-01", "SEG-THAMRIN-01"])
    assert len(events) > 0
    sample = events[0]
    assert "segment_id" in sample
    assert "speed_kmh" in sample
    assert "free_flow_speed_kmh" in sample
    assert sample["speed_kmh"] > 0
    assert sample["is_synthetic"] is True


def test_kafka_event_validation():
    valid_event = {
        "segment_id": "SEG-001",
        "event_time": "2026-10-26 08:00:00+07",
        "speed_kmh": 22.5,
        "free_flow_speed_kmh": 50.0,
        "is_synthetic": True
    }
    parsed = validate_and_transform_event(valid_event)
    assert parsed is not None
    assert parsed[0] == "SEG-001"
    assert parsed[2] == 22.5

    # Invalid event missing segment_id
    invalid_event = {
        "event_time": "2026-10-26 08:00:00+07",
        "speed_kmh": 22.5,
        "free_flow_speed_kmh": 50.0
    }
    assert validate_and_transform_event(invalid_event) is None
