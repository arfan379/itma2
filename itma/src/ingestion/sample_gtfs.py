"""
ITMA - Sample GTFS Builder (sample_gtfs.py)

Membuat feed GTFS *contoh* (sintetis, BUKAN data resmi TransJakarta) dalam bentuk zip.
Dipakai oleh CI dan pengembangan lokal supaya tabel Bronze raw_gtfs_* terisi dan
tes referensial dbt (relationships) bisa dijalankan tanpa internet.

Pemakaian:
    python -m src.ingestion.sample_gtfs --out /tmp/sample_gtfs.zip
    python src/ingestion/api_extractor.py --zip-path /tmp/sample_gtfs.zip
"""

import io
import os
import zipfile
import logging
from typing import Any, Dict, List, Optional

from src.simulator.delay_simulator import generate_fallback_schedules

logger = logging.getLogger("sample_gtfs")

SAMPLE_FEED_VERSION = "ITMA_SAMPLE_v1"


def _to_csv(header: List[str], rows: List[List[Any]]) -> str:
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    return "\n".join(lines) + "\n"


def build_sample_gtfs_zip(routes: Optional[List[str]] = None) -> bytes:
    """Builds an in-memory GTFS zip consistent with the simulator's fallback schedules."""
    schedules: List[Dict[str, Any]] = generate_fallback_schedules(routes)

    route_ids = sorted({s["route_id"] for s in schedules})
    routes_rows = [[r, r.replace("TJ-", ""), f"Sample Route {r}", 3] for r in route_ids]

    stop_ids = sorted({s["stop_id"] for s in schedules})
    stops_rows = [
        [sid, sid, round(-6.20 + i * 0.001, 6), round(106.82 + i * 0.001, 6)]
        for i, sid in enumerate(stop_ids)
    ]

    trips: Dict[str, str] = {}
    for s in schedules:
        trips.setdefault(s["trip_id"], s["route_id"])
    trips_rows = [[tid, rid, "DAILY", f"Sample {rid}", 0] for tid, rid in sorted(trips.items())]

    stop_times_rows = [
        [s["trip_id"], s["stop_sequence"], s["stop_id"], s["arrival_time"], s["arrival_time"]]
        for s in schedules
    ]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("routes.txt", _to_csv(
            ["route_id", "route_short_name", "route_long_name", "route_type"], routes_rows))
        zf.writestr("stops.txt", _to_csv(
            ["stop_id", "stop_name", "stop_lat", "stop_lon"], stops_rows))
        zf.writestr("trips.txt", _to_csv(
            ["trip_id", "route_id", "service_id", "trip_headsign", "direction_id"], trips_rows))
        zf.writestr("stop_times.txt", _to_csv(
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"], stop_times_rows))
        zf.writestr("feed_info.txt", _to_csv(
            ["feed_publisher_name", "feed_version"], [["ITMA Sample (synthetic)", SAMPLE_FEED_VERSION]]))
    return buf.getvalue()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="ITMA sample GTFS builder (synthetic)")
    parser.add_argument("--out", default="data/sample_gtfs.zip", help="Output zip path")
    args = parser.parse_args()

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "wb") as f:
        f.write(build_sample_gtfs_zip())
    logger.info("Sample GTFS written to %s", args.out)
