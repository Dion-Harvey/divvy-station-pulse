"""Chicago Station Pulse — scheduled GBFS ingestion pipeline.

Fetch → validate → transform (DuckDB SQL) → emit compact JSON artifacts.
Every run is all-or-nothing: any failed quality gate exits non-zero so the
scheduler surfaces the failure instead of publishing bad data.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "out"
STATUS_URL = "https://gbfs.divvybikes.com/gbfs/en/station_status.json"
INFO_URL = "https://gbfs.divvybikes.com/gbfs/en/station_information.json"
HISTORY_LIMIT = 7 * 24 * 2  # 7 days of 30-minute points


def fetch(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "station-pulse/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def run_quality_gates(status: dict, info: dict) -> list[dict]:
    """Each gate returns a record; any hard-gate failure aborts the run."""
    stations = status["data"]["stations"]
    infos = info["data"]["stations"]
    feed_age_min = (time.time() - status["last_updated"]) / 60
    status_ids = {s["station_id"] for s in stations}
    info_ids = {s["station_id"] for s in infos}
    join_coverage = len(status_ids & info_ids) / max(len(status_ids), 1)
    negatives = sum(
        1 for s in stations
        if s["num_bikes_available"] < 0 or s["num_docks_available"] < 0
    )
    total_bikes = sum(s["num_bikes_available"] for s in stations)

    gates = [
        ("station_count_in_range", 1200 <= len(stations) <= 4000, f"{len(stations)} stations", True),
        ("feed_freshness_under_30min", feed_age_min < 30, f"{feed_age_min:.1f} min old", True),
        ("status_info_join_coverage_99pct", join_coverage >= 0.99, f"{join_coverage:.2%}", True),
        ("no_negative_counts", negatives == 0, f"{negatives} negative rows", True),
        ("citywide_bikes_sane", 500 <= total_bikes <= 60000, f"{total_bikes} bikes", True),
    ]
    results = [
        {"check": name, "passed": ok, "observed": obs, "blocking": blocking}
        for name, ok, obs, blocking in gates
    ]
    failed = [r for r in results if r["blocking"] and not r["passed"]]
    if failed:
        for f in failed:
            print(f"GATE FAILED: {f['check']} ({f['observed']})", file=sys.stderr)
        (OUT / "quality.json").write_text(json.dumps({"passed": False, "checks": results}, indent=1))
        sys.exit(1)
    return results


def main() -> None:
    OUT.mkdir(exist_ok=True)
    t0 = time.time()
    status, info = fetch(STATUS_URL), fetch(INFO_URL)
    checks = run_quality_gates(status, info)

    # stage raw feeds for DuckDB (read_json_auto takes file paths)
    status_raw = OUT / "_status_raw.json"
    info_raw = OUT / "_info_raw.json"
    status_raw.write_text(json.dumps(status["data"]["stations"]))
    info_raw.write_text(json.dumps(info["data"]["stations"]))

    con = duckdb.connect()
    con.execute("CREATE TABLE status AS SELECT * FROM read_json_auto(?)", [status_raw.as_posix()])
    con.execute("CREATE TABLE info AS SELECT * FROM read_json_auto(?)", [info_raw.as_posix()])
    status_raw.unlink()
    info_raw.unlink()
    sql = (ROOT / "pipeline" / "transform.sql").read_text()
    for statement in [s.strip() for s in sql.split(";") if s.strip()]:
        con.execute(statement)

    kpis = con.execute("SELECT * FROM citywide").fetchone()
    top = con.execute("SELECT * FROM top_stations").fetchall()
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    latest = {
        "generated_at": now_iso,
        "feed_last_updated": datetime.fromtimestamp(
            status["last_updated"], timezone.utc
        ).isoformat(timespec="seconds"),
        "stations_online": kpis[0],
        "bikes_available": kpis[1],
        "ebikes_available": kpis[2],
        "docks_available": kpis[3],
        "pct_stations_empty": kpis[4],
        "pct_stations_full": kpis[5],
        "system_capacity_used_pct": kpis[6],
    }
    stations_out = [
        {"name": r[0], "lat": r[1], "lon": r[2], "bikes": r[3], "ebikes": r[4], "docks": r[5], "capacity": r[6]}
        for r in top
    ]

    # rolling history: append this run's citywide point, trim to window
    hist_file = OUT / "history.json"
    history = json.loads(hist_file.read_text()) if hist_file.exists() else []
    history.append({
        "t": now_iso,
        "bikes": latest["bikes_available"],
        "ebikes": latest["ebikes_available"],
        "empty_pct": latest["pct_stations_empty"],
    })
    history = history[-HISTORY_LIMIT:]

    quality = {
        "passed": True,
        "checks": checks,
        "rows_processed": len(status["data"]["stations"]) + len(info["data"]["stations"]),
        "runtime_seconds": round(time.time() - t0, 2),
        "run_at": now_iso,
    }

    (OUT / "latest.json").write_text(json.dumps(latest, indent=1))
    (OUT / "stations.json").write_text(json.dumps(stations_out, indent=1))
    hist_file.write_text(json.dumps(history, indent=1))
    (OUT / "quality.json").write_text(json.dumps(quality, indent=1))
    print(
        f"OK: {latest['stations_online']} stations, {latest['bikes_available']} bikes "
        f"({latest['ebikes_available']} electric), {len(history)} history points, "
        f"{quality['runtime_seconds']}s"
    )


if __name__ == "__main__":
    main()
