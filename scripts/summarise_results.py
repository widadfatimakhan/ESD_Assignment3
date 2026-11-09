"""Build the Part D summary table from the load_test.py result files.

Reads results/*.json (or the files you name), oldest run first, and prints a
Markdown table with the brief's columns. The numbers are copied from the
result files exactly, so nothing is typed by hand. The Explanation column is
left empty for you to fill in.

Run from the repo root:
    python scripts/summarise_results.py
    python scripts/summarise_results.py results/C-rr.json results/D1-weighted.json
"""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def load(paths: list[str]) -> list[dict]:
    files = [Path(p) for p in paths] or sorted((REPO / "results").glob("*.json"))
    runs = [json.loads(f.read_text(encoding="utf-8")) for f in files]
    return sorted(runs, key=lambda r: r["start_utc"])


def shares(s: dict) -> str:
    return " / ".join(f"{s['share_pct'].get(r, 0.0):.1f}" for r in "ABC") + " %"


def main() -> None:
    runs = load(sys.argv[1:])
    if not runs:
        sys.exit("No result files found in results/. Run scripts/load_test.py first.")
    print("| Run / interface | Policy and fault | Target / actual requests per second "
          "| A/B/C traffic shares | Client p95 | Error rate | Explanation |")
    print("|---|---|---|---|---|---|---|")
    for run in runs:
        for s in run["interfaces"]:
            print(f"| {run['label']} / {'HTTP' if s['interface'] == 'http' else 'gRPC'} "
                  f"| {run['policy'] or '-'}; fault: {run['fault']} "
                  f"| {s['target_rps']:g} / {s['actual_send_rps']} "
                  f"| {shares(s)} "
                  f"| {s['client_latency_ms']['p95']} ms "
                  f"| {s['error_rate_pct']} % "
                  f"|  |")
    print(f"\n{len(runs)} runs. Time ranges (UTC), for Grafana and Kibana:")
    for run in runs:
        print(f"  {run['label']}: {run['start_utc']} to {run['end_utc']}")


if __name__ == "__main__":
    main()