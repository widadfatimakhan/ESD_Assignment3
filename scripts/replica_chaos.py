"""Set the chaos delay on ONE replica (Assignment 3, Part D "least request").

PUT /chaos/delay through Envoy would land on whichever replica Envoy picks,
and the replicas publish no ports of their own. So this script runs the call
from INSIDE the chosen replica's container:

    docker compose exec -T --index 2 app python -c "...PUT http://localhost:8000/chaos/delay..."

--index N is Compose's replica number: 1 = A, 2 = B, 3 = C.

Run from the repo root (stack running):
    python scripts/replica_chaos.py B --delay 0.2      slow B down by 200 ms per call
    python scripts/replica_chaos.py B --delay 0        back to normal
    python scripts/replica_chaos.py all --delay 0      reset every replica
    python scripts/replica_chaos.py all                show every replica's settings
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
INDEX = {"A": 1, "B": 2, "C": 3}

# Runs inside the container (python:3.13-slim has no curl, but it has Python).
INSIDE = """
import json, sys, urllib.request
delay = sys.argv[1]
if delay != "-":
    req = urllib.request.Request("http://localhost:8000/chaos/delay", method="PUT",
                                 data=json.dumps({"seconds": float(delay)}).encode(),
                                 headers={"Content-Type": "application/json"})
else:
    req = urllib.request.Request("http://localhost:8000/chaos")
with urllib.request.urlopen(req, timeout=5) as resp:
    print(resp.headers.get("X-Replica"), resp.read().decode())
"""


def run_on(letter: str, delay: float | None) -> None:
    cmd = ["docker", "compose", "exec", "-T", "--index", str(INDEX[letter]), "app",
           "python", "-c", INSIDE, "-" if delay is None else str(delay)]
    result = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"replica {letter}: {result.stderr.strip() or 'docker compose exec failed'}")
    answered_by, body = result.stdout.strip().split(" ", 1)
    if answered_by != letter:                       # safety check: did we reach the right one?
        sys.exit(f"Expected replica {letter} but replica {answered_by} answered. Check the container order.")
    print(f"replica {letter}: {json.loads(body)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Chaos switches on one replica.")
    parser.add_argument("replica", choices=["A", "B", "C", "all"])
    parser.add_argument("--delay", type=float, help="seconds added to every API call (0 = off); omit to just show")
    args = parser.parse_args()
    if args.delay is not None and not 0 <= args.delay <= 10:
        sys.exit("--delay must be between 0 and 10 seconds")
    for letter in (INDEX if args.replica == "all" else [args.replica]):
        run_on(letter, args.delay)


if __name__ == "__main__":
    main()