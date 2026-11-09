"""Fixed-rate test client for Assignment 3 (Parts B, C and D).

Sends requests at a FIXED RATE for a FIXED DURATION through Envoy, WITHOUT
waiting for each response before sending the next one ("open loop"): request
number i leaves at  start + i / rate,  whether or not earlier ones have
answered. A slow replica therefore cannot slow the client down and hide
itself, which a "send, wait, send" loop would do.

Test operations (one per interface, both from A2):
  HTTP  GET /bookings/{id}     read-only; not rate limited (unlike /availability)
  gRPC  CreateBooking          always with the SAME idempotency key and body, so
                               every call is a replay: nothing new is written,
                               and every replica must return the same booking
                               (this checks the idempotency records are shared)

No retries and no circuit breaker: one attempt per request, so every failure
is counted. gRPC uses ONE channel (one HTTP/2 connection to Envoy) for all
calls, like a normal gRPC client.

For every request the client records which replica answered (the X-Replica
header / x-replica metadata), so the A/B/C shares can be checked against the
Grafana dashboard and Envoy's access logs.

Output:
  a summary on screen, and in results/<label>.json
  every request in results/<label>-requests.csv (time, latency, outcome, replica, trace id)

Examples (repo root, stack running):
  .venv\\Scripts\\python scripts/load_test.py --label B-baseline --policy "A only"
  .venv\\Scripts\\python scripts/load_test.py --label C-rr --policy "round robin"
  .venv\\Scripts\\python scripts/load_test.py --label D3-failure --policy "round robin + health checks" --fault "B stopped at 30 s, started at 60 s" --stop-replica B --stop-at 30 --start-at 60
"""
import argparse
import asyncio
import csv
import json
import math
import random
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import grpc
import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "studyslot"))
import booking_pb2        # noqa: E402
import booking_pb2_grpc   # noqa: E402

RESULTS = REPO / "results"

# The booking each interface reads / replays. Its own room+slot, so the A2
# demo scripts never collide with it.
STUDENTS = ["lt-1", "lt-2", "lt-3", "lt-4", "lt-5"]
HTTP_TARGET = {"room": "R3", "slot": "16:30", "key": "a3-loadtest-http-target"}
GRPC_TARGET = {"room": "R3", "slot": "16:00", "key": "a3-loadtest-grpc-target"}
CONTAINERS = {"A": "studyslot-app-1", "B": "studyslot-app-2", "C": "studyslot-app-3"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_trace() -> tuple[str, str]:
    trace_id = f"{random.getrandbits(128):032x}"
    return trace_id, f"00-{trace_id}-{random.getrandbits(64):016x}-01"


def percentile(values: list[float], q: float) -> float | None:
    """Nearest-rank percentile (q between 0 and 100)."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


# ---------------------------------------------------------------------------
# Setup: make sure the target bookings exist (not counted in the results)
# ---------------------------------------------------------------------------
async def prepare_http(http: httpx.AsyncClient) -> str:
    body = {"room": HTTP_TARGET["room"], "slot": HTTP_TARGET["slot"], "student_ids": STUDENTS}
    r = await http.post("/bookings", json=body, headers={"Idempotency-Key": HTTP_TARGET["key"]})
    if r.status_code != 201:
        sys.exit(f"HTTP setup failed: POST /bookings -> {r.status_code} {r.text}\n"
                 f"If {HTTP_TARGET['room']} {HTTP_TARGET['slot']} is booked by something else, cancel it first.")
    return r.json()["id"]


async def prepare_grpc(stub) -> str:
    request = grpc_request()
    try:
        booking = await stub.CreateBooking(request, timeout=5)
    except grpc.aio.AioRpcError as err:
        sys.exit(f"gRPC setup failed: CreateBooking -> {err.code().name} {err.details()}\n"
                 f"If {GRPC_TARGET['room']} {GRPC_TARGET['slot']} is booked by something else, cancel it first.")
    return booking.id


def grpc_request():
    return booking_pb2.CreateBookingRequest(room=GRPC_TARGET["room"], slot=GRPC_TARGET["slot"],
                                            student_ids=STUDENTS, idempotency_key=GRPC_TARGET["key"])


# ---------------------------------------------------------------------------
# One request each. Every request gets its own trace-id and request-id, so
# Envoy's log line and the replica's log line can be found together in Kibana.
# ---------------------------------------------------------------------------
async def http_call(http: httpx.AsyncClient, booking_id: str, timeout: float) -> tuple[str, str, str]:
    trace_id, traceparent = new_trace()
    headers = {"traceparent": traceparent, "X-Request-ID": uuid.uuid4().hex[:16]}
    try:
        r = await http.get(f"/bookings/{booking_id}", headers=headers, timeout=timeout)
        return str(r.status_code), r.headers.get("x-replica", "-"), trace_id
    except httpx.TimeoutException:
        return "timeout", "-", trace_id
    except httpx.HTTPError:
        return "connection_error", "-", trace_id


async def grpc_call(stub, booking_id: str, timeout: float) -> tuple[str, str, str]:
    trace_id, traceparent = new_trace()
    metadata = (("traceparent", traceparent), ("x-request-id", uuid.uuid4().hex[:16]))
    call = stub.CreateBooking(grpc_request(), timeout=timeout, metadata=metadata)
    try:
        booking = await call
        replica = dict(await call.initial_metadata() or ()).get("x-replica", "-")
        # Every call is a replay of the same key, so it must return the SAME booking,
        # whichever replica answers. Anything else means duplicate prevention broke.
        outcome = "OK" if booking.id == booking_id and booking.replayed else "WRONG_BOOKING"
        return outcome, replica, trace_id
    except grpc.aio.AioRpcError as err:
        replica = dict(err.initial_metadata() or ()).get("x-replica", "-")
        return err.code().name, replica, trace_id


# ---------------------------------------------------------------------------
# The fixed-rate schedule
# ---------------------------------------------------------------------------
async def run_schedule(name: str, call, rate: float, duration: float, t0: float,
                       records: list) -> None:
    """Start request i at t0 + i/rate, without waiting for earlier ones."""
    loop = asyncio.get_running_loop()
    total = int(rate * duration)
    tasks = []

    async def one(i: int, planned: float) -> None:
        started = loop.time()
        outcome, replica, trace_id = await call()
        latency_ms = (loop.time() - started) * 1000
        records.append({"interface": name, "i": i, "planned_s": round(planned - t0, 4),
                        "sent_s": round(started - t0, 4), "latency_ms": round(latency_ms, 2),
                        "outcome": outcome, "replica": replica, "trace_id": trace_id})

    for i in range(total):
        planned = t0 + i / rate
        await asyncio.sleep(max(0.0, planned - loop.time()))
        tasks.append(asyncio.create_task(one(i, planned)))
    await asyncio.gather(*tasks)


async def docker_action(action: str, container: str, at: float, t0: float, events: list) -> None:
    """Run 'docker stop|start <container>' at t0 + at seconds and note when it happened."""
    loop = asyncio.get_running_loop()
    await asyncio.sleep(max(0.0, t0 + at - loop.time()))
    issued = loop.time()
    issued_utc = utc_now()
    # Run in a thread so the request schedule keeps going while Docker works
    # (docker stop can take a few seconds: it waits for in-flight requests).
    proc = await asyncio.to_thread(subprocess.run, ["docker", action, container],
                                   capture_output=True, text=True)
    done = loop.time()
    error = proc.stderr.strip() if proc.returncode else ""
    events.append({"action": action, "container": container,
                   "issued_s": round(issued - t0, 3), "returned_s": round(done - t0, 3),
                   "issued_utc": issued_utc, "returned_utc": utc_now(),
                   "ok": proc.returncode == 0, "error": error})
    print(f"  [{issued - t0:6.2f} s] docker {action} {container} issued; "
          f"returned after {done - issued:.2f} s: {'ok' if proc.returncode == 0 else error}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def summarise(name: str, records: list, rate: float, duration: float, events: list,
              stop_replica: str | None) -> dict:
    rows = [r for r in records if r["interface"] == name]
    if not rows:
        return {}
    success = "200" if name == "http" else "OK"
    ok = [r for r in rows if r["outcome"] == success]
    failures = Counter(r["outcome"] for r in rows if r["outcome"] != success)
    sent_times = [r["sent_s"] for r in rows]
    send_span = max(sent_times) - min(sent_times)
    replicas = Counter(r["replica"] for r in ok)
    latencies = [r["latency_ms"] for r in rows]
    summary = {
        "interface": name,
        "operation": "GET /bookings/{id}" if name == "http" else "CreateBooking (replay)",
        "target_rps": rate,
        "duration_s": duration,
        "sent": len(rows),
        "actual_send_rps": round((len(rows) - 1) / send_span, 2) if send_span > 0 else None,
        "max_send_lag_ms": round(max(r["sent_s"] - r["planned_s"] for r in rows) * 1000, 1),
        "successes": len(ok),
        "failures": sum(failures.values()),
        "error_rate_pct": round(100 * sum(failures.values()) / len(rows), 2),
        "failures_by_outcome": dict(failures),
        "client_latency_ms": {
            "p50": percentile(latencies, 50), "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99), "max": max(latencies),
        },
        "successes_by_replica": dict(sorted(replicas.items())),
        "share_pct": {k: round(100 * v / len(ok), 1) for k, v in sorted(replicas.items())} if ok else {},
    }
    if stop_replica and events:
        summary["failure_timing"] = failure_timing(rows, success, stop_replica, events)
    return summary


def failure_timing(rows: list, success: str, replica: str, events: list) -> dict:
    """Client's view of the failure run: when errors started/stopped, when the replica came back."""
    stop = next((e for e in events if e["action"] == "stop"), None)
    start = next((e for e in events if e["action"] == "start"), None)
    failed = sorted(r["sent_s"] for r in rows if r["outcome"] != success)
    served = sorted(r["sent_s"] for r in rows if r["outcome"] == success and r["replica"] == replica)
    timing = {"first_error_sent_s": failed[0] if failed else None,
              "last_error_sent_s": failed[-1] if failed else None}
    if stop:
        before = [t for t in served if t < stop["issued_s"]]
        timing["last_success_from_replica_before_stop_s"] = before[-1] if before else None
        if failed:
            timing["errors_end_after_stop_issued_s"] = round(failed[-1] - stop["issued_s"], 2)
    if start:
        after = [t for t in served if t >= start["issued_s"]]
        timing["first_success_from_replica_after_start_s"] = after[0] if after else None
        if after:
            timing["back_in_rotation_after_start_issued_s"] = round(after[0] - start["issued_s"], 2)
    return timing


def print_summary(s: dict) -> None:
    lat = s["client_latency_ms"]
    print(f"\n  {s['interface'].upper():4} {s['operation']}")
    print(f"       target {s['target_rps']}/s for {s['duration_s']} s -> sent {s['sent']} "
          f"(actual {s['actual_send_rps']}/s, max send lag {s['max_send_lag_ms']} ms)")
    print(f"       successes {s['successes']}, failures {s['failures']} ({s['error_rate_pct']} %)"
          + (f"  {s['failures_by_outcome']}" if s["failures"] else ""))
    print(f"       client latency ms: p50 {lat['p50']}, p95 {lat['p95']}, p99 {lat['p99']}, max {lat['max']}")
    shares = "  ".join(f"{k} {v} ({s['share_pct'][k]} %)" for k, v in s["successes_by_replica"].items())
    print(f"       answered by: {shares}")
    if "failure_timing" in s:
        print(f"       failure timing: {s['failure_timing']}")
    if s["max_send_lag_ms"] > 50:
        print("       WARNING: the client fell behind its schedule; the target rate was not held.")


# ---------------------------------------------------------------------------
async def main_async(args) -> None:
    RESULTS.mkdir(exist_ok=True)
    interfaces = ["http", "grpc"] if args.interface == "both" else [args.interface]
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    # trust_env=False: ignore any HTTP proxy settings on the machine; talk to Envoy directly.
    http = httpx.AsyncClient(base_url=args.http_url, limits=limits, trust_env=False)
    channel = grpc.aio.insecure_channel(args.grpc_target, options=[
        ("grpc.enable_retries", 0),          # gRPC's own transparent retries: off
    ])
    stub = booking_pb2_grpc.BookingServiceStub(channel)
    try:
        http_id = await prepare_http(http) if "http" in interfaces else None
        grpc_id = await prepare_grpc(stub) if "grpc" in interfaces else None
    except httpx.HTTPError as err:
        sys.exit(f"Cannot reach {args.http_url} ({err!r}). Is the stack running? Try: docker compose up -d")

    calls = {
        "http": lambda: http_call(http, http_id, args.timeout),
        "grpc": lambda: grpc_call(stub, grpc_id, args.timeout),
    }
    records: list = []
    events: list = []
    loop = asyncio.get_running_loop()
    t0 = loop.time() + 0.5                   # small head start so both schedules begin together
    start_utc = utc_now()
    print(f"[{time.strftime('%H:%M:%S')}] {args.label}: {', '.join(interfaces)} at "
          f"{args.rate}/s each for {args.duration} s ({args.policy}; fault: {args.fault})")

    jobs = [run_schedule(name, calls[name], args.rate, args.duration, t0, records) for name in interfaces]
    if args.stop_replica:
        container = CONTAINERS[args.stop_replica]
        jobs.append(docker_action("stop", container, args.stop_at, t0, events))
        if args.start_at is not None:
            jobs.append(docker_action("start", container, args.start_at, t0, events))
    await asyncio.gather(*jobs)
    end_utc = utc_now()
    await http.aclose()
    await channel.close()

    summaries = [summarise(name, records, args.rate, args.duration, events, args.stop_replica)
                 for name in interfaces]
    print(f"[{time.strftime('%H:%M:%S')}] done")
    for s in summaries:
        print_summary(s)

    result = {"label": args.label, "policy": args.policy, "fault": args.fault,
              "start_utc": start_utc, "end_utc": end_utc, "timeout_s": args.timeout,
              "events": events, "interfaces": summaries}
    out = RESULTS / f"{args.label}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    with open(RESULTS / f"{args.label}-requests.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(sorted(records, key=lambda r: (r["interface"], r["i"])))
    print(f"\nSaved {out.relative_to(REPO)} and {args.label}-requests.csv "
          f"(time range for Grafana/Kibana: {start_utc} to {end_utc})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fixed-rate HTTP + gRPC load through Envoy.")
    parser.add_argument("--label", required=True, help="run name, used for the results files, e.g. C-rr")
    parser.add_argument("--policy", default="", help="what Envoy is doing, for the results table")
    parser.add_argument("--fault", default="none", help="what was broken on purpose, for the results table")
    parser.add_argument("--interface", choices=["both", "http", "grpc"], default="both")
    parser.add_argument("--rate", type=float, default=40, help="requests per second, per interface (default 40)")
    parser.add_argument("--duration", type=float, default=90, help="seconds (default 90)")
    parser.add_argument("--timeout", type=float, default=2.0, help="client timeout per request, seconds (default 2)")
    parser.add_argument("--stop-replica", choices=list(CONTAINERS), help="failure run: replica to stop")
    parser.add_argument("--stop-at", type=float, default=20, help="seconds into the run to stop it (default 20)")
    parser.add_argument("--start-at", type=float, default=None, help="seconds into the run to start it again")
    parser.add_argument("--http-url", default="http://localhost:8000")
    parser.add_argument("--grpc-target", default="localhost:50051")
    parser.add_argument("--selector-loop", action="store_true",
                        help="Windows only: use the selector event loop (try this if gRPC complains about the event loop)")
    args = parser.parse_args()
    if args.rate <= 0 or args.duration <= 0:
        sys.exit("--rate and --duration must be positive")
    if args.selector_loop and sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()