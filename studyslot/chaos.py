"""Chaos switches, shared by the HTTP API and the gRPC service of ONE replica.

In Assignment 2 the delay lived in main.py and only slowed POST /bookings.
In Assignment 3 both APIs run in the same process, and the Part D
"least request" experiment needs one replica (B) to be slow on BOTH
interfaces. So the switches moved here, and:

  delay_seconds       is added to every API call this replica handles
                      (HTTP and gRPC), inside the request metrics, so the
                      replica's own p95 and in-flight gauges show it.
  fail_next_bookings  the next N POST /bookings fail with 503 (A2 caller demo).

The values live in this process's memory, so each replica has its own
switches. That is deliberate: setting a delay on replica B leaves A and C
untouched (see scripts/replica_chaos.py).
"""
import threading

_lock = threading.Lock()
_delay_seconds = 0.0
_fail_next_bookings = 0


def delay_seconds() -> float:
    return _delay_seconds


def set_delay(seconds: float) -> None:
    global _delay_seconds
    _delay_seconds = seconds


def fail_next_bookings() -> int:
    return _fail_next_bookings


def set_fail_next_bookings(count: int) -> None:
    global _fail_next_bookings
    with _lock:
        _fail_next_bookings = count


def consume_injected_failure() -> bool:
    """True if this booking should fail (and counts the switch down)."""
    global _fail_next_bookings
    with _lock:
        if _fail_next_bookings > 0:
            _fail_next_bookings -= 1
            return True
    return False


def state() -> dict:
    return {"delay_seconds": _delay_seconds, "fail_next_bookings": _fail_next_bookings}


def reset() -> None:
    set_delay(0.0)
    set_fail_next_bookings(0)