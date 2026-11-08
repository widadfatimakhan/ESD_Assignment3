"""Prometheus metrics shared by the HTTP API and the gRPC service (Part C).

Each process (studyslot-app and studyslot-grpc) has its own copy of these
metrics and serves them on its own /metrics page. Prometheus adds a `job`
label (studyslot / studyslot-grpc), so the two can be compared or summed.

Label rule: label values always come from small, fixed sets (HTTP methods,
route templates, status codes, RPC names). Never user IDs, booking IDs,
request IDs or trace IDs - those would create a new time series per request.
"""
from prometheus_client import Counter, Gauge

from replica import CONTAINER, REPLICA

# Latency buckets (seconds), the same for HTTP and gRPC so they can be compared.
# A normal booking takes ~2-30 ms (one SQLite transaction), so the low end is
# fine-grained. The top end covers chaos delays and the caller's 1 s timeout,
# so slow calls land in a real bucket instead of only in +Inf.
LATENCY_BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]

# Business metrics: incremented by whichever API handled the booking.
BOOKINGS_TOTAL = Counter(
    "studyslot_bookings_total",
    "Bookings actually created (idempotent replays are not counted)",
)
IDEMPOTENT_REPLAYS = Counter(
    "studyslot_idempotent_replays_total",
    "Booking requests answered by replaying an earlier result (same idempotency key)",
)


# Assignment 3: which replica this process believes it is. Always 1; the
# information is in the labels. Prometheus also attaches its own `replica`
# label to every metric from this target (prometheus.yml), so a dashboard
# table of this metric shows whether the two agree. The label is called
# replica_name, not replica, so it does not clash with Prometheus's label.
REPLICA_INFO = Gauge(
    "studyslot_replica_info",
    "Identity of this replica (value is always 1)",
    ["replica_name", "container"],
)
REPLICA_INFO.labels(replica_name=REPLICA, container=CONTAINER).set(1)