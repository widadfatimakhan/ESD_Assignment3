"""StudySlot HTTP API (FastAPI).

This file only deals with HTTP: routes, status codes, the error format,
metrics and chaos switches. The booking rules live in booking_core.py and the
data in db.py, so the gRPC service (Part B) reuses exactly the same rules and data.

Assignment 3 changes (one process = one replica, both interfaces):
  - when STUDYSLOT_GRPC_PORT is set, this process also starts the gRPC server
    (grpc_server.start_in_background), so each replica serves HTTP and gRPC;
  - every response carries X-Replica (A, B or C, see replica.py);
  - request metrics skip /metrics, /health and /chaos*, as in the Week 1 lab,
    so Envoy's health checks and our chaos calls are not counted as traffic;
  - the chaos delay (chaos.py) slows every API call on this replica;
  - behind Envoy, the rate limiter identifies the caller from X-Forwarded-For.
"""
import asyncio
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_redoc_html
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import BaseModel, ConfigDict, Field
from prometheus_client import (
    Counter, Gauge, Histogram,
    generate_latest, CONTENT_TYPE_LATEST,
)
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.routing import Match

import booking_core
import chaos
import db
import grpc_server
from logging_setup import log_event, request_id_var, trace_context_var
from replica import REPLICA
from tracing import child_span
from metrics import BOOKINGS_TOTAL, IDEMPOTENT_REPLAYS, LATENCY_BUCKETS
from rate_limit import RateLimitExceeded, SlidingWindowRateLimiter

db.init_db()   # create the database file and table on startup if needed

# A3: the gRPC server runs in this same process when this is set (docker-compose
# sets it to 50051). The tests leave it unset and start their own gRPC server.
GRPC_PORT = os.getenv("STUDYSLOT_GRPC_PORT")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the gRPC server with the HTTP app, and stop it when Uvicorn shuts down."""
    server = grpc_server.start_in_background(int(GRPC_PORT)) if GRPC_PORT else None
    log_event("replica started", level="INFO", grpc_port=GRPC_PORT)
    yield
    if server is not None:
        grpc_server.stop(server)


app = FastAPI(
    title="StudySlot",
    version="3.0.0",
    lifespan=lifespan,
    description=(
        "Campus study-room booking service (ESD Assignments 2 and 3).\n\n"
        "- Every response carries an `X-Request-ID` header. Send your own to correlate calls; "
        "otherwise one is generated. The same ID appears in the logs and in error bodies.\n"
        "- W3C Trace Context: send a `traceparent` header to join an existing trace. The response "
        "`traceparent` carries the same trace-id with this service's span-id, and every log line "
        "includes `trace.id` (shared with the gRPC service for the same trace).\n"
        "- Every error uses the `ErrorResponse` shape: `{\"error\": {\"code\", \"message\", "
        "\"request_id\", \"details\"}}`.\n"
        "- `POST /bookings` accepts an `Idempotency-Key` header so retries are safe.\n"
        "- `GET /availability` is rate limited to 5 calls per 10 seconds per client IP.\n"
        "- Every response carries an `X-Replica` header naming the replica (A, B or C) that handled it."
    ),
    redoc_url=None,   # replaced by the /redoc route below (FastAPI's default ReDoc link is broken)
)

# ---------------------------------------------------------------------------
# Metrics
# A1 metrics are kept (see the Part C audit in REPORT.md); the business
# counters studyslot_bookings_total and studyslot_idempotent_replays_total
# now live in metrics.py so the gRPC service reports them too.
# ---------------------------------------------------------------------------
# Reads the count from the database at every scrape, so it stays correct after
# restarts and when the gRPC service changes bookings too.
ROOMS_OCCUPIED = Gauge(
    "studyslot_rooms_occupied",
    "Number of room-slots currently booked",
)
ROOMS_OCCUPIED.set_function(db.count_bookings)

# Chaos switches (delay, injected failures) now live in chaos.py, shared with
# the gRPC server in this process. Each replica has its own copy.


class InjectedFailure(Exception):
    """Simulated outage: main.py turns this into 503 Service Unavailable."""

AVAILABILITY_CHECK_SECONDS = Histogram(
    "studyslot_availability_check_seconds",
    "Time spent checking room availability during a booking",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# --- Part C: request metrics for every HTTP route (RED: rate, errors, duration) ---
HTTP_REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests handled, by method, route template and status code",
    ["method", "route", "status"],
)
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "Time to handle an HTTP request, measured on the server",
    ["method", "route"],
    buckets=LATENCY_BUCKETS,
)
HTTP_IN_FLIGHT = Gauge(
    "http_requests_in_flight",
    "HTTP requests being handled right now",
    ["method", "route"],
)


# ---------------------------------------------------------------------------
# Request metrics (Part C). Labels use the route TEMPLATE, e.g.
# "/bookings/{booking_id}", never the real path with an ID in it, so the
# number of time series stays small and fixed.
# ---------------------------------------------------------------------------
_KNOWN_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}

# A3: not API traffic, so not measured (the Week 1 lab's rule). /health is
# called by Envoy's health checks every few seconds; /chaos is our own
# control panel; /metrics is Prometheus.
_NOT_MEASURED = ("/metrics", "/health", "/chaos")


def route_label(request: Request) -> str:
    """The route template this request matches, or "unmatched" for unknown paths."""
    partial = None
    for route in app.router.routes:
        match, _ = route.matches(request.scope)
        if match == Match.FULL:
            return route.path
        if match == Match.PARTIAL and partial is None:   # right path, wrong method (405)
            partial = route.path
    return partial or "unmatched"


@app.middleware("http")
async def measure_http_requests(request: Request, call_next):
    route = route_label(request)
    if route.startswith(_NOT_MEASURED):     # scrapes, health checks, chaos calls
        return await call_next(request)
    method = request.method if request.method in _KNOWN_METHODS else "other"

    in_flight = HTTP_IN_FLIGHT.labels(method=method, route=route)
    in_flight.inc()
    start = time.perf_counter()
    status = 500                            # if the app crashes, count it as a 500
    try:
        # A3 chaos: a slow replica. asyncio.sleep, not time.sleep, so the
        # event loop keeps serving other requests while this one waits.
        delay = chaos.delay_seconds()
        if delay:
            await asyncio.sleep(delay)
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        elapsed = time.perf_counter() - start
        HTTP_DURATION.labels(method=method, route=route).observe(elapsed)
        HTTP_REQUESTS.labels(method=method, route=route, status=str(status)).inc()
        in_flight.dec()
        # A3: one log line per API request, so every request Envoy logs has a
        # matching line from the replica that served it (same trace.id). The
        # IDs go in the log, never in the metric labels above.
        log_event("request handled", level="INFO", method=method, route=route,
                  path=request.url.path, status=status, duration_ms=round(elapsed * 1000, 2))


# ---------------------------------------------------------------------------
# Request IDs and tracing. (A3: defined AFTER the metrics middleware, which
# makes it the OUTER one, so the request ID and trace are already set while
# the metrics middleware runs and its "request handled" log line carries them.)
# - X-Request-ID: one ID per request, sent back in the response header,
#   included in every log line and in every error body.
# - traceparent (W3C Trace Context, as in the course lab): the trace-id is
#   shared by every service that handles one operation; each request gets
#   its own span-id. Logged as trace.id / span.id.
# ---------------------------------------------------------------------------
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9-]{1,64}$")


@app.middleware("http")
async def assign_request_id(request: Request, call_next):
    incoming = request.headers.get("X-Request-ID", "")
    # Reuse the caller's ID if it looks safe, otherwise make a new one.
    request_id = incoming if _SAFE_REQUEST_ID.match(incoming) else uuid.uuid4().hex[:16]
    request.state.request_id = request_id
    span = child_span(request.headers.get("traceparent"))
    token = request_id_var.set(request_id)
    trace_token = trace_context_var.set(span)
    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(token)
        trace_context_var.reset(trace_token)
    response.headers["X-Request-ID"] = request_id
    response.headers["traceparent"] = span["traceparent"]
    response.headers["X-Replica"] = REPLICA          # A3: which replica answered
    return response


# ---------------------------------------------------------------------------
# One error format for every error response:
#   {"error": {"code": "...", "message": "...", "request_id": "...", "details": {...}}}
# ---------------------------------------------------------------------------
class ErrorBody(BaseModel):
    code: str = Field(description="Short machine-readable error code", examples=["double_booked"])
    message: str = Field(description="Human-readable explanation")
    request_id: str | None = Field(None, description="Matches the X-Request-ID header and the logs")
    details: dict | None = Field(None, description="Extra information, when there is any")


class ErrorResponse(BaseModel):
    error: ErrorBody


def error_response(request: Request, status: int, code: str, message: str,
                   details: dict | None = None, headers: dict | None = None) -> JSONResponse:
    body = {"code": code, "message": message,
            "request_id": getattr(request.state, "request_id", None)}
    if details:
        body["details"] = details
    return JSONResponse(status_code=status, content={"error": body}, headers=headers)


# Which HTTP status each kind of booking-rule problem becomes.
# Anything not listed is 422: the request was readable but broke a rule.
HTTP_STATUS_FOR = {
    booking_core.AlreadyBooked: 409,
    booking_core.BookingNotFound: 404,
}


def status_for(err: booking_core.BookingError) -> int:
    return HTTP_STATUS_FOR.get(type(err), 422)


@app.exception_handler(booking_core.BookingError)
async def handle_booking_error(request: Request, err: booking_core.BookingError):
    return error_response(request, status_for(err), err.reason, err.message, err.details)


@app.exception_handler(RequestValidationError)
async def handle_validation_error(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    if any(e["type"] == "json_invalid" for e in errors):
        status, code, message, details = 400, "malformed_json", "The request body is not valid JSON.", None
    else:
        problems = [{"field": ".".join(str(p) for p in e["loc"]), "problem": e["msg"]} for e in errors]
        status, code, message, details = 422, "validation_error", "Some fields are missing or invalid.", {"problems": problems}
    log_event("request rejected", level="WARNING", method=request.method,
              path=request.url.path, status=status, reason=code)
    return error_response(request, status, code, message, details)


@app.exception_handler(StarletteHTTPException)
async def handle_http_error(request: Request, exc: StarletteHTTPException):
    # e.g. 404 for an unknown path, 405 for a wrong method
    code = HTTPStatus(exc.status_code).phrase.lower().replace(" ", "_")
    return error_response(request, exc.status_code, code, str(exc.detail), headers=exc.headers)


@app.exception_handler(RateLimitExceeded)
async def handle_rate_limited(request: Request, exc: RateLimitExceeded):
    log_event("rate limited", level="WARNING", method=request.method, path=request.url.path,
              client=exc.client, status=429, retry_after_seconds=exc.retry_after)
    return error_response(
        request, 429, "rate_limited",
        f"Too many requests: at most {exc.limit} calls per {exc.window_seconds} seconds "
        f"per client. Try again in {exc.retry_after} seconds.",
        details={"limit": exc.limit, "window_seconds": exc.window_seconds,
                 "retry_after_seconds": exc.retry_after, "client": exc.client},
        headers={"Retry-After": str(exc.retry_after),
                 "X-RateLimit-Limit": str(exc.limit),
                 "X-RateLimit-Remaining": "0"},
    )


@app.exception_handler(db.StorageBusy)
async def handle_storage_busy(request: Request, exc: db.StorageBusy):
    log_event("storage busy", level="WARNING", method=request.method,
              path=request.url.path, status=503, reason="storage_busy")
    return error_response(request, 503, "storage_busy",
                          "The booking database is busy right now. Nothing was changed; please retry.",
                          headers={"Retry-After": "1"})


@app.exception_handler(InjectedFailure)
async def handle_injected_failure(request: Request, exc: InjectedFailure):
    return error_response(request, 503, "service_unavailable",
                          "The booking service is temporarily unavailable (injected failure). Try again shortly.")


@app.exception_handler(Exception)
async def handle_unexpected_error(request: Request, exc: Exception):
    log_event("unhandled error", level="ERROR", path=request.url.path, error=repr(exc))
    return error_response(request, 500, "internal_error", "Something went wrong on our side.")


def header_doc(description: str, type_: str = "string") -> dict:
    """OpenAPI helper: describe one response header."""
    return {"description": description, "schema": {"type": type_}}


RATE_LIMIT_HEADERS = {
    "X-RateLimit-Limit": header_doc("Calls allowed per window", "integer"),
    "X-RateLimit-Remaining": header_doc("Calls left in the current window", "integer"),
}


def errors(*codes: int) -> dict:
    """OpenAPI helper: document that these status codes return ErrorResponse."""
    descriptions = {400: "Malformed request", 404: "Not found", 409: "Conflict",
                    422: "Invalid input or broken booking rule", 429: "Too many requests",
                    503: "Service temporarily unavailable"}
    docs = {c: {"model": ErrorResponse, "description": descriptions.get(c, "Error")} for c in codes}
    if 429 in docs:
        docs[429]["headers"] = {
            "Retry-After": header_doc("Seconds to wait before calling again", "integer"),
            **RATE_LIMIT_HEADERS,
        }
    return docs


# ---------------------------------------------------------------------------
# Request and response shapes
# ---------------------------------------------------------------------------
StudentId = Annotated[str, Field(min_length=1, max_length=32, pattern=r"^[A-Za-z0-9_-]+$")]


class BookingRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",   # unknown fields are rejected instead of silently ignored
        json_schema_extra={"examples": [
            {"room": "R3", "slot": "09:00", "student_ids": ["s1", "s2", "s3", "s4", "s5"]}
        ]},
    )
    room: str = Field(description="Room code, R1 to R5")
    slot: str = Field(description="Start time of a 30-minute slot, 09:00 to 16:30")
    student_ids: list[StudentId] = Field(min_length=1, max_length=50,
                                         description="Every student in the group, each listed once")


class Booking(BaseModel):
    id: str
    room: str
    slot: str
    student_ids: list[str]
    group_size: int
    created_at: str


class RoomInfo(BaseModel):
    room: str
    capacity: int
    min_group: int
    booked_slots: list[str]
    free_slot_count: int


class FreeRoom(BaseModel):
    room: str
    capacity: int
    min_group: int


class Availability(BaseModel):
    slot: str
    group_size: int | None
    free_rooms: list[FreeRoom]


class DelaySetting(BaseModel):
    seconds: float = Field(ge=0, le=10, description="Delay added to every API call on this replica (HTTP and gRPC); 0 turns it off")


class FailureSetting(BaseModel):
    count: int = Field(ge=0, le=100, description="How many of the next POST /bookings requests fail with 503; 0 turns it off")


class ChaosState(BaseModel):
    delay_seconds: float
    fail_next_bookings: int


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", tags=["service"])
def home():
    return {"service": "StudySlot", "status": "running"}


@app.get("/health", tags=["service"])
def health():
    return {"status": "ok"}


@app.get("/rooms", response_model=list[RoomInfo], tags=["rooms"])
def list_rooms():
    """Every room with its size limits and which slots are booked."""
    return booking_core.list_rooms()


# ---------------------------------------------------------------------------
# Rate limiting for GET /availability
# Students refresh availability to hunt for a free room, so this is the
# endpoint most likely to be hammered. Each client (identified by its IP
# address) gets 5 calls in any 10-second period.
# ---------------------------------------------------------------------------
AVAILABILITY_LIMIT = 5
AVAILABILITY_WINDOW_SECONDS = 10
availability_limiter = SlidingWindowRateLimiter(AVAILABILITY_LIMIT, AVAILABILITY_WINDOW_SECONDS)


# A3: behind Envoy, request.client.host is always ENVOY's address, so every
# user would share one budget. Envoy appends the address it received the
# request from to X-Forwarded-For (use_remote_address in envoy.yaml). Only the
# LAST entry is trustworthy: anything before it was written by the caller and
# could be faked. Trusting the header is only safe because the replicas are not
# published to the host; the only way in is through Envoy.
TRUST_X_FORWARDED_FOR = os.getenv("TRUST_X_FORWARDED_FOR", "false").lower() == "true"


def client_address(request: Request) -> str:
    if TRUST_X_FORWARDED_FOR:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded.strip():
            return forwarded.split(",")[-1].strip()
    return request.client.host if request.client else "unknown"


def limit_availability_calls(request: Request, response: Response) -> None:
    """Dependency: runs before /availability. Raises RateLimitExceeded when over the limit.

    The counts live in THIS replica's memory (rate_limit.py), so with three
    replicas each one keeps its own count. See the A3 report, Part A.
    """
    client = client_address(request)
    remaining = availability_limiter.check(client)
    response.headers["X-RateLimit-Limit"] = str(AVAILABILITY_LIMIT)
    response.headers["X-RateLimit-Remaining"] = str(remaining)


@app.get("/availability", response_model=Availability,
         responses={200: {"headers": RATE_LIMIT_HEADERS}, **errors(422, 429)},
         dependencies=[Depends(limit_availability_calls)], tags=["rooms"])
def availability(
    slot: str = Query(description="Slot start time, e.g. 09:00"),
    group_size: int | None = Query(None, ge=1, le=50, description="Only show rooms that fit this many students"),
):
    """Free rooms for a slot. Pass group_size to see only rooms that fit.

    Rate limited: 5 calls per 10 seconds per client IP. Over the limit you get
    429 with a Retry-After header saying how many seconds to wait.
    """
    return booking_core.availability(slot, group_size)


@app.post("/bookings", status_code=201, response_model=Booking, tags=["bookings"],
          responses={
              201: {"description": "Booking created, or replayed for a repeated Idempotency-Key",
                    "headers": {
                        "Location": header_doc("URL of the booking, e.g. /bookings/{id}"),
                        "Idempotent-Replayed": header_doc("'true' when this is a replay of an earlier request"),
                    }},
              **errors(400, 409, 422, 503),
          })
def create_booking(
    req: BookingRequest,
    response: Response,
    idempotency_key: str | None = Header(
        None, min_length=8, max_length=64, pattern=r"^[A-Za-z0-9_-]+$",
        description=(
            "Optional but recommended. A unique value (e.g. a UUID) chosen by the client for "
            "this booking. Retrying with the same key and body returns the original booking "
            "instead of creating a new one or failing with 409."
        ),
    ),
):
    """Book a room for a slot. Returns 201 with a Location header pointing at the new booking.

    A replayed request (same Idempotency-Key, same body) returns the original 201
    response with the header `Idempotent-Replayed: true`.
    """
    if chaos.consume_injected_failure():   # chaos: simulated outage
        log_event("booking failed", level="ERROR", status=503, reason="injected_failure")
        raise InjectedFailure()

    # A1 timer. Since A3 the chaos delay is added in the request middleware
    # (for every route and for gRPC), so it no longer shows up in this histogram.
    start = time.perf_counter()
    try:
        booking, replayed = booking_core.create_booking(
            req.room, req.slot, req.student_ids, idempotency_key=idempotency_key
        )
    except booking_core.BookingError as err:
        log_event("booking rejected", level="WARNING", room=req.room, slot=req.slot,
                  group_size=len(req.student_ids), status=status_for(err), reason=err.reason)
        raise                          # handle_booking_error turns it into the error response
    finally:
        AVAILABILITY_CHECK_SECONDS.observe(time.perf_counter() - start)

    response.headers["Location"] = f"/bookings/{booking['id']}"
    if replayed:
        response.headers["Idempotent-Replayed"] = "true"
        IDEMPOTENT_REPLAYS.inc()
        log_event("booking replayed", level="INFO", booking_id=booking["id"], room=booking["room"],
                  slot=booking["slot"], status=201, idempotent_replay=True)
    else:
        BOOKINGS_TOTAL.inc()           # count only bookings that were really created
        log_event("booking confirmed", level="INFO", booking_id=booking["id"], room=booking["room"],
                  slot=booking["slot"], group_size=booking["group_size"], status=201)
    return booking


@app.get("/bookings", response_model=list[Booking], tags=["bookings"])
def list_bookings():
    """Librarian view: everything currently booked."""
    return booking_core.list_bookings()


@app.get("/bookings/{booking_id}", response_model=Booking, responses=errors(404, 422), tags=["bookings"])
def get_booking(booking_id: uuid.UUID):
    """One booking by its ID."""
    return booking_core.get_booking(str(booking_id))


@app.delete("/bookings/{booking_id}", status_code=204, responses=errors(404, 422, 503), tags=["bookings"])
def cancel_booking(booking_id: uuid.UUID):
    """Cancel a booking. Returns 204 with no body."""
    booking = booking_core.cancel_booking(str(booking_id))
    log_event("booking cancelled", level="INFO", booking_id=booking["id"],
              room=booking["room"], slot=booking["slot"], status=204)
    return Response(status_code=204)


@app.get("/redoc", include_in_schema=False)
def redoc():
    """API reference page drawn from the OpenAPI spec.

    FastAPI 0.109 loads ReDoc from the CDN tag "redoc@next", which no longer
    renders (blank page), so we point it at the stable 2.x release instead.
    """
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title="StudySlot - ReDoc",
        redoc_js_url="https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js",
    )


@app.get("/metrics", response_class=PlainTextResponse, tags=["service"],
         responses={200: {"description": "Prometheus text exposition format"}})
def metrics():
    """The page Prometheus scrapes. Plain text, not JSON."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# --- Chaos switches (from A1; per replica since A3, see chaos.py) ---
# Through Envoy these calls land on whichever replica Envoy picks. To change
# ONE replica, use scripts/replica_chaos.py, which calls it from inside that
# replica's container.
@app.get("/chaos", response_model=ChaosState, tags=["chaos"])
def get_chaos():
    """Current chaos settings of the replica that answers (see X-Replica)."""
    return chaos.state()


@app.put("/chaos/delay", response_model=ChaosState, responses=errors(422), tags=["chaos"])
def set_delay(setting: DelaySetting):
    """Add a fixed delay to every API call (HTTP and gRPC) on this replica. 0 turns it off."""
    chaos.set_delay(setting.seconds)
    log_event("chaos delay set", level="WARNING", injected_delay_seconds=setting.seconds)
    return chaos.state()


@app.put("/chaos/failures", response_model=ChaosState, responses=errors(422), tags=["chaos"])
def set_failures(setting: FailureSetting):
    """Make the next N POST /bookings requests fail with 503 (simulated outage). 0 turns it off."""
    chaos.set_fail_next_bookings(setting.count)
    log_event("chaos failures set", level="WARNING", fail_next_bookings=setting.count)
    return chaos.state()