"""StudySlot gRPC server (Assignment 2, Part B; changed in Assignment 3).

Serves ONE operation, CreateBooking, defined in protos/booking.proto, on port
50051. In Assignment 2 it ran in its own container (studyslot-grpc). In
Assignment 3 every replica must expose BOTH interfaces, so main.py starts this
server inside the HTTP app's process (start_in_background below). Running
`python grpc_server.py` on its own still works, as in Assignment 2.

Assignment 3 additions:
  - the standard gRPC health service (grpc.health.v1.Health), which Envoy's
    active health checks call;
  - an `x-replica` entry in the response metadata, so callers and Envoy's
    access log can see which replica answered;
  - the chaos delay (chaos.py) applied to every CreateBooking call.

The handler calls booking_core.create_booking(), exactly like POST /bookings
in main.py, and that writes to the same SQLite file through the shared Docker
volume. So the gRPC and HTTP versions share their business rules and their
source of truth; this is NOT a wrapper that forwards calls to the HTTP API.

Generated code used here (from booking.proto, see the generation command there):
  booking_pb2.py       - the message classes (CreateBookingRequest, Booking)
  booking_pb2_grpc.py  - BookingServiceServicer (base class for our handler)
                         and add_BookingServiceServicer_to_server()
"""
import os
import re
import signal
import time
import uuid
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from prometheus_client import Counter, Gauge, Histogram, start_http_server

import booking_core
import booking_pb2
import booking_pb2_grpc
import chaos
import db
from logging_setup import log_event, request_id_var, trace_context_var
from metrics import BOOKINGS_TOTAL, IDEMPOTENT_REPLAYS, LATENCY_BUCKETS
from replica import REPLICA
from tracing import child_span

GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))   # only used when run on its own (serve())

# The health service is infrastructure, like GET /health: it is not measured,
# traced or delayed (same rule as the course's Week 1 lab middleware).
HEALTH_SERVICE = "grpc.health.v1.Health"

# ---------------------------------------------------------------------------
# Metrics (Part C), recorded for every RPC by MetricsInterceptor below.
# Names follow the common gRPC conventions (grpc_server_*). Labels are the
# service and method names and the status code: small, fixed sets.
# ---------------------------------------------------------------------------
GRPC_HANDLED = Counter(
    "grpc_server_handled_total",
    "RPCs completed on the server, by service, method and gRPC status code",
    ["grpc_service", "grpc_method", "grpc_code"],
)
GRPC_DURATION = Histogram(
    "grpc_server_handling_seconds",
    "Time to handle an RPC, measured on the server (request received to response sent)",
    ["grpc_service", "grpc_method"],
    buckets=LATENCY_BUCKETS,
)
GRPC_IN_FLIGHT = Gauge(
    "grpc_server_in_flight_requests",
    "RPCs being handled right now",
    ["grpc_service", "grpc_method"],
)

# Which gRPC status each kind of booking-rule problem becomes.
# Anything not listed is INVALID_ARGUMENT (the gRPC equivalent of HTTP 422).
GRPC_STATUS_FOR = {
    booking_core.AlreadyBooked: grpc.StatusCode.ALREADY_EXISTS,      # HTTP 409
    booking_core.BookingNotFound: grpc.StatusCode.NOT_FOUND,         # HTTP 404
    # The request itself is fine; it clashes with what the server already
    # remembers about this key. gRPC calls that FAILED_PRECONDITION
    # (INVALID_ARGUMENT is for input that is wrong whatever the state).
    booking_core.IdempotencyKeyReused: grpc.StatusCode.FAILED_PRECONDITION,   # HTTP 422
}


def grpc_status_for(err: booking_core.BookingError) -> grpc.StatusCode:
    return GRPC_STATUS_FOR.get(type(err), grpc.StatusCode.INVALID_ARGUMENT)


# ---------------------------------------------------------------------------
# The handler: implements the service defined in booking.proto
# ---------------------------------------------------------------------------
class BookingService(booking_pb2_grpc.BookingServiceServicer):

    def CreateBooking(self, request: booking_pb2.CreateBookingRequest, context: grpc.ServicerContext):
        student_ids = list(request.student_ids)
        try:
            booking, replayed = booking_core.create_booking(
                request.room, request.slot, student_ids,
                idempotency_key=request.idempotency_key or None,   # "" means "no key"
            )
        except booking_core.BookingError as err:
            code = grpc_status_for(err)
            log_event("booking rejected", level="WARNING", rpc="CreateBooking",
                      room=request.room, slot=request.slot, group_size=len(student_ids),
                      grpc_code=code.name, reason=err.reason)
            # The machine-readable reason travels in trailing metadata, the
            # human-readable message as the status details.
            context.set_trailing_metadata((("error-code", err.reason),))
            context.abort(code, err.message)
        except db.StorageBusy:                         # database busy: nothing written, safe to retry
            log_event("storage busy", level="WARNING", rpc="CreateBooking",
                      grpc_code="UNAVAILABLE", reason="storage_busy")
            context.set_trailing_metadata((("error-code", "storage_busy"),))
            context.abort(grpc.StatusCode.UNAVAILABLE, "The booking database is busy right now; please retry.")
        except Exception as exc:                       # anything unexpected
            log_event("unhandled error", level="ERROR", rpc="CreateBooking", error=repr(exc))
            context.abort(grpc.StatusCode.INTERNAL, "Something went wrong on our side.")

        if replayed:
            IDEMPOTENT_REPLAYS.inc()
        else:
            BOOKINGS_TOTAL.inc()
        log_event("booking replayed" if replayed else "booking confirmed", level="INFO",
                  rpc="CreateBooking", booking_id=booking["id"], room=booking["room"],
                  slot=booking["slot"], group_size=booking["group_size"], grpc_code="OK",
                  idempotent_replay=replayed)
        return booking_pb2.Booking(
            id=booking["id"],
            room=booking["room"],
            slot=booking["slot"],
            student_ids=booking["student_ids"],
            group_size=booking["group_size"],
            created_at=booking["created_at"],
            replayed=replayed,
        )


# ---------------------------------------------------------------------------
# Trace correlation across the gRPC boundary
# gRPC has no HTTP headers of its own; the equivalent is call *metadata*.
# The client sends "traceparent" (W3C Trace Context, as in the course lab) and
# "x-request-id" there. This interceptor runs before the handler: it continues
# the caller's trace with a new span, puts both IDs in the logging context so
# every log line for this call carries them, and sends them back to the client
# in the response metadata.
# ---------------------------------------------------------------------------
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9-]{1,64}$")


class TracingInterceptor(grpc.ServerInterceptor):

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler
        if handler_call_details.method.startswith(f"/{HEALTH_SERVICE}/"):
            return handler

        metadata = dict(handler_call_details.invocation_metadata or ())
        incoming_request_id = metadata.get("x-request-id", "")
        incoming_traceparent = metadata.get("traceparent")
        original = handler.unary_unary

        def traced(request, context):
            request_id = (incoming_request_id if _SAFE_REQUEST_ID.match(incoming_request_id)
                          else uuid.uuid4().hex[:16])
            span = child_span(incoming_traceparent)
            context.send_initial_metadata((
                ("x-request-id", request_id),
                ("traceparent", span["traceparent"]),
                ("x-replica", REPLICA),          # A3: which replica answered
            ))
            request_token = request_id_var.set(request_id)
            trace_token = trace_context_var.set(span)
            try:
                return original(request, context)
            finally:
                request_id_var.reset(request_token)
                trace_context_var.reset(trace_token)

        return grpc.unary_unary_rpc_method_handler(
            traced,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class MetricsInterceptor(grpc.ServerInterceptor):
    """Counts every RPC by status code, times it, and tracks how many are in progress."""

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler      # unknown method: gRPC itself answers UNIMPLEMENTED

        # "/studyslot.v1.BookingService/CreateBooking" -> service, method
        service, method = handler_call_details.method.lstrip("/").split("/", 1)
        if service == HEALTH_SERVICE:
            return handler      # Envoy's health checks are not API traffic
        original = handler.unary_unary

        def measured(request, context):
            in_flight = GRPC_IN_FLIGHT.labels(grpc_service=service, grpc_method=method)
            in_flight.inc()
            start = time.perf_counter()
            code = grpc.StatusCode.UNKNOWN
            try:
                # A3 chaos: a slow replica. Inside the timing and the in-flight
                # gauge, so this replica's p95 and in-flight count show it.
                delay = chaos.delay_seconds()
                if delay:
                    time.sleep(delay)
                response = original(request, context)
                code = context.code() or grpc.StatusCode.OK
                return response
            except Exception:
                # context.abort() raises; the status it set is still on the context.
                code = context.code() or grpc.StatusCode.UNKNOWN
                raise
            finally:
                GRPC_DURATION.labels(grpc_service=service, grpc_method=method).observe(
                    time.perf_counter() - start)
                GRPC_HANDLED.labels(grpc_service=service, grpc_method=method,
                                    grpc_code=code.name).inc()
                in_flight.dec()

        return grpc.unary_unary_rpc_method_handler(
            measured,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


# ---------------------------------------------------------------------------
# Server start-up
# ---------------------------------------------------------------------------
def create_server(address: str = f"[::]:{GRPC_PORT}") -> tuple[grpc.Server, int]:
    """Build the server (not started yet). Returns it and the port it bound."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        # Order matters: MetricsInterceptor is outermost, so its timing covers everything.
        interceptors=[MetricsInterceptor(), TracingInterceptor()],
    )
    booking_pb2_grpc.add_BookingServiceServicer_to_server(BookingService(), server)

    # A3: the standard health service (grpc.health.v1). Envoy calls Check()
    # on it; SERVING means "send me traffic". "" is the whole server.
    health_servicer = health.HealthServicer()
    for name in ("", "studyslot.v1.BookingService"):
        health_servicer.set(name, health_pb2.HealthCheckResponse.SERVING)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    server.health_servicer = health_servicer   # kept so shutdown can report NOT_SERVING

    port = server.add_insecure_port(address)
    return server, port


def start_in_background(port: int = GRPC_PORT) -> grpc.Server:
    """A3: start the gRPC server inside another process (main.py's HTTP app).

    grpc.server() runs on its own worker threads, so it serves calls while
    Uvicorn serves HTTP in the same process. The Prometheus metrics share the
    process's default registry, so the HTTP app's /metrics page shows both.
    """
    server, bound = create_server(f"[::]:{port}")
    server.start()
    log_event("grpc server started", level="INFO", port=bound)
    return server


def stop(server: grpc.Server, grace: float = 2.0) -> None:
    """Stop accepting calls, tell health checkers we are going, finish in-flight calls."""
    server.health_servicer.enter_graceful_shutdown()   # every service -> NOT_SERVING
    server.stop(grace=grace).wait()
    log_event("grpc server stopped", level="INFO")


def serve() -> None:
    db.init_db()
    start_http_server(METRICS_PORT)          # /metrics for Prometheus, on its own port
    server, port = create_server()
    server.start()
    log_event("grpc server started", level="INFO", port=port, metrics_port=METRICS_PORT)

    # "docker compose stop" sends SIGTERM: finish in-flight calls (up to 5 s), then exit.
    signal.signal(signal.SIGTERM, lambda *_: server.stop(grace=5))
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
