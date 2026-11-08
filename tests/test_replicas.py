"""Tests for the Assignment 3 changes: replica identity, health, delay, rate-limit client."""
import time

import grpc
from fastapi.testclient import TestClient
from grpc_health.v1 import health_pb2, health_pb2_grpc
from prometheus_client import REGISTRY

import booking_pb2
import booking_pb2_grpc
import chaos
import grpc_server
import main
from replica import CONTAINER, letter_for

SERVICE = "studyslot.v1.BookingService"


def sample(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


# --- Replica identity --------------------------------------------------------------
def test_container_number_becomes_a_letter():
    assert letter_for("studyslot-app-1.studyslot_default.") == "A"
    assert letter_for("studyslot-app-2") == "B"
    assert letter_for("studyslot-app-3.studyslot_default") == "C"
    assert letter_for("4f1c2e9ab7d3") is None          # a container ID has no "-N" at the end


def test_every_response_names_the_replica(client):
    assert client.get("/rooms").headers["X-Replica"] == "T"          # set in conftest.py
    assert client.get("/bookings/not-a-uuid").headers["X-Replica"] == "T"   # errors too


def test_replica_info_metric(client):
    assert sample("studyslot_replica_info", replica_name="T", container=CONTAINER) == 1


# --- Health checks and chaos calls are not traffic --------------------------------
def test_health_and_chaos_calls_are_not_measured(client):
    client.get("/health")
    client.get("/chaos")
    text = client.get("/metrics").text
    assert 'route="/health"' not in text
    assert 'route="/chaos' not in text


# --- The chaos delay slows every API call on the replica --------------------------
def test_delay_slows_http_calls_and_shows_in_server_latency(client):
    labels = {"method": "GET", "route": "/rooms"}
    before = sample("http_request_duration_seconds_sum", **labels)
    client.put("/chaos/delay", json={"seconds": 0.2})
    start = time.perf_counter()
    client.get("/rooms")
    assert time.perf_counter() - start >= 0.2
    assert sample("http_request_duration_seconds_sum", **labels) - before >= 0.2
    client.put("/chaos/delay", json={"seconds": 0})


def test_grpc_health_replica_metadata_and_delay(client):
    server, port = grpc_server.create_server("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            # Health: SERVING, and health calls are not counted as API traffic.
            health_stub = health_pb2_grpc.HealthStub(channel)
            reply = health_stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=2)
            assert reply.status == health_pb2.HealthCheckResponse.SERVING
            assert sample("grpc_server_handled_total", grpc_service="grpc.health.v1.Health",
                          grpc_method="Check", grpc_code="OK") == 0

            # x-replica comes back in the response metadata.
            stub = booking_pb2_grpc.BookingServiceStub(channel)
            request = booking_pb2.CreateBookingRequest(room="R4", slot="11:00",
                                                       student_ids=["a", "b", "c"],
                                                       idempotency_key="replica-test-01")
            _, call = stub.CreateBooking.with_call(request, timeout=2)
            assert dict(call.initial_metadata())["x-replica"] == "T"

            # The same chaos switch slows gRPC too.
            chaos.set_delay(0.2)
            start = time.perf_counter()
            booking = stub.CreateBooking(request, timeout=2)
            assert time.perf_counter() - start >= 0.2
            assert booking.replayed
    finally:
        chaos.set_delay(0)
        grpc_server.stop(server, grace=0)
    # After a graceful stop the health service reports NOT_SERVING.
    assert server.health_servicer._server_status[""] == health_pb2.HealthCheckResponse.NOT_SERVING


# --- Behind Envoy, the rate limiter must not see one shared caller ----------------
def test_rate_limit_uses_last_x_forwarded_for_entry(client, monkeypatch):
    monkeypatch.setattr(main, "TRUST_X_FORWARDED_FOR", True)
    alice = {"X-Forwarded-For": "10.0.0.1"}
    # Bob tries to pretend to be Alice by putting her address first; Envoy appends his real one.
    bob = {"X-Forwarded-For": "10.0.0.1, 10.0.0.2"}
    for _ in range(5):
        assert client.get("/availability?slot=09:00", headers=alice).status_code == 200
    assert client.get("/availability?slot=09:00", headers=alice).status_code == 429
    assert client.get("/availability?slot=09:00", headers=bob).status_code == 200


def test_header_ignored_unless_trusted(client):
    assert main.TRUST_X_FORWARDED_FOR is False        # default outside docker-compose
    for i in range(5):
        client.get("/availability?slot=09:00", headers={"X-Forwarded-For": f"10.0.0.{i}"})
    # Five different "addresses", but without trust they are all the same caller.
    assert client.get("/availability?slot=09:00", headers={"X-Forwarded-For": "10.0.0.9"}).status_code == 429


# --- One process serves both interfaces --------------------------------------------
def test_app_starts_and_stops_its_grpc_server(monkeypatch):
    started = {}
    real_start, real_stop = grpc_server.start_in_background, grpc_server.stop

    def fake_start(port):
        started["server"] = real_start(0)          # any free port
        return started["server"]

    def fake_stop(server, grace=2.0):
        started["stopped"] = True
        real_stop(server, grace=0)

    monkeypatch.setattr(main, "GRPC_PORT", "50051")
    monkeypatch.setattr(grpc_server, "start_in_background", fake_start)
    monkeypatch.setattr(grpc_server, "stop", fake_stop)
    with TestClient(main.app) as c:                 # "with" runs start-up and shut-down
        assert c.get("/health").status_code == 200
        assert "server" in started
    assert started.get("stopped") is True