"""Envoy configurations for Assignment 3, and a command to switch between them.

Every run in Parts A-D needs a slightly different Envoy setup (which replicas
are destinations, the balancing policy, weights, health checks). Writing six
full YAML files by hand would mean six copies of the same 200 lines that can
silently drift apart. Instead, this script builds them all from ONE
description below, so the runs differ ONLY in what the experiment changes.

    generate   write every variant to envoy/configs/<name>.yaml
    use NAME   copy envoy/configs/NAME.yaml to envoy/envoy.yaml (the file
               the Envoy container reads) and restart Envoy

Run from the repo root:
    .venv\\Scripts\\python scripts/envoy_config.py generate
    .venv\\Scripts\\python scripts/envoy_config.py use rr
    .venv\\Scripts\\python scripts/envoy_config.py use rr --no-restart   (only copy)

Variants (same for HTTP and gRPC unless noted):
    a-only         Part A/B: A is the only destination; B and C run but get nothing
    rr             Part C:   A, B, C, equal-weight round robin
    weighted       Part D:   round robin, weights A:B:C = 2:1:1
    least-request  Part D:   least request, equal weights
    rr-health      Part D:   round robin + active health checks (failure run)
    rr-tcp-grpc    extra:    gRPC listener as a plain TCP proxy, to show the
                             brief's note (all calls stick to one replica)

Things that are the same in every variant, on purpose:
    - no retry_policy on any route, so Envoy never retries (the brief asks
      for no retries during the tests);
    - circuit-breaker thresholds raised far above anything the tests reach,
      so Envoy's default limits (1024) never reject a request;
    - JSON access logs on stdout, shipped by Filebeat (Part B);
    - each replica addressed by its container name (studyslot-app-1 = A ...),
      resolved through Docker's DNS every second, so a restarted replica
      that comes back on a new IP address is found again.
"""
import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO / "envoy" / "configs"
ACTIVE = REPO / "envoy" / "envoy.yaml"

REPLICAS = {"A": "studyslot-app-1", "B": "studyslot-app-2", "C": "studyslot-app-3"}
HTTP_PORT, GRPC_PORT = 8000, 50051            # same ports as A2, so A2's callers work unchanged

# Active health-check settings (Part D, failure and recovery).
HEALTH = {
    "interval": "2s",            # how often each replica is checked
    "timeout": "1s",             # a check with no answer after 1 s counts as failed
    "unhealthy_threshold": 2,    # 2 failed checks in a row -> removed
    "healthy_threshold": 2,      # 2 passed checks in a row -> added back
}

DESCRIPTIONS = {
    "a-only": "Part A/B baseline: replica A only",
    "rr": "Part C: equal-weight round robin over A, B, C",
    "weighted": "Part D: weighted round robin, A:B:C = 2:1:1",
    "least-request": "Part D: least request (power of two choices), equal weights",
    "rr-health": "Part D: round robin + active health checks",
    "rr-tcp-grpc": "Extra: round robin, gRPC listener as plain TCP proxy",
}


# ---------------------------------------------------------------------------
# Access log: one JSON object per request, on stdout (Filebeat ships it).
# Field names follow the app's logs where they mean the same thing
# (service.name, replica, request_id), so one Kibana search finds both.
# ---------------------------------------------------------------------------
def http_access_log(interface: str) -> list:
    return [{
        "name": "envoy.access_loggers.stdout",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog",
            "log_format": {"json_format": {
                "@timestamp": "%START_TIME(%Y-%m-%dT%H:%M:%S.%3fZ)%",
                "service": {"name": "envoy"},
                "log": {"level": "info"},
                "message": "access",
                "network": {"protocol": interface},
                "http": {
                    "request": {"method": "%REQ(:METHOD)%"},
                    "response": {"status_code": "%RESPONSE_CODE%"},
                },
                "url": {"path": "%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%"},
                "grpc_status": "%GRPC_STATUS%",
                "response_flags": "%RESPONSE_FLAGS%",       # e.g. UF = upstream connection failure
                "duration_ms": "%DURATION%",
                "upstream": {
                    "cluster": "%UPSTREAM_CLUSTER%",
                    "host": "%UPSTREAM_HOST%",              # IP:port Envoy picked
                    "name": "%UPSTREAM_HOST_NAME%",         # container name Envoy picked
                },
                "replica": "%RESP(X-REPLICA)%",             # the replica's own answer
                "request_id": "%REQ(X-REQUEST-ID)%",
                "traceparent": "%REQ(TRACEPARENT)%",        # Filebeat extracts trace.id from this
            }},
        },
    }]


def tcp_access_log() -> list:
    return [{
        "name": "envoy.access_loggers.stdout",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog",
            "log_format": {"json_format": {
                "@timestamp": "%START_TIME(%Y-%m-%dT%H:%M:%S.%3fZ)%",
                "service": {"name": "envoy"},
                "log": {"level": "info"},
                "message": "tcp connection",
                "network": {"protocol": "grpc-tcp"},
                "duration_ms": "%DURATION%",
                "upstream": {"cluster": "%UPSTREAM_CLUSTER%", "host": "%UPSTREAM_HOST%",
                             "name": "%UPSTREAM_HOST_NAME%"},
            }},
        },
    }]


# ---------------------------------------------------------------------------
# Listeners: where clients connect.
# ---------------------------------------------------------------------------
def http_connection_manager(name: str, cluster: str, interface: str, http2: bool) -> dict:
    """An HTTP (layer 7) listener: Envoy reads every request and routes each one separately."""
    hcm = {
        "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
        "stat_prefix": name,
        "codec_type": "AUTO",
        # Envoy is the edge: it appends the address it received the request from
        # to X-Forwarded-For (the rate limiter reads it), and keeps an
        # X-Request-ID the client sent instead of replacing it.
        "use_remote_address": True,
        "preserve_external_request_id": True,
        "access_log": http_access_log(interface),
        "route_config": {
            "name": f"{name}_route",
            "virtual_hosts": [{
                "name": "studyslot",
                "domains": ["*"],
                # No retry_policy: Envoy does not retry (brief: no retries during tests).
                "routes": [{"match": {"prefix": "/"}, "route": {"cluster": cluster, "timeout": "5s"}}],
            }],
        },
        "http_filters": [{
            "name": "envoy.filters.http.router",
            "typed_config": {"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"},
        }],
    }
    if http2:
        # gRPC: accept HTTP/2 from clients. Envoy then sees every call (stream)
        # on the client's single connection and routes each call separately.
        hcm["http2_protocol_options"] = {}
    return {
        "name": name,
        "address": {"socket_address": {"address": "0.0.0.0", "port_value": HTTP_PORT if not http2 else GRPC_PORT}},
        "filter_chains": [{"filters": [{"name": "envoy.filters.network.http_connection_manager",
                                        "typed_config": hcm}]}],
    }


def tcp_proxy_listener(name: str, cluster: str) -> dict:
    """A plain TCP (layer 4) listener: Envoy only sees connections, never the calls inside them."""
    return {
        "name": name,
        "address": {"socket_address": {"address": "0.0.0.0", "port_value": GRPC_PORT}},
        "filter_chains": [{"filters": [{
            "name": "envoy.filters.network.tcp_proxy",
            "typed_config": {
                "@type": "type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy",
                "stat_prefix": name,
                "cluster": cluster,
                "access_log": tcp_access_log(),
            },
        }]}],
    }


# ---------------------------------------------------------------------------
# Clusters: the replicas a listener sends to, and how it chooses.
# ---------------------------------------------------------------------------
def cluster(name: str, port: int, replicas: dict, policy: str, health: str | None,
            http2_upstream: bool) -> dict:
    """replicas maps letter -> weight. health is None, "http" or "grpc"."""
    endpoints = [{
        "endpoint": {"address": {"socket_address": {"address": REPLICAS[letter], "port_value": port}}},
        "load_balancing_weight": weight,
    } for letter, weight in replicas.items()]
    c = {
        "name": name,
        # Look the container names up in Docker's DNS (Envoy's "strict DNS"
        # cluster: one host per name), and re-resolve every second, because a
        # restarted replica may come back on a different IP address.
        "cluster_type": {
            "name": "envoy.clusters.dns",
            "typed_config": {
                "@type": "type.googleapis.com/envoy.extensions.clusters.dns.v3.DnsCluster",
                "dns_refresh_rate": "1s",
                "dns_lookup_family": "V4_ONLY",
            },
        },
        "connect_timeout": "1s",
        "lb_policy": policy,
        "load_assignment": {"cluster_name": name, "endpoints": [{"lb_endpoints": endpoints}]},
        "circuit_breakers": {"thresholds": [{
            # Far above anything the tests reach, so Envoy's circuit breaker never trips.
            "max_connections": 1_000_000,
            "max_pending_requests": 1_000_000,
            "max_requests": 1_000_000,
            "max_retries": 0,                 # and no retries at all
        }]},
        "common_lb_config": {
            # Envoy's default, written out because it matters in the failure run:
            # if fewer than 50 % of replicas are healthy, Envoy stops trusting the
            # health checks and balances across ALL replicas ("panic mode").
            "healthy_panic_threshold": {"value": 50.0},
        },
    }
    if policy == "LEAST_REQUEST":
        # Envoy's least request: pick 2 replicas at random, send to the one with
        # fewer active requests ("power of two choices"). 2 is the default.
        c["least_request_lb_config"] = {"choice_count": 2}
    if http2_upstream:
        # gRPC needs HTTP/2 between Envoy and the replicas as well.
        c["typed_extension_protocol_options"] = {
            "envoy.extensions.upstreams.http.v3.HttpProtocolOptions": {
                "@type": "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
                "explicit_http_config": {"http2_protocol_options": {}},
            }
        }
    if health:
        check = {
            **HEALTH,
            # Also check at the normal interval before any traffic has arrived
            # (Envoy's default for that case is every 60 s).
            "no_traffic_interval": HEALTH["interval"],
            # Every removal and re-addition is written to stdout as JSON, with a
            # timestamp, so Kibana shows exactly when Envoy changed its mind.
            "event_logger": [{
                "name": "envoy.health_check.event_sink.file",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.health_check.event_sinks.file.v3.HealthCheckEventFileSink",
                    "event_log_path": "/dev/stdout",
                },
            }],
            "always_log_health_check_failures": True,
        }
        if health == "http":
            check["http_health_check"] = {"path": "/health"}
        else:
            check["grpc_health_check"] = {"service_name": ""}    # grpc.health.v1, whole server
        c["health_checks"] = [check]
    return c


def build(variant: str, weights: tuple[int, int, int]) -> dict:
    equal = {"A": 1, "B": 1, "C": 1}
    replicas = {
        "a-only": {"A": 1},
        "weighted": dict(zip("ABC", weights)),
    }.get(variant, equal)
    policy = "LEAST_REQUEST" if variant == "least-request" else "ROUND_ROBIN"
    with_health = variant == "rr-health"
    tcp_grpc = variant == "rr-tcp-grpc"

    listeners = [http_connection_manager("ingress_http", "studyslot_http", "http", http2=False)]
    clusters = [cluster("studyslot_http", HTTP_PORT, replicas, policy,
                        "http" if with_health else None, http2_upstream=False)]
    if tcp_grpc:
        listeners.append(tcp_proxy_listener("ingress_grpc", "studyslot_grpc"))
        clusters.append(cluster("studyslot_grpc", GRPC_PORT, replicas, policy, None, http2_upstream=False))
    else:
        listeners.append(http_connection_manager("ingress_grpc", "studyslot_grpc", "grpc", http2=True))
        clusters.append(cluster("studyslot_grpc", GRPC_PORT, replicas, policy,
                                "grpc" if with_health else None, http2_upstream=True))

    return {
        # Admin page: /stats/prometheus (scraped by Prometheus), /clusters
        # (health of each replica), /config_dump. Only inside the Docker network
        # and on localhost:9901; never expose this in production.
        "admin": {"address": {"socket_address": {"address": "0.0.0.0", "port_value": 9901}}},
        "static_resources": {"listeners": listeners, "clusters": clusters},
    }


def write_all(weights: tuple[int, int, int]) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    for variant, description in DESCRIPTIONS.items():
        header = (f"# {description}\n"
                  f"# GENERATED by scripts/envoy_config.py - edit the script, not this file.\n")
        body = yaml.safe_dump(build(variant, weights), sort_keys=False, width=120)
        (CONFIG_DIR / f"{variant}.yaml").write_text(header + body, encoding="utf-8", newline="\n")
        print(f"wrote envoy/configs/{variant}.yaml")


def use(variant: str, restart: bool) -> None:
    source = CONFIG_DIR / f"{variant}.yaml"
    if not source.exists():
        sys.exit(f"{source} not found. Run:  python scripts/envoy_config.py generate")
    shutil.copyfile(source, ACTIVE)
    print(f"envoy/envoy.yaml <- envoy/configs/{variant}.yaml ({DESCRIPTIONS[variant]})")
    if restart:
        subprocess.run(["docker", "compose", "restart", "envoy"], cwd=REPO, check=True)
        print("Envoy restarted. Check http://localhost:9901/clusters before starting a run.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate or switch Envoy configurations.")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="write every variant to envoy/configs/")
    gen.add_argument("--weights", default="2,1,1", help="A,B,C weights for 'weighted' (default 2,1,1)")
    use_p = sub.add_parser("use", help="make one variant active and restart Envoy")
    use_p.add_argument("variant", choices=list(DESCRIPTIONS))
    use_p.add_argument("--no-restart", action="store_true", help="only copy the file")
    args = parser.parse_args()

    if args.command == "generate":
        weights = tuple(int(w) for w in args.weights.split(","))
        if len(weights) != 3 or min(weights) < 1:
            sys.exit("--weights needs three positive whole numbers, e.g. 2,1,1")
        write_all(weights)
    else:
        use(args.variant, restart=not args.no_restart)


if __name__ == "__main__":
    main()