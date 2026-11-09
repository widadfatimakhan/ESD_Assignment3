# StudySlot (ESD Assignments)

A campus study-room booking service (Python + FastAPI) with a gRPC version of
its booking operation, run as three replicas behind an Envoy load balancer and
observed with Prometheus + Grafana (metrics) and Filebeat + Elasticsearch +
Kibana (logs), all with Docker Compose.

- **Assignment 3** (load balancing and observability): [REPORT.md](REPORT.md)
- **Assignment 2** (HTTP APIs, gRPC, metrics): [docs/a2/REPORT.md](docs/a2/REPORT.md). The code as submitted for A2 is at git tag `a2-submitted`.
- **Assignment 1** (observability): [docs/a1/REPORT.md](docs/a1/REPORT.md)

## What it does
Students book a limited group study room for a time slot, listing their group's
student IDs. The service enforces room size limits and prevents double-booking.
Bookings can be made over HTTP (`POST /bookings`) or gRPC
(`BookingService.CreateBooking`). Both use the same rules
(`studyslot/booking_core.py`) and the same SQLite database (Docker volume
`studyslot-data`).

Since Assignment 3, one container image runs **three replicas (A, B, C)**,
each serving HTTP and gRPC from one process. Clients never call a replica
directly: **Envoy** listens on A2's ports (8000 HTTP, 50051 gRPC) and forwards
each request or call to a replica, so every A2 caller works unchanged.

| Container | Port(s) on localhost | Role |
|---|---|---|
| `studyslot-envoy` | 8000 (HTTP), 50051 (gRPC), 9901 (admin) | Load balancer in front of the replicas |
| `studyslot-app-1` / `-2` / `-3` = replicas **A** / **B** / **C** | none (Docker network only: 8000 HTTP + `/metrics`, 50051 gRPC) | The StudySlot service |
| `studyslot-prometheus` | 9090 | Scrapes each replica and Envoy directly |
| `studyslot-grafana` | 3000 | Dashboards (admin / admin) |
| `studyslot-elasticsearch`, `studyslot-kibana`, `studyslot-filebeat` | 9200, 5601 | Logs (replicas and Envoy) |
| `studyslot-node-exporter` | 9100 | Machine metrics |

## Requirements
- Docker Desktop with Docker Compose v2 (`docker compose version`)
- About 6 GB free RAM (Elasticsearch is the heavy component)
- Python 3.13, for the tests and the client scripts

## Setup
From the repo root:
```powershell
docker compose up -d --build
docker compose ps
```
All services should be up and the three `studyslot-app-N` replicas `(healthy)`.
Envoy waits for them before starting. If Grafana exited on first start (a
known Grafana 10.2 start-up race), run `docker compose start grafana`.

`deploy.replicas: 3` in `docker-compose.yml` is what starts three replicas.
The same thing on the command line: `docker compose up -d --scale app=3`.

One-time setup for the tests and client scripts (Windows; on macOS/Linux use
`.venv/bin/python`):
```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt
```

Check that everything is wired up:

| URL / command | Expect |
|---|---|
| `curl.exe -i http://localhost:8000/rooms` | `200` and an `X-Replica: A` header (Envoy starts in the A-only configuration) |
| `.venv\Scripts\python scripts/grpc_client.py --room R4 --slot 10:00 --students s1 s2 s3` | `OK`, through Envoy on 50051 |
| http://localhost:9901/clusters | Envoy's view of each replica (`health_flags::healthy`) |
| http://localhost:9090/targets | `studyslot` A, B, C, `envoy` and `node-exporter` all UP |
| http://localhost:3000 | Dashboard **StudySlot A3: load balancing (HTTP and gRPC per replica)** (the A1 and A2 dashboards are still there) |
| http://localhost:5601/app/discover | Data view `studyslot-logs-*`, timestamp `@timestamp` |

(In PowerShell, `curl` is an alias for `Invoke-WebRequest`; use `curl.exe`.)

The A1 and A2 dashboards still load, with two side effects of the replicas:
their "by API" panels show one line (HTTP and gRPC now share a process and a
Prometheus job), and per-replica metrics appear three times, once per replica.

## Envoy configurations
`scripts/envoy_config.py` builds every configuration from one description, so
the runs differ only in what each experiment changes. The generated files are
committed in `envoy/configs/`; Envoy reads `envoy/envoy.yaml`.

| Name | Used in | Destinations and policy |
|---|---|---|
| `a-only` | Parts A, B | A only (B and C run but get no traffic) |
| `rr` | Part C, and the normal state for Part D | A, B, C, equal-weight round robin |
| `weighted` | Part D | Round robin, weights A:B:C = 2:1:1 |
| `least-request` | Part D | Least request, equal weights |
| `rr-health` | Part D | Round robin + active health checks |
| `rr-tcp-grpc` | Extra | gRPC listener as a plain TCP proxy (the brief's note) |

```powershell
.venv\Scripts\python scripts/envoy_config.py use rr          # copy envoy/configs/rr.yaml to envoy/envoy.yaml, restart Envoy
.venv\Scripts\python scripts/envoy_config.py generate        # rebuild all configs after editing the script
```

In every configuration Envoy has **no retry policy** and its **circuit-breaker
limits are raised** far above the test load, so neither affects the results.
The load-test client does not retry either.

**Active health checks** (`rr-health` only):

| Setting | Value |
|---|---|
| HTTP cluster | `GET /health`, expects 200 |
| gRPC cluster | standard `grpc.health.v1.Health/Check`, expects `SERVING` |
| Interval | 2 s (also before any traffic: `no_traffic_interval`) |
| Timeout | 1 s |
| Unhealthy threshold | 2 failed checks in a row |
| Healthy threshold | 2 passed checks in a row |
| Panic threshold | 50 % (Envoy's default, written out) |
| Event log | every failure, removal and re-addition, as JSON on Envoy's stdout (shipped to Kibana) |

## Experiments
Every run uses the same workload: `scripts/load_test.py` sends HTTP
`GET /bookings/{id}` and gRPC `CreateBooking` (always replaying one
idempotency key) at a **fixed 40 requests per second each, for 90 seconds**,
without waiting for responses, with a 2 s timeout and no retries. It prints
the rate, requests sent, successes, failures, client p50/p95, and which replica
answered each request, and saves `results/<label>.json` plus every request in
`results/<label>-requests.csv`.

Let about a minute pass between runs, so the 1-minute rate windows on the
dashboard do not mix two runs. To read one run on the dashboard, set Grafana's
time range to the start and end the client prints (the dashboard is in UTC,
like the results files).

### Parts A and B: A-only baseline
```powershell
.venv\Scripts\python scripts/envoy_config.py use a-only
.venv\Scripts\python scripts/load_test.py --label B-baseline --policy "A only"
```

### Part C: round robin
```powershell
.venv\Scripts\python scripts/envoy_config.py use rr
.venv\Scripts\python scripts/load_test.py --label C-rr --policy "round robin"
```
Extra, showing why the gRPC listener must be HTTP/2:
```powershell
.venv\Scripts\python scripts/envoy_config.py use rr-tcp-grpc
.venv\Scripts\python scripts/load_test.py --label C-tcp-grpc --policy "round robin, gRPC as TCP proxy" --interface grpc --duration 30
.venv\Scripts\python scripts/envoy_config.py use rr
```

### Part D1: weighted round robin
```powershell
.venv\Scripts\python scripts/envoy_config.py use weighted
.venv\Scripts\python scripts/load_test.py --label D1-weighted --policy "weighted RR 2:1:1"
.venv\Scripts\python scripts/envoy_config.py use rr                     # restore
```

### Part D2: least request vs round robin, with B slowed down
```powershell
.venv\Scripts\python scripts/replica_chaos.py B --delay 0.2             # B adds 200 ms to every call
.venv\Scripts\python scripts/envoy_config.py use rr
.venv\Scripts\python scripts/load_test.py --label D2a-rr-delay --policy "round robin" --fault "B +200 ms"
.venv\Scripts\python scripts/envoy_config.py use least-request
.venv\Scripts\python scripts/load_test.py --label D2b-lr-delay --policy "least request" --fault "B +200 ms"
.venv\Scripts\python scripts/replica_chaos.py all --delay 0             # restore
.venv\Scripts\python scripts/envoy_config.py use rr
```
The delay is applied inside B, on both interfaces, and counts in B's own
latency and in-flight metrics. `python scripts/replica_chaos.py all` shows the
current settings of all three replicas.

### Part D3: failure and recovery
```powershell
.venv\Scripts\python scripts/envoy_config.py use rr-health
# check http://localhost:9901/clusters: every replica healthy
.venv\Scripts\python scripts/load_test.py --label D3-failure --policy "round robin + health checks" --fault "B stopped at 30 s, started at 60 s" --stop-replica B --stop-at 30 --start-at 60
.venv\Scripts\python scripts/envoy_config.py use rr                     # restore
```
The client runs `docker stop studyslot-app-2` and `docker start studyslot-app-2`
itself at the given seconds, records the exact times, and prints when errors
stopped and when B answered again (`failure timing`).

### Results table and log checks
```powershell
.venv\Scripts\python scripts/summarise_results.py                       # the Part D table, from results/*.json
.venv\Scripts\python scripts/envoy_log_counts.py results/C-rr.json      # A/B/C counts from Envoy's access logs, same time range
```

Kibana searches (KQL), using a `trace_id` from any `results/<label>-requests.csv`:

| Search | Returns |
|---|---|
| `trace.id : "<trace_id of an http row>"` | Envoy's access-log event (`service.name: envoy`, `replica`, `upstream.name`) and the replica's `request handled` event |
| `trace.id : "<trace_id of a grpc row>"` | Envoy's event and the replica's `booking replayed` event |
| `service.name : "envoy" and response_flags : "UF"` | Requests Envoy could not deliver (upstream connection failure) |
| `eject_unhealthy_event.failure_type : * or add_healthy_event : *` | Health-check removals and re-additions, with timestamps |
| `replica : "B"` | Everything B logged and every Envoy event B answered |

If an Envoy event has no `trace.id`, search its raw header instead:
`traceparent : *<trace_id>*`.

## Recovery
After any experiment, or if something is left in a strange state:

| Problem | Command |
|---|---|
| Back to the normal round-robin setup | `.venv\Scripts\python scripts/envoy_config.py use rr` |
| A replica is still slow | `.venv\Scripts\python scripts/replica_chaos.py all --delay 0` |
| B is still stopped | `docker start studyslot-app-2` (or `docker compose up -d`) |
| Envoy is not running | `docker compose up -d envoy`, then check http://localhost:9901/ready |
| A replica is unhealthy in http://localhost:9901/clusters | `docker compose logs app --tail 50`, then `docker compose restart app` |
| The load test's target booking is taken (setup fails with 409 / `ALREADY_EXISTS`) | Cancel the booking in that slot (R3 16:00 or 16:30) with `DELETE /bookings/{id}` |
| gRPC client complains about the event loop on Windows | Add `--selector-loop` to the `load_test.py` command |

## Test
```powershell
.venv\Scripts\python -m pytest -v
```
The tests run the real app in-process against a throwaway database, so Docker
does not need to be running. `tests/test_replicas.py` covers the Assignment 3
changes: replica identity, the `X-Replica` header and metadata, the gRPC health
service, the per-replica delay on both interfaces, health checks and chaos
calls not counted as traffic, and the rate limiter behind Envoy.

## Stop and clean up
- Pause (keeps everything): `docker compose stop`
- Remove containers, keep the bookings volume: `docker compose down`
- Remove everything, **including stored bookings**: `docker compose down -v`

## What changed in Assignment 3
| File | Change |
|---|---|
| `docker-compose.yml` | One `app` service with 3 replicas (replaces `studyslot-app` + `studyslot-grpc`); Envoy in front; project name `studyslot` fixes the container names |
| `studyslot/main.py` | Starts the gRPC server in the same process; `X-Replica` header; one `request handled` log line per request; metrics skip `/metrics`, `/health`, `/chaos*` (as in the Week 1 lab); rate limiter reads the last `X-Forwarded-For` entry |
| `studyslot/grpc_server.py` | Standard gRPC health service; `x-replica` metadata; `start_in_background()` / `stop()` |
| `studyslot/replica.py` | New: works out A/B/C from the container name (Docker's DNS) |
| `studyslot/chaos.py` | New: chaos switches shared by HTTP and gRPC; the delay now applies to every API call on that replica |
| `envoy/`, `scripts/envoy_config.py` | New: Envoy configurations |
| `monitoring/prometheus/prometheus.yml` | Scrapes `studyslot-app-1/2/3` directly with a `replica` label, and Envoy's `/stats/prometheus` |
| `monitoring/filebeat/filebeat.yml` | Also ships Envoy's JSON access log and health-check events; extracts `trace.id` from `traceparent` |
| `monitoring/grafana/.../studyslot-a3-load-balancing.json`, `scripts/make_dashboard.py` | New dashboard, and the script that builds it |
| `scripts/load_test.py`, `replica_chaos.py`, `summarise_results.py`, `envoy_log_counts.py` | New experiment tools |

## Metrics summary
| Metric | Type | Labels (plus `replica` from Prometheus) |
|---|---|---|
| `http_requests_total` | Counter | `method`, `route`, `status` |
| `http_request_duration_seconds` | Histogram | `method`, `route` |
| `http_requests_in_flight` | Gauge | `method`, `route` |
| `grpc_server_handled_total` | Counter | `grpc_service`, `grpc_method`, `grpc_code` |
| `grpc_server_handling_seconds` | Histogram | `grpc_service`, `grpc_method` |
| `grpc_server_in_flight_requests` | Gauge | `grpc_service`, `grpc_method` |
| `studyslot_bookings_total` | Counter | none |
| `studyslot_idempotent_replays_total` | Counter | none |
| `studyslot_rooms_occupied` | Gauge | none (reads the shared database, so every replica reports the same number: use `max`, not `sum`) |
| `studyslot_replica_info` | Gauge | `replica_name`, `container` (what the replica thinks it is) |
| `studyslot_availability_check_seconds` | Histogram | none (A1) |
| `envoy_cluster_*`, `envoy_http_*` | Envoy | `envoy_cluster_name`, ... |
| `node_*` | Node Exporter | machine CPU/memory/disk/network |

## Credits
- Course Week 1 lab (Midnight Launch): tool stack, JSON logging style, W3C
  `traceparent` propagation, Prometheus/Grafana provisioning, and the rule that
  request metrics skip `/metrics`, `/health` and `/chaos*`.
- Envoy documentation: load-balancing policies, health checking, access-log
  format, and the `dns` cluster type.
- AI assistance: Claude helped write part of the code and configuration.