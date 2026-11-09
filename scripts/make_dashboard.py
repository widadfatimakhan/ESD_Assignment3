"""Build the Assignment 3 Grafana dashboard (Part B).

Writes monitoring/grafana/provisioning/dashboards/studyslot-a3-load-balancing.json,
which Grafana loads automatically. Building the JSON from this script keeps the
HTTP and gRPC halves identical apart from the metric names.

Run from the repo root after changing a panel:
    .venv\\Scripts\\python scripts/make_dashboard.py
then restart Grafana (docker compose restart grafana) or wait for it to reload.
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "PBFA97CFB590B2093"}
panels = []
pid = [0]

def nid():
    pid[0] += 1
    return pid[0]

def target(expr, legend, ref, instant=False):
    t = {"datasource": DS, "editorMode": "code", "expr": expr, "legendFormat": legend, "refId": ref}
    if instant:
        t.update({"instant": True, "range": False})
    else:
        t["range"] = True
    return t

def timeseries(title, desc, targets, x, y, w=12, h=8, unit="short", minv=0, max_=None, thresholds_lines=None, overrides=None):
    steps = [{"color": "transparent", "value": None}]
    if thresholds_lines:
        steps += [{"color": "rgba(150,150,150,0.8)", "value": v} for v in thresholds_lines]
    defaults = {
        "color": {"mode": "palette-classic"},
        "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 5, "showPoints": "never",
                   "spanNulls": False, "axisPlacement": "auto",
                   "hideFrom": {"legend": False, "tooltip": False, "viz": False},
                   "scaleDistribution": {"type": "linear"},
                   "stacking": {"group": "A", "mode": "none"},
                   "thresholdsStyle": {"mode": "dashed" if thresholds_lines else "off"}},
        "mappings": [], "unit": unit, "min": minv,
        "thresholds": {"mode": "absolute", "steps": steps},
    }
    if max_ is not None:
        defaults["max"] = max_
    return {"datasource": DS, "description": desc, "type": "timeseries", "title": title, "id": nid(),
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": {"legend": {"calcs": ["mean", "max", "lastNotNull"], "displayMode": "table",
                                   "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
            "targets": targets}

def bargauge(title, desc, targets, x, y, w=6, h=7, unit="percent", max_=100, decimals=1):
    return {"datasource": DS, "description": desc, "type": "bargauge", "title": title, "id": nid(),
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "unit": unit, "min": 0,
                                         "max": max_, "decimals": decimals, "mappings": [],
                                         "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]}},
                            "overrides": []},
            "options": {"displayMode": "basic", "orientation": "horizontal", "showUnfilled": True,
                        "valueMode": "text", "namePlacement": "left", "minVizHeight": 10, "minVizWidth": 0,
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
            "targets": targets}

def stat(title, desc, targets, x, y, w=6, h=5, mappings=None, thresholds=None, unit="none"):
    return {"datasource": DS, "description": desc, "type": "stat", "title": title, "id": nid(),
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "unit": unit, "mappings": mappings or [],
                                         "thresholds": thresholds or {"mode": "absolute", "steps": [{"color": "green", "value": None}]}},
                            "overrides": []},
            "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "center",
                        "orientation": "horizontal", "textMode": "value_and_name", "wideLayout": True,
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
            "targets": targets}

def row(title, y):
    return {"type": "row", "title": title, "collapsed": False, "id": nid(),
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}

HTTP_SEL = 'method="GET", route="/bookings/{booking_id}"'
GRPC_SEL = 'grpc_method="CreateBooking"'
iface = {
    "HTTP": {"x": 0, "name": "HTTP GET /bookings/{id}",
             "count": f'http_requests_total{{{HTTP_SEL}}}',
             "err": f'http_requests_total{{{HTTP_SEL}, status!~"2.."}}', "errlabel": "status",
             "inflight": f'http_requests_in_flight{{{HTTP_SEL}}}',
             "bucket": f'http_request_duration_seconds_bucket{{{HTTP_SEL}}}',
             "cluster": "studyslot_http"},
    "gRPC": {"x": 12, "name": "gRPC CreateBooking",
             "count": f'grpc_server_handled_total{{{GRPC_SEL}}}',
             "err": f'grpc_server_handled_total{{{GRPC_SEL}, grpc_code!="OK"}}', "errlabel": "grpc_code",
             "inflight": f'grpc_server_in_flight_requests{{{GRPC_SEL}}}',
             "bucket": f'grpc_server_handling_seconds_bucket{{{GRPC_SEL}}}',
             "cluster": "studyslot_grpc"},
}

panels.append({"type": "text", "title": "", "id": nid(), "gridPos": {"h": 4, "w": 24, "x": 0, "y": 0},
               "options": {"mode": "markdown", "content":
    "**StudySlot A3: load balancing across replicas A, B, C.** HTTP on the left, gRPC on the right. "
    "Test operations: HTTP `GET /bookings/{id}` and gRPC `CreateBooking` (idempotent replay). "
    "Prometheus scrapes each replica directly (`studyslot-app-1/2/3`, label `replica`) and Envoy's admin port, never through Envoy. "
    "Rates and percentiles use a **1-minute window** (`[1m]`). "
    "The *share over the selected time range* panels use `$__range`: set the time picker to a run's start and end "
    "(printed by `scripts/load_test.py`) to read that run's A/B/C split. This dashboard shows times in **UTC**, like the results files."}})

y = 4
panels.append(row("Replicas and Envoy", y)); y += 1
panels.append(stat("Replica up (scraped directly)",
    "Prometheus `up` for each replica target. 0 means Prometheus could not scrape it (stopped or unreachable).",
    [target('up{job="studyslot"}', "{{replica}}", "A")], 0, y, w=6, h=5,
    mappings=[{"type": "value", "options": {"0": {"text": "DOWN", "color": "red"}, "1": {"text": "UP", "color": "green"}}}],
    thresholds={"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]}))
panels.append(stat("Envoy: healthy replicas per cluster",
    "envoy_cluster_membership_healthy: how many replicas Envoy currently sends traffic to, out of envoy_cluster_membership_total.",
    [target('envoy_cluster_membership_healthy{envoy_cluster_name=~"studyslot_.*"}', "{{envoy_cluster_name}}", "A")],
    6, y, w=6, h=5,
    thresholds={"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "orange", "value": 1}, {"color": "green", "value": 3}]}))
panels.append(timeseries("Envoy: health-check failures and connection failures per second",
    "Active health checks that failed, and connections to a replica that could not be opened (e.g. the replica is stopped). 1m window.",
    [target('sum by (envoy_cluster_name) (rate(envoy_cluster_health_check_failure{envoy_cluster_name=~"studyslot_.*"}[1m]))', "health-check failures {{envoy_cluster_name}}", "A"),
     target('sum by (envoy_cluster_name) (rate(envoy_cluster_upstream_cx_connect_fail{envoy_cluster_name=~"studyslot_.*"}[1m]))', "connect failures {{envoy_cluster_name}}", "B")],
    12, y, w=12, h=5, unit="ops"))
panels.append(stat("Replica identity (self-reported vs scrape label)",
    "studyslot_replica_info: the letter each replica worked out for itself (replica_name) next to the label Prometheus gave its target (replica). They should match.",
    [target('studyslot_replica_info', "scrape {{replica}} = self {{replica_name}} ({{container}})", "A")], 0, y + 5, w=24, h=3,
    thresholds={"mode": "absolute", "steps": [{"color": "blue", "value": None}]}))
y += 8

for label, d in iface.items():
    x = d["x"]
    yy = y
    if label == "HTTP":
        panels.append(row("Traffic per replica (HTTP left, gRPC right)", yy))
    yy += 1
    panels.append(timeseries(f"{d['name']}: requests per second by replica",
        f"Calls completed per second on each replica, from the replica's own counter ({d['count']}), and the total. 1m window.",
        [target(f"sum by (replica) (rate({d['count']}[1m]))", "{{replica}}", "A"),
         target(f"sum(rate({d['count']}[1m]))", "total", "B")],
        x, yy, unit="reqps",
        overrides=[{"matcher": {"id": "byName", "options": "total"},
                    "properties": [{"id": "custom.lineStyle", "value": {"dash": [10, 10], "fill": "dash"}},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "text"}}]}]))
    yy += 8
    panels.append(timeseries(f"{d['name']}: share of traffic by replica (%)",
        "Each replica's rate divided by the total rate, 1m window. Dashed lines at 25 %, 33.3 % and 50 % (the targets for equal and 2:1:1 weights).",
        [target(f"100 * sum by (replica) (rate({d['count']}[1m])) / ignoring(replica) group_left sum(rate({d['count']}[1m]))", "{{replica}}", "A")],
        x, yy, unit="percent", max_=100, thresholds_lines=[25, 33.33, 50]))
    yy += 8
    panels.append(bargauge(f"{d['name']}: share over the selected time range",
        "increase() of each replica's counter over the whole time range shown ($__range), as a % of the total. Set the time range to one run.",
        [target(f"100 * sum by (replica) (increase({d['count']}[$__range])) / ignoring(replica) group_left sum(increase({d['count']}[$__range]))", "{{replica}}", "A", instant=True)],
        x, yy, w=6))
    panels.append(bargauge(f"{d['name']}: requests over the selected time range",
        "increase() of each replica's counter over the time range. Prometheus extrapolates to the range edges, so treat as approximate; the exact counts are in the load_test results and Envoy's logs.",
        [target(f"sum by (replica) (increase({d['count']}[$__range]))", "{{replica}}", "A", instant=True)],
        x + 6, yy, w=6, unit="short", max_=None, decimals=0))
    yy += 7
    panels.append(timeseries(f"{d['name']}: active requests by replica",
        f"In-flight gauge ({d['inflight']}). Solid: average over 1m (smooths the 5 s samples). Dotted: the raw sample at each scrape. "
        f"Dashed grey: Envoy's active requests to this cluster (all replicas together).",
        [target(f"sum by (replica) (avg_over_time({d['inflight']}[1m]))", "{{replica}} (1m avg)", "A"),
         target(f"sum by (replica) ({d['inflight']})", "{{replica}} (sample)", "B"),
         target(f'sum(envoy_cluster_upstream_rq_active{{envoy_cluster_name="{d["cluster"]}"}})', "Envoy active (cluster)", "C")],
        x, yy, unit="short",
        overrides=[{"matcher": {"id": "byRegexp", "options": ".*sample.*"},
                    "properties": [{"id": "custom.lineStyle", "value": {"dash": [2, 6], "fill": "dot"}},
                                   {"id": "custom.lineWidth", "value": 1}]},
                   {"matcher": {"id": "byName", "options": "Envoy active (cluster)"},
                    "properties": [{"id": "custom.lineStyle", "value": {"dash": [10, 10], "fill": "dash"}},
                                   {"id": "color", "value": {"mode": "fixed", "fixedColor": "text"}}]}]))
    yy += 8
    panels.append(timeseries(f"{d['name']}: p50 / p95 latency by replica (server side, 1m window)",
        f"histogram_quantile over {d['bucket']}, per replica, rate window 1m. Measured inside the replica, so it excludes Envoy and the network (the client's p95 is in the load_test output).",
        [target(f"histogram_quantile(0.50, sum by (le, replica) (rate({d['bucket']}[1m])))", "{{replica}} p50", "A"),
         target(f"histogram_quantile(0.95, sum by (le, replica) (rate({d['bucket']}[1m])))", "{{replica}} p95", "B")],
        x, yy, unit="s"))
    yy += 8
    panels.append(timeseries(f"{d['name']}: errors per second by replica and status",
        f"Non-success results counted by each replica ({d['err']}), plus requests Envoy itself failed because no replica answered "
        f"(upstream 5xx on cluster {d['cluster']}, e.g. a stopped replica). For gRPC, Envoy still answers HTTP 200 and puts the error "
        f"in grpc-status (UNAVAILABLE), but its cluster counts the failed upstream request as 503. 1m window.",
        [target(f"sum by (replica, {d['errlabel']}) (rate({d['err']}[1m]))", "{{replica}} {{" + d['errlabel'] + "}}", "A"),
         target(f'sum(rate(envoy_cluster_upstream_rq_xx{{envoy_cluster_name="{d["cluster"]}", envoy_response_code_class="5"}}[1m]))', "Envoy: no replica answered (5xx)", "B")],
        x, yy, unit="reqps"))
    end_y = yy + 8

dash = {
    "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                              "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"}]},
    "editable": True, "fiscalYearStartMonth": 0, "graphTooltip": 1, "id": None, "links": [], "liveNow": False,
    "panels": panels, "refresh": "5s", "schemaVersion": 38, "tags": ["studyslot", "assignment-3"],
    "templating": {"list": []}, "time": {"from": "now-15m", "to": "now"}, "timepicker": {}, "timezone": "utc",
    "title": "StudySlot A3: load balancing (HTTP and gRPC per replica)", "uid": "studyslot-a3-lb", "version": 1, "weekStart": "",
}
out = Path(__file__).resolve().parent.parent / "monitoring/grafana/provisioning/dashboards/studyslot-a3-load-balancing.json"
out.write_text(json.dumps(dash, indent=2) + "\n", encoding="utf-8")
print(f"wrote {out.name}: {len(panels)} panels")