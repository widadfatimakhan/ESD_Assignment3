"""Check one run's A/B/C distribution against Envoy's access logs (Part D).

Asks Elasticsearch: for Envoy's access-log events in this run's time range,
how many went to each replica? Then prints those counts next to what the
load-test client counted for the same run. Envoy's log, the client and the
Grafana dashboard are three independent records of the same traffic.

The time range and the client's counts come from the run's result file, so
the query covers exactly that run.

Run from the repo root, about 15 s after the run ends (Filebeat needs a moment
to ship the last lines):
    python scripts/envoy_log_counts.py results/C-rr.json
"""
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ES = "http://localhost:9200/studyslot-logs-*/_search"


def search(body: dict) -> dict:
    req = urllib.request.Request(ES, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as err:
        raise RuntimeError(err.read().decode()[:500]) from None
    except urllib.error.URLError as err:
        sys.exit(f"Cannot reach Elasticsearch on localhost:9200 ({err.reason}). Is the stack running?")


def envoy_counts(interface: str, start: str, end: str) -> tuple[int, dict, dict]:
    """(total, by replica, by outcome) for Envoy's access-log lines of one interface."""
    outcome_field = "http.response.status_code" if interface == "http" else "grpc_status"
    body = {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": [
            {"term": {"service.name": "envoy"}},
            {"term": {"interface": interface}},
            {"exists": {"field": "traceparent"}},       # only the load test's calls carry one
            {"range": {"@timestamp": {"gte": start, "lte": end}}},
        ]}},
        "aggs": {
            "by_replica": {"terms": {"field": "replica", "missing": "(none)", "size": 10}},
            "by_outcome": {"terms": {"field": outcome_field, "size": 20}},
        },
    }
    try:
        result = search(body)
    except RuntimeError as err:
        # If Elasticsearch stored these fields as text (not keyword), aggregate
        # on their automatic ".keyword" sub-fields instead.
        if "fielddata" not in str(err).lower() and "text field" not in str(err).lower():
            raise
        body["aggs"]["by_replica"]["terms"]["field"] = "replica.keyword"
        if interface == "grpc":
            body["aggs"]["by_outcome"]["terms"]["field"] = "grpc_status.keyword"
        body["query"]["bool"]["filter"][1] = {"term": {"interface.keyword": interface}}
        result = search(body)
    total = result["hits"]["total"]["value"]
    by_replica = {b["key"]: b["doc_count"] for b in result["aggregations"]["by_replica"]["buckets"]}
    by_outcome = {str(b["key"]): b["doc_count"] for b in result["aggregations"]["by_outcome"]["buckets"]}
    return total, by_replica, by_outcome


def fmt(counts: dict, total: int) -> str:
    return "  ".join(f"{k} {v} ({100 * v / total:.1f} %)" for k, v in sorted(counts.items())) if total else "-"


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: python scripts/envoy_log_counts.py results/<label>.json")
    run = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    print(f"{run['label']}: {run['policy']}; fault: {run['fault']}")
    print(f"time range (UTC): {run['start_utc']} to {run['end_utc']}\n")
    for s in run["interfaces"]:
        total, by_replica, by_outcome = envoy_counts(s["interface"], run["start_utc"], run["end_utc"])
        client_total = s["sent"]
        print(f"{s['interface'].upper()} {s['operation']}")
        print(f"  Envoy's access log : {total} requests")
        print(f"     by replica      : {fmt(by_replica, total)}")
        print(f"     by outcome      : {fmt(by_outcome, total)}")
        print(f"  load-test client   : {client_total} requests")
        print(f"     successes by replica: {fmt(s['successes_by_replica'], s['successes'])}")
        if total != client_total:
            print("  NOTE: totals differ. If Envoy has fewer, Filebeat may not have shipped the last lines yet;"
                  " wait a few seconds and run this again.")
        print()


if __name__ == "__main__":
    main()