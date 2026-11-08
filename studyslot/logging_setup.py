"""Structured JSON logging, shared by every StudySlot process.

Each call to log_event() writes ONE line of JSON to stdout (which Filebeat
ships to Elasticsearch) and to logs/app.log. SERVICE_NAME can be overridden
with an environment variable so the gRPC service (Part B) can label its own logs.
"""
import json
import logging
import os
import sys
from contextvars import ContextVar
from datetime import datetime, timezone


from replica import REPLICA

SERVICE_NAME = os.getenv("SERVICE_NAME", "studyslot")

# The ID of the request currently being handled. The HTTP middleware in main.py
# sets it (and the gRPC server will in Part B); log_event() adds it to every
# log line automatically, so all lines from one request can be found together.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

# The W3C trace context of the current request (see tracing.py), set by the
# same middleware / interceptor. log_event() adds trace.id and span.id from it.
trace_context_var: ContextVar[dict | None] = ContextVar("trace_context", default=None)

_logger = logging.getLogger(SERVICE_NAME)
_logger.setLevel(logging.INFO)
_logger.propagate = False
if not _logger.handlers:
    # 1) Print to the screen / container output (Filebeat reads this).
    _stream_handler = logging.StreamHandler(sys.stdout)
    _stream_handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_stream_handler)

    # 2) Also keep a copy in logs/app.log.
    os.makedirs("logs", exist_ok=True)
    _file_handler = logging.FileHandler("logs/app.log", encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_file_handler)


def log_event(message: str, *, level: str = "INFO", **fields) -> None:
    """Write one JSON log line with timestamp, service name, level, message and extra fields."""
    record = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "service": {"name": SERVICE_NAME},
        "replica": REPLICA,
        "log": {"level": level.lower()},
        "message": message,
    }
    request_id = request_id_var.get()
    if request_id:
        record["request_id"] = request_id
    span = trace_context_var.get()
    if span:
        record["trace"] = {"id": span["trace_id"]}
        record["span"] = {"id": span["span_id"]}
        if span.get("parent_span_id"):
            record["span"]["parent_id"] = span["parent_span_id"]
    record.update(fields)
    line = json.dumps(record, separators=(",", ":"))
    getattr(_logger, level.lower(), _logger.info)(line)
