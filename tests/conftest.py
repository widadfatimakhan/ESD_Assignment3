"""Shared test setup.

The tests import the real app (studyslot/main.py) and talk to it through
FastAPI's TestClient, so no server or Docker is needed. They use their own
throwaway database file, never the one in the Docker volume.
"""
import os
import sys
import tempfile
from pathlib import Path

# Must happen BEFORE the app is imported: point it at a temporary database.
os.environ["STUDYSLOT_DB_PATH"] = str(Path(tempfile.mkdtemp()) / "test.db")

# A3: outside Docker the replica cannot look up its container name, so name it here.
os.environ["REPLICA_ID"] = "T"

# Let "import main" find the files in studyslot/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "studyslot"))

import pytest                                  # noqa: E402
from fastapi.testclient import TestClient      # noqa: E402
import chaos                                   # noqa: E402
import db                                      # noqa: E402
import main                                    # noqa: E402


@pytest.fixture
def client():
    """A fresh start for every test: empty tables, rate limiter reset, no chaos."""
    with db.connect() as conn:
        conn.execute("DELETE FROM bookings")
        conn.execute("DELETE FROM idempotency_keys")
    main.availability_limiter.reset()
    chaos.reset()
    return TestClient(main.app)


@pytest.fixture
def booking_body():
    return {"room": "R3", "slot": "09:00", "student_ids": ["s1", "s2", "s3", "s4", "s5"]}