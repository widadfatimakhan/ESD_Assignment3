FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all the app's Python modules (not just main.py any more).
# .dockerignore keeps caches, logs and local data out of the image.
COPY . .

# 8000 = HTTP API and /metrics, 50051 = gRPC (A3: both in one process,
# started by main.py when STUDYSLOT_GRPC_PORT is set).
EXPOSE 8000 50051

# On "docker stop", give in-flight requests up to 5 s to finish.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--timeout-graceful-shutdown", "5"]