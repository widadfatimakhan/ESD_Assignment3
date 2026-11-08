"""Docker Compose scaling starts three identical containers from one service
definition, so every replica gets exactly the same image, command and
environment. The only thing that differs is the container NAME that Compose
gives each one: studyslot-app-1, studyslot-app-2, studyslot-app-3.

A container cannot read its own name directly, but Docker's built-in DNS
server (127.0.0.11) can turn the container's IP address back into its name
(a reverse "PTR" lookup):

    172.21.0.7  ->  "studyslot-app-2.studyslot_default."

The number at the end of the name becomes the replica letter:
1 -> A, 2 -> B, 3 -> C. That letter goes on every log line, on every
response (X-Replica header / x-replica gRPC metadata) and on the
studyslot_replica_info metric.

Why not socket.gethostbyaddr()? It checks /etc/hosts first, and Docker writes
the container ID there, so it would return the ID instead of the name.
dnspython asks the DNS server directly.

Outside Docker (tests, local runs) set REPLICA_ID yourself, e.g. REPLICA_ID=A.
"""
import os
import re
import socket
import time

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def letter_for(container_name: str) -> str | None:
    """'studyslot-app-2.studyslot_default.' -> 'B'. None if there is no number at the end."""
    short_name = container_name.split(".")[0]
    match = re.search(r"-(\d+)$", short_name)
    if not match:
        return None
    index = int(match.group(1))
    return LETTERS[index - 1] if 1 <= index <= len(LETTERS) else f"R{index}"


def _container_name_from_dns() -> str | None:
    """Ask Docker's DNS which container owns this IP. None if that is not possible."""
    if not os.path.exists("/.dockerenv"):    # not inside a Docker container
        return None
    try:
        import dns.resolver
        import dns.reversename
    except ImportError:          # dnspython not installed (e.g. a minimal local venv)
        return None
    try:
        my_ip = socket.gethostbyname(socket.gethostname())
    except OSError:
        return None
    # At start-up the network can take a moment to be ready, so try a few times.
    for _ in range(5):
        try:
            answer = dns.resolver.resolve(dns.reversename.from_address(my_ip), "PTR", lifetime=1.0)
            return str(answer[0]).rstrip(".")
        except Exception:
            time.sleep(0.5)
    return None


def detect() -> tuple[str, str]:
    """Return (replica letter, container name)."""
    hostname = socket.gethostname()
    override = os.getenv("REPLICA_ID")
    if override:
        return override, hostname
    container = _container_name_from_dns()
    if container:
        letter = letter_for(container)
        if letter:
            return letter, container.split(".")[0]
    # Last resort: the container ID. Still unique per replica, just not A/B/C.
    return hostname[:12], hostname


REPLICA, CONTAINER = detect()