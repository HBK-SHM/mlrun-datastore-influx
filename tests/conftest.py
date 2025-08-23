# tests/conftest.py
import os, subprocess, pytest

def _discover_docker_host():
    try:
        ctx = subprocess.check_output(["docker", "context", "show"], text=True).strip()
        host = subprocess.check_output(
            ["docker", "context", "inspect", ctx, "--format", '{{(index .Endpoints "docker").Host}}'],
            text=True,
        ).strip()
        return host
    except Exception:
        return None

# NEW: normalize socket for testcontainers
def _normalize_docker_socket_for_testcontainers():
    docker_host = os.environ.get("DOCKER_HOST", "")
    # If Desktop exposes a per-user socket, map it to the canonical path for mounts
    if docker_host.startswith("unix://") and "/.docker/run/docker.sock" in docker_host:
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"

@pytest.fixture(scope="session", autouse=True)
def ensure_docker_host_env():
    if not os.environ.get("DOCKER_HOST"):
        host = _discover_docker_host()
        if host:
            os.environ["DOCKER_HOST"] = host

    _normalize_docker_socket_for_testcontainers()

    # Optionally skip integration tests if still not reachable
    try:
        import docker
        docker.from_env().ping()
    except Exception:
        os.environ["SKIP_INTEGRATION"] = "1"

def pytest_collection_modifyitems(config, items):
    if os.environ.get("SKIP_INTEGRATION") == "1":
        skip_mark = pytest.mark.skip(reason="Docker daemon not reachable for integration tests")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_mark)
