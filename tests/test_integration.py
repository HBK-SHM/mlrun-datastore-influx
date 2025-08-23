import time

import pandas as pd
import pytest
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from testcontainers.core.container import DockerContainer

from mlrun_influx_store.datastore import InfluxStore


@pytest.fixture(scope="session")
def influxdb_container():
    """Start InfluxDB 2.x in a Docker container."""
    container = DockerContainer("influxdb:2.7")
    container.with_exposed_ports(8086)
    container.with_env("DOCKER_INFLUXDB_INIT_MODE", "setup")
    container.with_env("DOCKER_INFLUXDB_INIT_USERNAME", "test-user")
    container.with_env("DOCKER_INFLUXDB_INIT_PASSWORD", "test-pass")
    container.with_env("DOCKER_INFLUXDB_INIT_ORG", "test-org")
    container.with_env("DOCKER_INFLUXDB_INIT_BUCKET", "test-bucket")
    container.with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "test-token")

    container.start()
    host = container.get_container_host_ip()
    port = container.get_exposed_port(8086)

    url = f"http://{host}:{port}"
    org = "test-org"
    bucket = "test-bucket"
    token = "test-token"

    # Give Influx a moment to initialize
    time.sleep(5)

    yield {"url": url, "org": org, "bucket": bucket, "token": token}

    container.stop()


import os
import mlrun
import pytest

@pytest.fixture
def setup_mlrun_project(influxdb_container):
    """Configure a local MLRun run context with Influx settings (no API server)."""
    url, org, bucket, token = (
        influxdb_container["url"],
        influxdb_container["org"],
        influxdb_container["bucket"],
        influxdb_container["token"],
    )

    # Offline mode – avoids API calls; warnings may still print (harmless)
    mlrun.mlconf.dbpath = ""
    os.environ["MLRUN_DBPATH"] = ""   # also quiet some versions

    # Create/open a local project (no server)
    project = mlrun.get_or_create_project(
        name="influx-it",
        context=".",
        user_project=False,
    )

    # Values we want available to the code under test
    run_params = {
        "INFLUX_DEV_URL": url,
        "INFLUX_DEV_ORG": org,
        "INFLUX_DEV_BUCKET": bucket,
    }

    # Secrets / env (common way plugins fetch tokens)
    os.environ["INFLUX_DEV_TOKEN"] = token
    # Also expose non-secret config via env in case your code reads envs:
    os.environ["INFLUX_DEV_URL"] = url
    os.environ["INFLUX_DEV_ORG"] = org
    os.environ["INFLUX_DEV_BUCKET"] = bucket

    # Create ctx first (older MLRun has no params=)
    ctx = mlrun.get_or_create_ctx("integration-test", project=project.metadata.name)

    # Populate params into ctx across MLRun versions
    try:
        # Newer API
        # noinspection PyUnresolvedReferences
        ctx.set_parameters(run_params)
    except AttributeError:
        # Older API(s)
        try:
            ctx.parameters.update(run_params)
        except Exception:
            # Last resort – some versions keep a private dict
            try:
                ctx._parameters.update(run_params)  # noqa: SLF001
            except Exception:
                # If none of the above exist, rely on env only
                pass

    return ctx, bucket

@pytest.fixture
def seed_influx(influxdb_container):
    """Insert test points into InfluxDB."""
    client = InfluxDBClient(
        url=influxdb_container["url"],
        token=influxdb_container["token"],
        org=influxdb_container["org"],
    )

    # Use a context manager to ensure clean close and avoid __del__ warnings
    with client.write_api(write_options=SYNCHRONOUS) as write_api:
        bucket = influxdb_container["bucket"]
        org    = influxdb_container["org"]

        # Insert test data that matches what the test is querying for
        # Use different timestamps to ensure both points are stored
        import time
        current_time = time.time_ns()

        write_api.write(
            bucket=bucket,
            org=org,
            record=[
                Point("temperature").tag("sensor", "bridge01").field("temp", 21.5).time(current_time),
                Point("temperature").tag("sensor", "bridge01").field("temp", 22.0).time(current_time + 1000000),  # 1ms later
            ],
        )

    # Return the ready client (or just the connection info)
    try:
        yield client
    finally:
        client.close()

@pytest.mark.integration
def test_influx_store_integration(setup_mlrun_project, influxdb_container, seed_influx):
    """Integration test with a real InfluxDB instance."""
    _, bucket = setup_mlrun_project
    store = InfluxStore(None, "influx", "test")

    # Query data
    key = f"{bucket}/temperature?field=temp&tag=sensor:bridge01&env=dev&range=-7d"
    item = store.get(key)

    # Access DataFrame directly from the item's internal body attribute
    df = item._body

    # Validate results
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "time" in df.columns
    assert df["measurement"].iloc[0] == "temperature"

    # Check that we got both expected values (order may vary)
    values = set(float(val) for val in df["value"])
    expected_values = {21.5, 22.0}
    assert values == expected_values, f"Expected values {expected_values}, but got {values}"

    # Verify we got exactly 2 records
    assert len(df) == 2
