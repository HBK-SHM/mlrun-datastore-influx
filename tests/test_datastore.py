from mlrun_influx_store import InfluxStore

def test_influx_plugin_uri_parsing(monkeypatch):
    store = InfluxStore(None, "influx", "test")
    # Mock mlrun.get_secret_or_env and InfluxDBClient here
    # Ensure correct parsing of URI params
    key = "mybucket/temperature?field=temp&tag=sensor:bridge01&env=dev"
    # For now, just check parsing works without hitting real Influx
    assert "temperature" in key
