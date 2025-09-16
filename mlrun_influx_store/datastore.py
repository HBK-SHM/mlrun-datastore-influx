from urllib.parse import parse_qs
import os
import pandas as pd
from influxdb_client import InfluxDBClient

import mlrun
from mlrun.datastore.base import DataStore, DataItem
from mlrun.utils import logger


class InfluxStore(DataStore):
    """
    MLRun datastore plugin for InfluxDB.

    Use URIs like:
        influx://<bucket>/<measurement>?field=<field>&tag=key:val&env=STAGING&range=-24h
        # optional direct config overrides:
        &url=http://host:8086&org=my-org&token=...   OR   &token_secret=INFLUX_STAGING_TOKEN

    Config resolution order (highest wins):
      1) URL query params: url=, org=, token=, token_secret=
      2) Secrets / env: INFLUX_<ENV>_URL, INFLUX_<ENV>_ORG, INFLUX_<ENV>_TOKEN
         (ENV defaults to DEV)
    """

    kind = "influx"

    # ----- MLRun will call this when resolving the scheme via entry points -----
    @classmethod
    def from_spec(cls, url: str = "", project=None, secrets=None, **kwargs):
        # MLRun passes the StoreManager as `parent` in kwargs (when applicable)
        parent = kwargs.get("parent")
        return cls(parent=parent, schema="influx", name="influx", endpoint="")

    def __init__(self, parent, schema, name, endpoint="", **kwargs):
        # normal base init
        super().__init__(parent, schema, name, endpoint, **kwargs)

    # ----- Data fetching API -----
    def as_df(self, url, subpath=None, columns=None, df_module=None, format=None, **kwargs):
        """
        Return a pandas DataFrame.
        If url is empty (DataItem already has _body), just return it via base logic.
        """
        if not url:
            return None  # let DataItem use its cached _body
        item = self.get(url)
        # noinspection PyProtectedMember,PyUnresolvedReferences
        return item._body

    def get(self, key: str, size=None, offset=0,ctx=None):
        """
        Fetch from InfluxDB and return a DataItem with a pandas DataFrame as body.

        key format:  "bucket/measurement?field=<field>&tag=key:val&env=STAGING&range=-24h
                      [&url=...&org=...&(token=...|token_secret=...)]"
        """
        # ---- Parse URI path & query ----
        path, query = (key.split("?", 1) + [""])[:2]
        if "/" not in path:
            raise ValueError(f"Invalid key: {key}. Expected format 'bucket/measurement'")

        bucket, measurement = path.split("/", 1)
        q = parse_qs(query)

        field_filter = q.get("field", [None])[0]
        tag_filters = q.get("tag", [])
        env = (q.get("env", ["DEV"])[0] or "DEV").upper()
        range_window = q.get("range", ["-1h"])[0]

        # direct overrides (optional)
        url_override = q.get("url", [None])[0]
        org_override = q.get("org", [None])[0]
        token_inline = q.get("token", [None])[0]
        token_secret = q.get("token_secret", [None])[0]

        # ---- Resolve config: URL / ORG / TOKEN ----
        influx_url = url_override or os.environ.get(f"INFLUX_{env}_URL")
        influx_org = org_override or os.environ.get(f"INFLUX_{env}_ORG")

        token = None
        if token_inline:
            token = token_inline
        elif token_secret:
            token = mlrun.get_secret_or_env(token_secret)
        else:
            token = mlrun.get_secret_or_env(f"INFLUX_{env}_TOKEN")
        if not token and ctx is not None:
            try:
                token = ctx.get_secret(token_secret or f"INFLUX_{env}_TOKEN")
            except Exception:
                token = token  # keep None if not set
        if not influx_url or not influx_org or not token:
            raise ValueError(
                f"Missing Influx config (env={env}). "
                f"Need url/org/token via URL or env/secrets: "
                f"INFLUX_{env}_URL : {influx_url}, INFLUX_{env}_ORG : {influx_org} and INFLUX_{env}_TOKEN"
            )

        # ---- Build Flux query ----
        query = (
            f'from(bucket:"{bucket}") '
            f'|> range(start: {range_window}) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}")'
        )
        if field_filter:
            query += f' |> filter(fn: (r) => r._field == "{field_filter}")'
        for tag in tag_filters:
            if ":" in tag:
                tagk, tagv = tag.split(":", 1)
                query += f' |> filter(fn: (r) => r.{tagk} == "{tagv}")'

        # ---- Query InfluxDB ----
        client = InfluxDBClient(url=influx_url, token=token, org=influx_org)
        query_api = client.query_api()
        tables = query_api.query(query)

        records = []
        for table in tables:
            for record in table.records:
                # capture remaining tags
                tags = {
                    k: v for k, v in record.values.items()
                    if k not in ["_time", "_field", "_value", "_measurement"]
                }
                records.append(
                    (record.get_time(), record.get_field(), record.get_value(),
                     record.get_measurement(), tags)
                )

        df = pd.DataFrame(records, columns=["time", "field", "value", "measurement", "tags"])

        # ---- Wrap in DataItem (with full URL) ----
        full_url = f"influx://{key}"
        item = DataItem(full_url, artifact_url=full_url, store=self, subpath="")
        item._body = df

        # (Optional) attach some metadata if available
        try:
            meta = getattr(item, "_meta", None) or getattr(item, "meta", None)
            if meta is not None:
                meta.update({
                    "bucket": bucket,
                    "measurement": measurement,
                    "field": field_filter,
                    "tags": tag_filters,
                    "env": env,
                    "range": range_window,
                    "url": influx_url,
                    "org": influx_org,
                })
        except Exception:  # best-effort
            logger.warning("Could not set metadata on DataItem", exc_info=False)

        return item

    def put(self, key: str, obj, append=False, **kwargs):
        """
        Write data to InfluxDB.

        Args:
            key: "bucket/measurement?env=STAGING[&url=...&org=...&token=...]"
            obj: pandas DataFrame or list of dicts
        """
        # Parse URI path & query
        path, query = (key.split("?", 1) + [""])[:2]
        if "/" not in path:
            raise ValueError(f"Invalid key: {key}. Expected format 'bucket/measurement'")
        bucket, measurement = path.split("/", 1)
        q = parse_qs(query)
        env = (q.get("env", ["DEV"])[0] or "DEV").upper()
        url_override = q.get("url", [None])[0]
        org_override = q.get("org", [None])[0]
        token_inline = q.get("token", [None])[0]
        token_secret = q.get("token_secret", [None])[0]

        influx_url = url_override or os.environ.get(f"INFLUX_{env}_URL")
        influx_org = org_override or os.environ.get(f"INFLUX_{env}_ORG")
        token = token_inline or mlrun.get_secret_or_env(token_secret or f"INFLUX_{env}_TOKEN")
        if not influx_url or not influx_org or not token:
            raise ValueError(
                f"Missing Influx config (env={env}). "
                f"Need url/org/token via URL or env/secrets: "
                f"INFLUX_{env}_URL : {influx_url}, INFLUX_{env}_ORG : {influx_org} and INFLUX_{env}_TOKEN"
            )

        # Prepare data
        if isinstance(obj, pd.DataFrame):
            records = obj.to_dict(orient="records")
        elif isinstance(obj, dict):
            records = [obj]
        elif isinstance(obj, list):
            records = obj
        else:
            raise ValueError("obj must be a DataFrame, dict, or list of dicts")

        # Convert records to InfluxDB line protocol
        points = []
        for rec in records:
            tags = rec.get("tags", {})
            fields = {k: v for k, v in rec.items() if k not in ["time", "tags"]}
            time = rec.get("time")
            point = {
                "measurement": measurement,
                "tags": tags,
                "fields": fields,
                "time": time,
            }
            points.append(point)

        # Write to InfluxDB
        client = InfluxDBClient(url=influx_url, token=token, org=influx_org)
        write_api = client.write_api()
        write_api.write(bucket=bucket, record=points)
