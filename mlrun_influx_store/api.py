# mlrun_influx_store/api.py
from urllib.parse import parse_qs
import pandas as pd
import mlrun
from mlrun.datastore.base import DataItem
from mlrun.datastore import store_manager
from .datastore import InfluxStore


# --- core helpers ------------------------------------------------------------

def get_dataitem(uri: str, ctx=None) -> DataItem:
    """
    Return a DataItem that already has the DataFrame loaded in _body.
    This avoids the MLRun path that may call store.as_df('') with an empty url.
    """
    if not uri.startswith("influx://"):
        raise ValueError("URI must start with influx://")
    key = uri.split("://", 1)[1]
    store = InfluxStore(parent=store_manager, schema="influx", name="influx", endpoint="")
    # noinspection PyArgumentList
    return store.get(key, ctx=ctx)  # <-- pass ctx


def read_df(uri: str, ctx=None) -> pd.DataFrame:
    """Return a pandas DataFrame for the given influx:// URI."""
    item = get_dataitem(uri,ctx=ctx)
    return getattr(item, "_body", None)  # DF is set by store.get(...)


# --- logging with MLRun labels & tag -----------------------------------------

def _auto_labels_from_uri(uri: str) -> dict:
    """Small, safe set of labels derived from the query itself (low cardinality)."""
    key = uri.split("://", 1)[1]
    path, query = (key.split("?", 1) + [""])[:2]
    bucket, measurement = path.split("/", 1)
    q = parse_qs(query)

    env = (q.get("env", ["DEV"])[0] or "DEV").upper()
    field = q.get("field", [None])[0]
    rng = q.get("range", [None])[0]

    labels = {
        "store": "influx",
        "env": env,
        "bucket": bucket,
        "measurement": measurement,
    }
    if field: labels["field"] = field
    if rng:   labels["range"] = rng
    return labels


def _labels_from_columns(df: pd.DataFrame, cols: list[str], max_len: int = 64) -> dict:
    """
    Create compact MLRun labels from dataframe columns.
    We join up to a few unique values to keep cardinality/length reasonable.
    """
    out = {}
    for c in cols or []:
        if c in df.columns:
            # up to 4 unique values, stringified, joined with '|'
            uniq = list(pd.Series(df[c].dropna().astype(str)).unique())[:4]
            if uniq:
                val = "|".join(uniq)
                out[f"col_{c}"] = val[:max_len]
    return out


def log_dataset(
        key: str,
        uri: str,
        *,
        ctx: mlrun.MLClientCtx | None = None,
        labels: dict | None = None,
        tag: str | None = None,
        label_from_columns: list[str] | None = None,
        store_run: bool = False,
) -> pd.DataFrame:
    """
    Read from Influx and log an MLRun dataset artifact with labels + optional artifact tag.

    - Puts a minimal, low-cardinality label set from the URI (env/bucket/measurement[/field/range]).
    - Optionally promotes selected dataframe columns into MLRun labels (summarized).
    - Records the source as `src_path=uri` so the artifact points back to Influx.
    """
    df = read_df(uri,ctx)

    auto = _auto_labels_from_uri(uri)
    col_labels = _labels_from_columns(df, label_from_columns or [])
    final_labels = {**auto, **col_labels, **(labels or {})}

    if ctx is None:
        # Keep notebooks snappy/offline unless user wants to persist the run:
        # noinspection PyArgumentList
        ctx = mlrun.get_or_create_ctx("influx-log", store_run=store_run)

    ctx.log_dataset(
        key=key,
        df=df,
        src_path=uri,  # traceability back to Influx (read-only plugin)
        labels=final_labels,  # <-- MLRun artifact labels
        tag=tag,  # <-- MLRun artifact version tag (e.g., 'v0')
    )
    return df


# --- optional: write to Influx with Influx tags -------------------------------

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def write_df(
        uri: str,
        df: pd.DataFrame,
        *,
        time_col: str = "time",
        tag_cols: list[str] | None = None,
        field_cols: list[str] | None = None,
):
    """
    Write to Influx (DataStore.put() equivalent) so you can manage Influx TAGS vs FIELDS.

    Usage:
      write_df("influx://bucket/measurement?env=STAGING&token_secret=INFLUX_STAGING_TOKEN",
               df, time_col="time", tag_cols=["sensor","asset_id","span_id"], field_cols=["value"])
    """
    key = uri.split("://", 1)[1]
    path, query = (key.split("?", 1) + [""])[:2]
    if "/" not in path:
        raise ValueError("URI path must look like bucket/measurement")
    bucket, measurement = path.split("/", 1)
    q = parse_qs(query)

    # resolve config via your store's rules
    from .datastore import InfluxStore
    # reuse the store's resolver by briefly constructing and reusing its parsing logic:
    _store = InfluxStore(parent=store_manager, schema="influx", name="influx", endpoint="")
    # DRY: minimally re-parse what we need here (could be refactored to a shared util if desired)
    env = (q.get("env", ["DEV"])[0] or "DEV").upper()
    url_override = q.get("url", [None])[0]
    org_override = q.get("org", [None])[0]
    token_inline = q.get("token", [None])[0]
    token_secret = q.get("token_secret", [None])[0]

    import os
    influx_url = url_override or os.environ.get(f"INFLUX_{env}_URL")
    influx_org = org_override or os.environ.get(f"INFLUX_{env}_ORG")
    token = token_inline or mlrun.get_secret_or_env(token_secret or f"INFLUX_{env}_TOKEN")
    if not (influx_url and influx_org and token):
        raise ValueError(f"Missing Influx config for env={env} (url/org/token).")

    # infer field columns if not provided
    if field_cols is None:
        field_cols = [c for c in df.columns if c not in ([time_col] + (tag_cols or []))]

    ts = pd.to_datetime(df[time_col], utc=True)
    client = InfluxDBClient(url=influx_url, token=token, org=influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for i, row in df.iterrows():
        p = Point(measurement).time(ts.iloc[i].to_pydatetime())
        for c in tag_cols or []:
            if c in df.columns and pd.notna(row[c]):
                p = p.tag(c, str(row[c]))
        for c in field_cols:
            if c in df.columns and pd.notna(row[c]):
                v = row[c]
                if isinstance(v, (bool, int, float)):
                    p = p.field(c, v)
                else:
                    # best effort cast
                    try:
                        p = p.field(c, float(v))
                    except Exception:
                        p = p.field(c, str(v))
        points.append(p)

    if points:
        write_api.write(bucket=bucket, org=influx_org, record=points)
