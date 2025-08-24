from urllib.parse import parse_qs

import mlrun
import mlrun.runtimes.utils
import pandas as pd
from influxdb_client import InfluxDBClient
from mlrun.datastore.base import DataStore, DataItem
from mlrun.utils import logger


class InfluxStore(DataStore):
    """
    MLRun datastore plugin for InfluxDB.
    Access with URIs like:

        influx://bucket/measurement?field=temp&tag=sensor:bridge01&env=staging&range=-24h

    Requirements:
    - Project parameters for each environment:
        INFLUX_DEV_URL, INFLUX_DEV_ORG
        INFLUX_STAGING_URL, INFLUX_STAGING_ORG
        INFLUX_PROD_URL, INFLUX_PROD_ORG

    - Project secrets for tokens:
        INFLUX_DEV_TOKEN
        INFLUX_STAGING_TOKEN
        INFLUX_PROD_TOKEN
    """

    kind = "influx"

    def __init__(self, parent, schema, name, endpoint="", **kwargs):
        super().__init__(parent, schema, name, endpoint, **kwargs)

    def as_df(self, url, subpath=None, columns=None, df_module=None, format=None, **kwargs):
        """
        Return dataframe from the InfluxDB query result.
        Since we've already queried the data and have it in memory,
        we just need to return it directly.
        """
        # If url is empty, this means we're being called from a DataItem that already has the data
        # in memory. In this case, we should not re-execute the query but return the cached data.
        # However, since we don't have access to the DataItem from here, we'll let the base class handle it.
        if not url or url == "":
            # Return None to let the DataItem use its cached _body
            return None

        # Execute the query and return the DataFrame directly
        item = self.get(url)
        # noinspection PyProtectedMember
        return item._body

    def get(self, key: str, size=None, offset=0):
        """
        Fetch a dataset from InfluxDB and return as DataItem with pandas DataFrame.

        :param key: URI path + query parameters.
            Format: bucket/measurement?field=<field>&tag=key:val&env=staging&range=-24h
        """
        # ---- Parse URI ----
        parts = key.split("?", 1)
        path = parts[0]
        query_params = parse_qs(parts[1]) if len(parts) > 1 else {}

        if "/" not in path:
            raise ValueError(
                f"Invalid key: {key}. Expected format bucket/measurement"
            )

        bucket, measurement = path.split("/", 1)
        field_filter = query_params.get("field", [None])[0]
        tag_filters = query_params.get("tag", [])
        env = query_params.get("env", ["dev"])[0]
        range_window = query_params.get("range", ["-1h"])[0]  # default = -1h

        # ---- Load project config ----
        # First try to get the current context if one exists
        context = None
        try:
            # Try to get the current global context
            if hasattr(mlrun.runtimes.utils, 'global_context'):
                context = mlrun.runtimes.utils.global_context.get()
        except (AttributeError, ImportError):
            pass

        # If no context found, create/get one
        if context is None:
            context = mlrun.get_or_create_ctx("influx-store")

        # Try to get parameters from context first, then fall back to environment variables
        url = context.get_param(f"INFLUX_{env.upper()}_URL")
        org = context.get_param(f"INFLUX_{env.upper()}_ORG")
        token = mlrun.get_secret_or_env(f"INFLUX_{env.upper()}_TOKEN")

        # Fallback to environment variables if not found in context
        if not url:
            import os
            url = os.environ.get(f"INFLUX_{env.upper()}_URL")
        if not org:
            import os
            org = os.environ.get(f"INFLUX_{env.upper()}_ORG")
        if not token:
            import os
            token = os.environ.get(f"INFLUX_{env.upper()}_TOKEN")

        if not url or not org or not token:
            raise ValueError(
                f"Missing Influx config for env={env}. "
                f"Expected project params: INFLUX_{env.upper()}_URL, INFLUX_{env.upper()}_ORG "
                f"and secret: INFLUX_{env.upper()}_TOKEN"
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
        client = InfluxDBClient(url=url, token=token, org=org)
        query_api = client.query_api()
        tables = query_api.query(query)

        records = []
        for table in tables:
            for record in table.records:
                records.append(
                    (
                        record.get_time(),
                        record.get_field(),
                        record.get_value(),
                        record.get_measurement(),
                        {k: v for k, v in record.values.items()
                         if k not in ["_time", "_field", "_value", "_measurement"]}
                    )
                )

        df = pd.DataFrame(records, columns=["time", "field", "value", "measurement", "tags"])

        # ---- Wrap in DataItem with metadata ----
        # Create DataItem properly - use the key as the artifact URL since we have the data in memory
        item = DataItem(key, artifact_url=key, store=self, subpath="")
        # Set the dataframe directly on the item
        item._body = df
        # Add metadata to the DataItem if possible
        try:
            # Try to update metadata if the item supports it
            if hasattr(item, '_meta') and item._meta is not None:
                # noinspection PyUnresolvedReferences
                item._meta.update({
                    "bucket": bucket,
                    "measurement": measurement,
                    "field": field_filter,
                    "tags": tag_filters,
                    "env": env,
                    "range": range_window,
                    "url": url,
                    "org": org,
                })
            elif hasattr(item, 'meta') and item.meta is not None:
                item.meta.update({
                    "bucket": bucket,
                    "measurement": measurement,
                    "field": field_filter,
                    "tags": tag_filters,
                    "env": env,
                    "range": range_window,
                    "url": url,
                    "org": org,
                })
        except (AttributeError, TypeError):
            # If metadata cannot be set, log a warning but continue
            logger.warning("Could not set metadata on DataItem")
        return item

    def put(self, key: str, obj, append=False, **kwargs):
        """
        Write support not implemented yet.
        (Could be extended to write back to Influx.)
        """
        raise NotImplementedError("InfluxStore.put() not supported")
    @classmethod
    def from_spec(cls, url: str = "", project=None, secrets=None, **kwargs):
        """
        Minimal factory for MLRun's schema registry.
        MLRun passes this to build a store from a URL scheme.
        We don't need anything special here; just instantiate.
        """
        parent = kwargs.get("parent")  # MLRun passes the StoreManager as parent
        # name/endpoint are mostly cosmetic for this store
        return cls(parent=parent, schema="influx", name="influx", endpoint="")
