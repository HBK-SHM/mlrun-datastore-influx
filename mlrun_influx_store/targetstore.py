from typing import Optional, Any, Union, List
import pandas as pd
from mlrun.datastore.targets import BaseStoreTarget, TargetTypes
from mlrun.data_types import is_spark_dataframe
import mlrun
from mlrun.utils import logger


class InfluxTarget(BaseStoreTarget):
    """
    InfluxDB target storage driver for MLRun feature store.

    Allows writing feature set data to InfluxDB with proper tag/field separation.

    :param name: Target name (default: "influx")
    :param path: InfluxDB path in format "bucket/measurement"
    :param attributes: Optional attributes dict containing:
        - env: Environment (DEV/STAGING/PROD, default: DEV)
        - url: InfluxDB URL override
        - org: InfluxDB organization override
        - token: Token override
        - token_secret: Token secret name override
        - time_col: Time column name (default: auto-detect from timestamp_key)
        - tag_cols: List of columns to store as InfluxDB tags
        - field_cols: List of columns to store as InfluxDB fields (auto-inferred if not provided)
    :param after_step: Optional step to run after in the processing graph
    :param columns: Optional list of columns to include
    :param storage_options: Optional storage options
    :param schema: Optional schema definition
    """

    kind = "influx"
    is_offline = True  # Can be used for historical data storage
    is_online = False  # Not typically used for real-time serving
    support_spark = False  # Spark not supported yet
    support_storey = True  # Storey engine support
    support_pandas = True  # Pandas support
    support_append = True  # Supports appending data

    def __init__(
        self,
        name: str = "",
        path: Optional[str] = None,
        attributes: Optional[dict[str, Any]] = None,
        after_step: Optional[str] = None,
        columns: Optional[List[str]] = None,
        partitioned: bool = False,
        key_bucketing_number: Optional[int] = None,
        partition_cols: Optional[List[str]] = None,
        time_partitioning_granularity: Optional[str] = None,
        max_events: Optional[int] = None,
        flush_after_seconds: Optional[int] = None,
        storage_options: Optional[dict[str, str]] = None,
        schema: Optional[dict[str, Any]] = None,
        credentials_prefix: Optional[str] = None,
        # InfluxDB-specific parameters
        env: str = "DEV",
        time_col: Optional[str] = None,
        tag_cols: Optional[List[str]] = None,
        field_cols: Optional[List[str]] = None,
    ):
        # Ensure attributes dict exists and populate with InfluxDB-specific settings
        attributes = attributes or {}
        attributes.setdefault("env", env)
        if time_col:
            attributes["time_col"] = time_col
        if tag_cols:
            attributes["tag_cols"] = tag_cols
        if field_cols:
            attributes["field_cols"] = field_cols

        super().__init__(
            name or self.kind,
            path,
            attributes,
            after_step,
            columns,
            partitioned,
            key_bucketing_number,
            partition_cols,
            time_partitioning_granularity,
            max_events=max_events,
            flush_after_seconds=flush_after_seconds,
            storage_options=storage_options,
            schema=schema,
            credentials_prefix=credentials_prefix,
        )

    def write_dataframe(
        self,
        df: pd.DataFrame,
        key_column: Optional[Union[str, List[str]]] = None,
        timestamp_key: Optional[str] = None,
        chunk_id: int = 0,
        **kwargs
    ) -> Optional[int]:
        """
        Write DataFrame to InfluxDB using the write_df function.

        :param df: DataFrame to write
        :param key_column: Key columns (will be used as InfluxDB tags if tag_cols not specified)
        :param timestamp_key: Timestamp column name
        :param chunk_id: Chunk identifier (unused for InfluxDB)
        :param kwargs: Additional arguments
        :return: Number of points written (or None if unable to determine)
        """
        if is_spark_dataframe(df):
            raise mlrun.errors.MLRunRuntimeError(
                "InfluxTarget does not support Spark DataFrames yet"
            )

        # Build InfluxDB URI
        target_path = self.get_target_path()
        if not target_path:
            raise mlrun.errors.MLRunInvalidArgumentError(
                "InfluxTarget requires a path in format 'bucket/measurement'"
            )

        # Construct full URI with query parameters
        env = self.attributes.get("env", "DEV")
        query_params = [f"env={env}"]

        # Add optional URL/org/token overrides
        for param in ["url", "org", "token", "token_secret"]:
            if param in self.attributes:
                query_params.append(f"{param}={self.attributes[param]}")

        query_string = "&".join(query_params) if query_params else ""
        uri = f"influx://{target_path}"
        if query_string:
            uri += f"?{query_string}"

        # Determine columns for tags and fields
        time_col = self._resolve_time_column(timestamp_key)
        tag_cols = self._resolve_tag_columns(key_column)
        field_cols = self._resolve_field_columns(df, time_col, tag_cols)

        # Use the write_df function from api.py
        try:
            from .api import write_df

            write_df(
                uri=uri,
                df=df,
                time_col=time_col,
                tag_cols=tag_cols,
                field_cols=field_cols,
            )

            logger.info(f"Successfully wrote {len(df)} rows to InfluxDB: {uri}")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to write DataFrame to InfluxDB: {e}")
            raise mlrun.errors.MLRunRuntimeError(f"InfluxDB write failed: {e}") from e

    def add_writer_step(
        self,
        graph,
        after,
        features,
        key_columns=None,
        timestamp_key=None,
        featureset_status=None,
    ):
        """
        Add InfluxDB writer step to the storey processing graph.
        """
        target_path = self.get_target_path()
        if not target_path:
            raise mlrun.errors.MLRunInvalidArgumentError(
                "InfluxTarget requires a path in format 'bucket/measurement'"
            )

        # Get column information
        key_column_names = list(key_columns.keys()) if key_columns else []
        column_list = self._get_column_list(
            features=features,
            timestamp_key=timestamp_key,
            key_columns=key_column_names,
            with_type=False,
        )

        # Prepare attributes for the step
        step_attributes = dict(self.attributes)
        step_attributes.update({
            "target_path": target_path,
            "timestamp_key": timestamp_key,
            "key_columns": key_column_names,
            "columns": column_list,
        })

        # Add the custom InfluxDB writer step
        graph.add_step(
            name=self.name or "InfluxTarget",
            after=after,
            graph_shape="cylinder",
            class_name="mlrun_influx_store.storey_target.InfluxStoreyTarget",
            **step_attributes,
        )

    def as_df(
        self,
        columns=None,
        df_module=None,
        entities=None,
        start_time=None,
        end_time=None,
        time_column=None,
        additional_filters=None,
        **kwargs,
    ):
        """
        Read data from InfluxDB target as DataFrame.
        Uses the InfluxStore datastore for reading.
        """
        target_path = self.get_target_path()
        if not target_path:
            raise mlrun.errors.MLRunInvalidArgumentError(
                "InfluxTarget requires a path for reading data"
            )

        # Build URI with query parameters for reading
        env = self.attributes.get("env", "DEV")
        query_params = [f"env={env}"]

        # Add time range if specified
        if start_time and end_time:
            # Convert to InfluxDB time range format
            import datetime
            if isinstance(start_time, str):
                start_time = pd.to_datetime(start_time)
            if isinstance(end_time, str):
                end_time = pd.to_datetime(end_time)

            duration = end_time - start_time
            # Use relative time range (InfluxDB format)
            range_str = f"-{int(duration.total_seconds())}s"
            query_params.append(f"range={range_str}")

        # Add field filter if specified
        if columns and len(columns) == 1:
            query_params.append(f"field={columns[0]}")

        query_string = "&".join(query_params)
        uri = f"influx://{target_path}?{query_string}"

        try:
            # Use MLRun's dataitem interface
            dataitem = mlrun.get_dataitem(uri)
            df = dataitem.as_df(
                columns=columns,
                df_module=df_module,
                **kwargs
            )

            # Apply additional filtering if needed
            if entities and 'tags' in df.columns:
                # Filter by entities if they exist in tags
                # This is InfluxDB-specific logic
                pass

            return df

        except Exception as e:
            logger.error(f"Failed to read from InfluxDB target: {e}")
            raise mlrun.errors.MLRunRuntimeError(f"InfluxDB read failed: {e}") from e

    def purge(self):
        """
        Delete/purge the InfluxDB measurement data.
        Note: InfluxDB doesn't support traditional file deletion,
        so this would require dropping the measurement or using retention policies.
        """
        logger.warning(
            "InfluxDB purge not implemented - use InfluxDB retention policies "
            "or drop measurement manually"
        )
        # Could implement measurement deletion via InfluxDB API if needed
        pass

    def _resolve_time_column(self, timestamp_key: Optional[str]) -> str:
        """Resolve the time column name."""
        return (
            self.attributes.get("time_col") or
            timestamp_key or
            "time"
        )

    def _resolve_tag_columns(self, key_column: Optional[Union[str, List[str]]]) -> List[str]:
        """Resolve which columns should be InfluxDB tags."""
        # Priority: explicit tag_cols > key_columns > empty list
        if "tag_cols" in self.attributes:
            return self.attributes["tag_cols"]

        if key_column:
            if isinstance(key_column, str):
                return [key_column]
            return list(key_column)

        return []

    def _resolve_field_columns(
        self,
        df: pd.DataFrame,
        time_col: str,
        tag_cols: List[str]
    ) -> Optional[List[str]]:
        """Resolve which columns should be InfluxDB fields."""
        if "field_cols" in self.attributes:
            return self.attributes["field_cols"]

        # Auto-infer: all columns except time and tags
        excluded_cols = {time_col} | set(tag_cols)
        field_cols = [c for c in df.columns if c not in excluded_cols]

        return field_cols if field_cols else None


# Register the target type
TargetTypes.influx = "influx"

# Update the TargetTypes.all() method to include influx
original_all = TargetTypes.all
def all_with_influx():
    result = original_all()
    if TargetTypes.influx not in result:
        result.append(TargetTypes.influx)
    return result
TargetTypes.all = staticmethod(all_with_influx)
