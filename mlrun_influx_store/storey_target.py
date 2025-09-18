# mlrun_influx_store/storey_target.py
import pandas as pd
from typing import Optional, List, Any
from mlrun.utils import logger
import mlrun


class InfluxStoreyTarget:
    """
    Storey step for writing data to InfluxDB.

    This class integrates with MLRun's Storey processing engine to provide
    streaming writes to InfluxDB as part of feature store ingestion pipelines.
    """

    def __init__(
        self,
        target_path: str,
        timestamp_key: Optional[str] = None,
        key_columns: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        env: str = "DEV",
        time_col: Optional[str] = None,
        tag_cols: Optional[List[str]] = None,
        field_cols: Optional[List[str]] = None,
        url: Optional[str] = None,
        org: Optional[str] = None,
        token: Optional[str] = None,
        token_secret: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize InfluxDB Storey target.

        :param target_path: InfluxDB path (bucket/measurement)
        :param timestamp_key: Name of timestamp column
        :param key_columns: List of key column names
        :param columns: List of all columns to include
        :param env: Environment (DEV/STAGING/PROD)
        :param time_col: Time column name override
        :param tag_cols: Columns to store as InfluxDB tags
        :param field_cols: Columns to store as InfluxDB fields
        :param url: InfluxDB URL override
        :param org: InfluxDB org override
        :param token: InfluxDB token override
        :param token_secret: InfluxDB token secret name override
        """
        self.target_path = target_path
        self.timestamp_key = timestamp_key
        self.key_columns = key_columns or []
        self.columns = columns or []
        self.env = env
        self.time_col = time_col
        self.tag_cols = tag_cols or []
        self.field_cols = field_cols

        # Store connection overrides
        self.connection_overrides = {}
        if url:
            self.connection_overrides["url"] = url
        if org:
            self.connection_overrides["org"] = org
        if token:
            self.connection_overrides["token"] = token
        if token_secret:
            self.connection_overrides["token_secret"] = token_secret

        logger.info(f"Initialized InfluxStoreyTarget for {target_path}")

    def __call__(self, event):
        """
        Process a single event and write to InfluxDB.

        This method is called by Storey for each event in the stream.
        """
        try:
            # Convert event to DataFrame if needed
            if hasattr(event, 'body') and isinstance(event.body, dict):
                # Single event case
                df = pd.DataFrame([event.body])
            elif hasattr(event, 'body') and isinstance(event.body, pd.DataFrame):
                # DataFrame case
                df = event.body
            elif isinstance(event, dict):
                # Direct dict event
                df = pd.DataFrame([event])
            elif isinstance(event, pd.DataFrame):
                # Direct DataFrame
                df = event
            else:
                logger.warning(f"Unsupported event type for InfluxDB: {type(event)}")
                return event

            if df.empty:
                return event

            # Write to InfluxDB
            self._write_dataframe_to_influx(df)

            return event

        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")
            # Don't fail the entire pipeline, just log the error
            return event

    def _write_dataframe_to_influx(self, df: pd.DataFrame):
        """Write DataFrame to InfluxDB using the write_df API function."""

        # Build URI with query parameters
        query_params = [f"env={self.env}"]

        # Add connection overrides
        for key, value in self.connection_overrides.items():
            if value:
                query_params.append(f"{key}={value}")

        query_string = "&".join(query_params)
        uri = f"influx://{self.target_path}?{query_string}"

        # Resolve column mappings
        time_col = self._resolve_time_column()
        tag_cols = self._resolve_tag_columns()
        field_cols = self._resolve_field_columns(df, time_col, tag_cols)

        # Import and use write_df
        try:
            from .api import write_df

            write_df(
                uri=uri,
                df=df,
                time_col=time_col,
                tag_cols=tag_cols,
                field_cols=field_cols,
            )

            logger.debug(f"Wrote {len(df)} rows to InfluxDB: {self.target_path}")

        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")
            raise

    def _resolve_time_column(self) -> str:
        """Resolve the time column name."""
        return (
            self.time_col or
            self.timestamp_key or
            "time"
        )

    def _resolve_tag_columns(self) -> List[str]:
        """Resolve which columns should be InfluxDB tags."""
        if self.tag_cols:
            return self.tag_cols

        # Use key columns as tags by default
        return self.key_columns

    def _resolve_field_columns(
        self,
        df: pd.DataFrame,
        time_col: str,
        tag_cols: List[str]
    ) -> Optional[List[str]]:
        """Resolve which columns should be InfluxDB fields."""
        if self.field_cols:
            return self.field_cols

        # Auto-infer: all columns except time and tags
        excluded_cols = {time_col} | set(tag_cols)
        field_cols = [c for c in df.columns if c not in excluded_cols]

        return field_cols if field_cols else None


# For batch processing support
class InfluxBatchStoreyTarget(InfluxStoreyTarget):
    """
    Batch version of InfluxDB Storey target.

    Accumulates events and writes them in batches for better performance.
    """

    def __init__(self, batch_size: int = 1000, flush_interval: int = 30, **kwargs):
        """
        Initialize batch InfluxDB target.

        :param batch_size: Number of events to accumulate before writing
        :param flush_interval: Maximum seconds to wait before flushing
        """
        super().__init__(**kwargs)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.batch_events = []
        self.last_flush_time = pd.Timestamp.now()

    def __call__(self, event):
        """Accumulate events and write in batches."""
        try:
            # Add event to batch
            if hasattr(event, 'body') and isinstance(event.body, dict):
                self.batch_events.append(event.body)
            elif isinstance(event, dict):
                self.batch_events.append(event)
            else:
                logger.warning(f"Unsupported event type for batching: {type(event)}")
                return event

            # Check if we should flush
            now = pd.Timestamp.now()
            should_flush = (
                len(self.batch_events) >= self.batch_size or
                (now - self.last_flush_time).total_seconds() >= self.flush_interval
            )

            if should_flush:
                self._flush_batch()

            return event

        except Exception as e:
            logger.error(f"Error in batch InfluxDB processing: {e}")
            return event

    def _flush_batch(self):
        """Flush accumulated events to InfluxDB."""
        if not self.batch_events:
            return

        try:
            # Convert batch to DataFrame
            df = pd.DataFrame(self.batch_events)

            # Write to InfluxDB
            self._write_dataframe_to_influx(df)

            logger.debug(f"Flushed {len(self.batch_events)} events to InfluxDB")

            # Clear batch
            self.batch_events.clear()
            self.last_flush_time = pd.Timestamp.now()

        except Exception as e:
            logger.error(f"Error flushing batch to InfluxDB: {e}")
            # Clear batch anyway to prevent infinite accumulation
            self.batch_events.clear()
            raise

    def __del__(self):
        """Ensure final flush on cleanup."""
        try:
            self._flush_batch()
        except Exception:
            pass  # Ignore errors during cleanup
