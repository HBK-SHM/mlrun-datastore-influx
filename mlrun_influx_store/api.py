# mlrun_influx_store/api.py
from mlrun.datastore.base import DataItem
from mlrun.datastore import store_manager
from .datastore import InfluxStore

def get_dataitem(uri: str) -> DataItem:
    if not uri.startswith("influx://"):
        raise ValueError("URI must start with influx://")
    key = uri.split("://", 1)[1]
    store = InfluxStore(parent=store_manager, schema="influx", name="influx", endpoint="")
    return DataItem(key, artifact_url=uri, store=store, subpath="")

def read_df(uri: str):
    return get_dataitem(uri).as_df()
