# mlrun_influx_store/__init__.py
from .datastore import InfluxStore

# Best-effort self-registration so the scheme works even if MLRun didn’t load EPs.
try:
    # Import after MLRun is importable; if MLRun isn’t installed yet, this no-ops.
    import mlrun.datastore.datastore as _ds  # type: ignore
    # MLRun 1.9.x keeps a module-level registry dict named `stores`
    if hasattr(_ds, "stores") and isinstance(_ds.stores, dict):
        _ds.stores.setdefault("influx", InfluxStore)
except Exception:
    # Don’t hard-fail on import; this is just a convenience path.
    pass

# Lightweight, lazy re-exports of helper API (avoids importing heavy deps at import time)
def get_dataitem(uri: str):
    from .api import get_dataitem as _get_dataitem
    return _get_dataitem(uri)

def read_df(uri: str):
    from .api import read_df as _read_df
    return _read_df(uri)

__all__ = ["InfluxStore", "get_dataitem", "read_df"]
