# mlrun_influx_store/__init__.py
from .datastore import InfluxStore

# lazy re-exports to keep import light
def get_dataitem(uri: str):
    from .api import get_dataitem as _gd
    return _gd(uri)

def read_df(uri: str):
    from .api import read_df as _rd
    return _rd(uri)

def log_dataset(*args, **kwargs):
    from .api import log_dataset as _ld
    return _ld(*args, **kwargs)

def write_df(*args, **kwargs):
    from .api import write_df as _wd
    return _wd(*args, **kwargs)

__all__ = ["InfluxStore", "get_dataitem", "read_df", "log_dataset", "write_df"]
