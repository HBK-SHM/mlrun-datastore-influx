# mlrun_influx_store/__init__.py
from .datastore import InfluxStore
from .targetstore import InfluxTarget

# Register the InfluxTarget with MLRun's target system
def _register_influx_target():
    """Register InfluxTarget with MLRun's kind_to_driver mapping."""
    try:
        from mlrun.datastore.targets import kind_to_driver, TargetTypes

        # Add influx to TargetTypes if not already there
        if not hasattr(TargetTypes, 'influx'):
            TargetTypes.influx = "influx"

        # Register the target driver
        kind_to_driver["influx"] = InfluxTarget

    except ImportError:
        # MLRun not available, skip registration
        pass

# Register on import
_register_influx_target()

# lazy re-exports to keep import light
def get_dataitem(uri: str, ctx=None):
    from .api import get_dataitem as _gd
    return _gd(uri, ctx=ctx)

def read_df(uri: str, ctx=None):
    from .api import read_df as _rd
    return _rd(uri, ctx=ctx)

def log_dataset(*args, **kwargs):
    from .api import log_dataset as _ld
    return _ld(*args, **kwargs)

def write_df(*args, **kwargs):
    from .api import write_df as _wd
    return _wd(*args, **kwargs)

__all__ = ["InfluxStore", "InfluxTarget", "get_dataitem", "read_df", "log_dataset", "write_df"]
