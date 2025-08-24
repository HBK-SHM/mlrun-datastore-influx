# setup.py
from setuptools import setup, find_packages

setup(
    name="mlrun-influx-store",
    version="0.0.1",
    packages=find_packages(include=["mlrun_influx_store", "mlrun_influx_store.*"]),
    python_requires=">=3.8",
    install_requires=[
        "mlrun>=1.9.2,<1.10",
        "influxdb-client>=1.39",
        "pandas>=1.3",
    ],
    entry_points={
        # MLRun 1.9.x reads this; also add v2 for forward-compat
        "mlrun.datastore": [
            "influx = mlrun_influx_store.datastore:InfluxStore",
        ],
        "mlrun.datastore.v2": [
            "influx = mlrun_influx_store.datastore:InfluxStore",
        ],
    },
)
