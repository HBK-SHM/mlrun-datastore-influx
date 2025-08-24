from setuptools import setup, find_packages

setup(
    name="mlrun-influx-plugin",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["mlrun>=1.9.2", "influxdb-client>=1.39"],
    entry_points={
        "mlrun.datastore": [
            "influx = mlrun_influx_store:InfluxStore"
        ],
        "mlrun.datastore.v2": [
            "influx = mlrun_influx_store:InfluxStore",
        ],
    },
)
