from setuptools import setup, find_packages

setup(
    name="mlrun-influx-plugin",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["mlrun>=1.6.0", "influxdb-client>=1.39"],
    entry_points={
        "mlrun.datastore": [
            "influx = mlrun_influx_store:InfluxStore"
        ],
    },
)
