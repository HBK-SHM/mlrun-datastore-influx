# mlrun-datastore-influx

📦 MLRun InfluxDB Plugin
A custom MLRun datastore plugin for accessing InfluxDB directly with influx:// URIs.
This lets you treat Influx measurements as first-class MLRun datasets with full artifact metadata, lineage, and environment support (dev/staging/prod).
🚀 Features
Use URIs like:
influx://bucket/measurement?field=temp&tag=sensor:bridge01&env=staging
Supports multi-environment configs (dev, staging, prod).
Reads directly into a pandas DataFrame.
Automatically attaches tags, fields, measurement, env as MLRun artifact metadata.
Secure handling of tokens via project secrets, while url and org come from project params.
📥 Installation

pip install git+https://github.com/<your-org>/mlrun-influx-plugin.git@main

Or add it to your MLRun function requirements:
fn.with_requirements([
    "git+https://github.com/<your-org>/mlrun-influx-plugin.git@main"
])

⚙️ Project Configuration
Define URLs & Orgs as project parameters, and tokens as secrets:
import mlrun

project = mlrun.get_or_create_project("bridge-ai", "./")

### Non-sensitive configs
project.set_params({
    "INFLUX_DEV_URL": "http://influx-dev:8086",
    "INFLUX_DEV_ORG": "dev-org",
    "INFLUX_STAGING_URL": "http://influx-staging:8086",
    "INFLUX_STAGING_ORG": "staging-org",
    "INFLUX_PROD_URL": "http://influx-prod:8086",
    "INFLUX_PROD_ORG": "prod-org",
})

### Sensitive tokens
project.set_secrets({
    "INFLUX_DEV_TOKEN": "xxx-dev-token-xxx",
    "INFLUX_STAGING_TOKEN": "yyy-staging-token-yyy",
    "INFLUX_PROD_TOKEN": "zzz-prod-token-zzz",
})
🔍 Usage
import mlrun

### Dev (default env)
df_dev = mlrun.get_dataitem("influx://sensors/temperature?field=temp").as_df()

### Staging
df_staging = mlrun.get_dataitem("influx://sensors/temperature?field=temp&env=staging").as_df()

### Production
df_prod = mlrun.get_dataitem("influx://sensors/temperature?field=temp&env=prod").as_df()

### Log dataset with lineage
ctx = mlrun.get_or_create_ctx("test-influx")
ctx.log_dataset("bridge_temp", df=df_prod, labels={"env": "prod"})
🏗️ Development
Clone this repo:
git clone https://github.com/<your-org>/mlrun-influx-plugin.git
cd mlrun-influx-plugin
Install locally:
pip install -e .
Run tests / examples (TODO add tests).
📌 Example URI Parameters
field → Influx field to query
tag → Tag filter (tag=sensor:bridge01)
env → Environment (dev | staging | prod, default = dev)
Example:
influx://metrics/cpu_load?field=usage&tag=host:server01&env=prod
🧩 Entry Point
This plugin registers with MLRun as a datastore kind:
entry_points={
    "mlrun.datastore": [
        "influx = mlrun_influx_store:InfluxStore"
    ],
}
So you can access it with:
mlrun.get_dataitem("influx://...")
