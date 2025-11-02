<div align="center">
<pre style="line-height: 1.1;">
 ██████   █████ █████ █████   █████████  
░░██████ ░░███ ░░███ ░░███   ███░░░░░███ 
 ░███░███ ░███  ░░███ ███   ███     ░░░  
 ░███░░███░███   ░░█████   ░███          
 ░███ ░░██████    ░░███    ░███          
 ░███  ░░█████     ░███    ░░███     ███ 
 █████  ░░█████    █████    ░░█████████  
 ░░░░░    ░░░░░    ░░░░░      ░░░░░░░░░  
  ███████████                        ███ 
 ░█░░░███░░░█                       ░░░ ‎ 
 ░   ░███  ░   ██████   █████ █████ ████ 
     ░███     ░░░░░███ ░░███ ░░███ ░░███ 
     ░███      ███████  ░░░█████░   ░███ 
     ░███     ███░░███   ███░░░███  ░███ 
     █████   ░░████████ █████ █████ █████
    ░░░░░     ░░░░░░░░ ░░░░░ ░░░░░ ░░░░░

</pre>

# NYC Taxi Trip - `dwib`

An _end-to-end_ data analytics using [NYC Taxi Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

[Getting started](#getting-started) •
[Installation](#installation) •
[Configuration](#configuration) •
[Visualization](#Visualization)

</div>

# Getting Started
This repo splits into **Apache Airflow** project, **Data Build Tool** project and **Visualization**.


# Installation
## Apache Airflow
For local development we can use [uv](https://docs.astral.sh/uv).

```bash
uv venv
cd "Apache Airflow"
uv pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.1/constraints-3.10.txt"
```
and run `uv run airflow standalone`. Open browser to `localhost:8080`

Or we can use `docker compose`
```bash
cd "Apache Airflow"
docker compose up --build
```

## Data Build Tool

Use [uv](https://docs.astral.sh/uv/pip/environments/#creating-a-virtual-environment) to manage virtual environment
```bash
uv venv
uv pip install dbt-duckdb

source .venv/bin/activate
```

> [!TIP] 
> If we aren't sourcing, we can prefix with `uv run` e.g. `uv run dbt debug`

Validate dbt version with `dbt --version`. Follow official documentation at [docs.getdbt.com](https://docs.getdbt.com/docs/about-dbt-install).

Create project
```bash
export DO_NOT_TRACK=1

cd Data\ Build\ Tool
dbt init --profiles-dir . nyc_taxi_project
```

Remove example models and create required model layer
```bash
cd nyc_taxi_project/
rm -rf models/example
mkdir models/{staging,intermediate,mart}
```

Check project config
```bash
dbt debug
```

Running project
```bash
dbt run
dbt test
```

Generate documentation
```bash
dbt docs generate
```

# Configuration
Apache airflow
![airflow-dashboard](_extra/Airflow%20Dashboard.png)

Registred DAGs
![dags](_extra/Dags.png)
# Visualization

Using marimo notebook to quickly visualize data:
```bash
cd Visualization

uv pip install marimo
marimo edit --sandbox notebook.py
#or
uvx marimo edit --sandbox notebook.py
```

Run notebook as a script
```bash
uv run notebook.py --help
```

## Metabase
~~Download [duckdb-metabase-driver](https://github.com/motherduckdb/metabase_duckdb_driver/releases). Put the file in Metabase/plugins/.~~

Run `docker compose up --build` and access metabase at `http://localhost:3000`.

Add duckdb as data source
![duckdb-data-source](_extra/Database%20·%20Admin%20·%20Metabase.png)

Dashboard
![metabase](_extra/A%20look%20at%20Raw%20Yellow%20Tripdata%20·%20Dasbor%20·%20Metabase.png)

---