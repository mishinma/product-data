# Demo project

This is a demo project to demostrate loading of data from an API into a database,
and transformation of data with dbt for an ml application.

## Setup

Install project with poetry:

```bash
poetry install
```

## Usage

Load data from API:

```bash
poetry run fetch_data
poetry run insert_data
```

Transform data with dbt:

```bash
cd dbt_models
poetry run dbt run
```

Run a prophet model (requires simulating historical data):

```bash
poetry run forecast <product_id>
```

Simulate ingesting historical data for the past year and run dbt:

```bash
poetry run python simulate_data_load.py
cd dbt_models
poetry run dbt run --target devsim
```
