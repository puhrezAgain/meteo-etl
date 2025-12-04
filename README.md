# API ETL
A sample project to demonstrate the ability to use modern data engineering tooling to create a simple ETL process from an API into Postgres.

Mostly just a space for me to try out contemporary data tooling and refresh my python knowledge

## Basic Usage
Here is a (very) basic guide to run a containerized development enviroment.

```bash
# build our services, pulling newest images
docker compose build --pull

# run our tests to ensure all is good
docker compose run --rm app poetry run pytest -q 

# run our migrations to get database to state
docker compose run --rm migrations

# (optional) run deamonized database service during development
docker compose up -d postgres

# run cli to see available commands
docker compose run --rm app poetry run etl --help

# run deamonized dashboard server
docker compose up -d streamlit
```


## Dev Usage
Here is a (very) basic guide to create an environment to begin local development, without needing Docker, however docker is very useful and might save you some time.

Be sure have set up python requirements as described in `pyproject.toml`, postgres (and its credentials) and relevant env vars described in `etl/config.py` and `tests/conftest.py` before attempting the following steps.
```bash
# installation via poetry
poetry install

# run tests to make sure everything is ok
poetry run pytest

# run cli with help to see available commands
poetry run etl --help
```

If these steps succesfully ran, as a last practice of due diligence run to one of both commands, check resulting data quality and if all checks out you should be good to go to get developing.

