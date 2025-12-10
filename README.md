# API ETL
A sample project to demonstrate the ability to use modern data engineering tooling to create data infrastructure from an API into Postgres.

Mostly just a space for me to try out contemporary data tooling and refresh my python knowledge.

## Project Structure

For now there are three main components to this project.

### `etl`
This module was the first to be written. It is responsible for encapsulating base database and data models, batch ETL commands and other like basic plumbing.

Here we have represented the concept of fetching from an API as `FetchMetadata` and weather observations as `Observation`.

The commands here are exposed generally serve to fetch from an API and either print the result, useful for command line work (piping and such), or normalize and persist in database, along with metadata about the fetch operation.

### `dashboard`
This module was the second to be written. It's a very simple analytics dashboard meant more than anything to explore new BI technology and refresh pandas knowledge.

### `streaming`

This module was the third to be written. It is responsible for wrapping around some of `etl` in a stream-first data lake architecture. The functional difference compared to `etl` is that the commands here compose parts of a stream-processing pipeline.

Currently, there're two commands:

#### `fetch-and-publish`
A command for fetching from an API, persisting data directly into local storage, and finally publishing an event representing the fetch operation with metadata exposing where the data is stored.

#### `consume-fetch-events`
A command for consuming published fetch events, normalizing the data they reference and persisting it to a database.

## Basic Usage
Here is a (very) basic guide to run a containerized development enviroment.

```bash
# build our services, pulling newest images
docker compose build --pull

# run our tests to ensure all is good
docker compose run --rm app poetry run pytest -q 

# run deamonized services during development
docker compose up -d

# run etl cli to see available batch commands
docker compose run --rm app poetry run etl --help

# run streaming cli to see available realtime commands
docker compose run --rm app poetry run streaming --help

```


## Dev Usage
Here is a (very) basic guide to create an environment to begin local development, without needing Docker, however docker is very useful and might save you some  (read: a fair amount of) setup time.

Be sure have set up python requirements as described in `pyproject.toml`, postgres (and its credentials) and relevant env vars described in `etl/config.py` and `tests/conftest.py` before attempting the following steps.
```bash
# installation via poetry
poetry install

# run tests to make sure everything is ok
poetry run pytest
```

If these steps succesfully ran, as a last practice of due diligence run to one of both commands, check resulting data quality and if all checks out you should be good to go to get developing.

