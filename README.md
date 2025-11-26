# API ETL
A sample project to demonstrate the ability to use modern data engineering tooling to create a simple ETL process from an API into Postgres.

Mostly just a space for me to try out contemporary data tooling and refresh my python knowledge

## Dev Usage
```bash
# installation via poetry
poetry install

# run fectch to pretty print JSON data from Meteo for coorindates 5, 3
poetry run python -m etl fetch  --long 5 --lat 3
```
