FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && \
    rm -rf /var/lib/lists/*

ENV APP_HOME=/app 
WORKDIR $APP_HOME 

COPY pyproject.toml poetry.lock* README.md /app/
COPY . /app


RUN pip install "poetry==2.2.1"
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "etl.cli"]