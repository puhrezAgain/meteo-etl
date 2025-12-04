import streamlit as st
import sqlalchemy as sql
import pandas as pd
from etl.config import DB_URL
from sqlalchemy.orm import sessionmaker, Session
from dashboard import queries


@st.cache_resource(show_spinner=False)
def get_engine() -> sql.Engine:
    return sql.create_engine(DB_URL, pool_pre_ping=True)


@st.cache_resource(show_spinner=False)
def get_session() -> sessionmaker:
    return sessionmaker(bind=get_engine())


@st.cache_data()
def load_observations() -> pd.DataFrame:
    query = """
        SELECT * FROM weather_observations ORDER BY timestamp LIMIT 5000
    """
    return pd.read_sql(query, get_engine())


@st.cache_data()
def load_metadata() -> pd.DataFrame:
    query = """
        SELECT * FROM fetch_metadata ORDER BY created_at LIMIT 5000
    """
    return pd.read_sql(query, get_engine())


@st.cache_data()
def get_counts():
    e = get_engine()
    observation_count = pd.read_sql(
        queries.VAR_COUNT_TABLE.format(table="weather_observations"), e
    ).iloc[0][0]
    metadata_count = pd.read_sql(
        queries.VAR_COUNT_TABLE.format(table="fetch_metadata"), e
    ).iloc[0][0]
    locations_count = pd.read_sql(queries.COUNT_LOCATIONS, e).iloc[0][0]

    return observation_count, metadata_count, locations_count


@st.cache_data()
def get_last_job_status():
    e = get_engine()
    status = pd.read_sql(queries.LAST_JOB_STATUS, e).iloc[0][0].capitalize()
    match status:
        case "error":
            return f"🔴 {status}"
        case "pending":
            return f"🟡 {status}"
        case _:
            return f"🟢 {status}"


def main():
    st.set_page_config(page_title="Meteo ETL Dashboard", layout="wide")

    st.title("🌤 Meteo ETL Dashboard")

    metadata_df = load_metadata()
    observation_df = load_observations()
    observation_count, metadata_count, location_count = get_counts()
    st.header("Observations", divider=True)
    col1, col2 = st.columns(2)
    col1.metric("Locations recorded", location_count)
    col2.metric("Observations recorded", observation_count)
    st.divider()

    summaries = observation_df.describe()
    col1, col2, col3 = st.columns(3)
    col1.metric("Global mean temp.", f"{round(summaries['temperature']['mean'], 2)} °C")
    col2.metric(
        "Global mean wind speed", f"{round(summaries['wind_speed']['mean'], 2)} kmh"
    )
    col3.metric(
        "Global mean wind precip.", f"{round(summaries['precipitation']['mean'], 2)} mm"
    )

    st.dataframe(observation_df)

    st.header("Fetch Jobs", divider=True)
    col1, col2 = st.columns(2)
    col1.metric("Last status recorded", get_last_job_status())
    col2.metric("Jobs recorded", metadata_count)
    st.dataframe(metadata_df)


if __name__ == "__main__":
    main()
