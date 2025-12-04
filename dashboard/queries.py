COUNT_LOCATIONS = "SELECT COUNT(DISTINCT (longitude, latitude)) FROM weather_observations"
VAR_COUNT_TABLE = "SELECT count(*) FROM {table}"
LAST_JOB_STATUS = "SELECT status from fetch_metadata ORDER BY created_at DESC LIMIT 1"