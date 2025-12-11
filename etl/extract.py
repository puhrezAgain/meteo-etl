"""
etl.extract encapuslates logic dealing with HTTP interactions
"""

import requests, json, logging
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException, ConnectionError
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class ExtractError(HTTPError):
    pass


def run_extractor(url: str, params: dict, user_agent: str | None = None) -> dict:
    http_session = _make_session(user_agent)
    return _fetch_data(http_session, url, params)


def _fetch_data(session: requests.Session, url: str, params: dict):
    try:
        response = session.get(url, params=params)
        response.raise_for_status()
    except HTTPError as httpError:
        status = httpError.response.status_code

        if status == 429:
            logger.error("Rate limited (429) fetching %s", url)

        if status is not None and 400 <= status < 500:
            logger.error("Client error %s fetching %s", status, url)

        if status is not None and 500 <= status < 600:
            logger.error("Server error %s from %s after retries", status, url)

        logger.exception("HTTP error fetching %s (status=%s)", url, status)
        raise ExtractError(
            request=httpError.request, response=httpError.response
        ) from httpError

    except RequestException as requestException:
        # Network errors after retries
        logger.exception(
            "Network/request errror fetching %s: %s", url, requestException
        )
        raise

    try:
        return response.json()
    except json.JSONDecodeError as e:
        logger.error(
            "Invalid JSON from %s (status=%s len=%r, content-type=%s)",
            url,
            response.status_code,
            len(response.content or b""),
            response.headers.get("Content-Type"),
        )
        raise


def _make_session(
    user_agent: str | None,
    max_retries: int = 5,
    backoff: float = 0.5,
    allowed_methods: list[str] = [
        "GET",
    ],
) -> requests.Session:
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(allowed_methods),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": f"{user_agent}"})
    return session
