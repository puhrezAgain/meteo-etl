import pytest, requests, json
from etl.sources import SourceName
from etl.load import LoadError
from etl.app import ETLError, _handle_etl_error, et, etl, ETError
from etl.db import FetchMetadata, Observation, FetchStatus


def _http_error_maker(code: int, text: str) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = code
    response._content = text.encode()
    return requests.exceptions.HTTPError(response=response)


def _raise(e):
    raise e


class TestETApp:
    @pytest.mark.unit
    def test_et_success(self, override_meteo_api, weather_records):
        returned = et(5.0, 3.0, SourceName.METEO)
        assert len(returned) == len(weather_records)
        assert returned == weather_records

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "error",
        [
            (_http_error_maker(500, "whatever"),),
            (json.JSONDecodeError("w", "e", 1),),
            (Exception("whatever"),),
        ],
        ids=["HTTP", "JSON", "WE"],
    )
    def test_et_errors(self, error, monkeypatch):
        monkeypatch.setattr(
            "etl.sources.MeteoSource.run_transform", lambda *args: _raise(error)
        )

        with pytest.raises(ETError):
            et(5.0, 3.0, SourceName.METEO)

    @pytest.mark.integration
    def test_etl_success(
        self,
        override_meteo_api,
        monkeypatch,
        db_session_maker,
        db_session,
        weather_records,
    ):
        fetch_id = etl(5.0, 3.0, SourceName.METEO, db_session_maker)
        fetch = db_session.get(FetchMetadata, fetch_id)
        assert fetch
        assert fetch.status == FetchStatus.SUCCESS
        assert db_session.query(Observation).count() == len(weather_records)

    @pytest.mark.integration  # integration because etl does a little db interaction before error handling
    @pytest.mark.parametrize(
        "error",
        [
            (LoadError(),),
            (_http_error_maker(500, "whatever"),),
            (json.JSONDecodeError("w", "e", 1),),
            (Exception("whatever"),),
        ],
        ids=["Load", "HTTP", "JSON", "WE"],
    )
    def test_etl_error(self, error, monkeypatch, db_session_maker, db_session):
        monkeypatch.setattr(
            "etl.sources.MeteoSource.run_transform", lambda *args: _raise(error)
        )

        with pytest.raises(ETLError) as exc:
            etl(5.0, 3.0, SourceName.METEO, db_session_maker)

        fetch = db_session.get(FetchMetadata, exc.value.fetch_id)
        assert fetch
        assert fetch.status == FetchStatus.ERROR


@pytest.mark.unit
@pytest.mark.parametrize(
    "error, expected",
    [
        (LoadError(), 200),
        (_http_error_maker(500, "whatever"), 500),
        (json.JSONDecodeError("w", "e", 1), 200),
        (Exception("whatever"), 500),
    ],
    ids=["Load", "HTTP", "JSON", "WE"],
)
def test_handle_etl_error_status(error, expected):
    resp = _handle_etl_error(error)
    assert resp.status_code == expected
