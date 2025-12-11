import pytest, requests, requests_mock, json
from etl.extract import _fetch_data, ExtractError


@pytest.mark.unit
@pytest.mark.parametrize(
    "status_code",
    [
        (500),
        (404),
        (429),
    ],
    ids=["Server Error", "Client Error", "Rate Limit"],
)
def test__fetch_data_extract_errors(status_code):
    url = "http://test"
    session = requests.Session()
    with requests_mock.Mocker() as m:
        m.get(url, status_code=status_code)

        with pytest.raises(ExtractError):
            _fetch_data(session, url, dict())


@pytest.mark.unit
def test__fetch_data_other_errors():
    url = "http://test"
    session = requests.Session()

    with requests_mock.Mocker() as m:
        m.get(url, exc=requests.RequestException())

        with pytest.raises(requests.RequestException):
            _fetch_data(session, url, dict())

    with requests_mock.Mocker() as m:
        m.get(url, text="not json", headers={"Content-Type": "application/json"})

        with pytest.raises(json.JSONDecodeError):
            _fetch_data(session, url, dict())
