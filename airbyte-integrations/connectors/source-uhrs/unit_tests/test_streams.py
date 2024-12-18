from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_uhrs.source import UhrsStream


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(UhrsStream, "path", "v0/example_endpoint")
    mocker.patch.object(UhrsStream, "primary_key", "test_primary_key")
    mocker.patch.object(UhrsStream, "__abstractmethods__", set())
    mocker.patch.object(UhrsStream, "__init__", lambda x: None)


def test_request_params(patch_base_class):
    stream = UhrsStream()
    inputs = {"stream_slice": {"pst_report_dt": "2022-02-12"}, "stream_state": None, "next_page_token": None}
    expected_params = {
        'startDate': "2022-02-12",
        'endDate': "2022-02-12",
        'hitappIds': '',
        'applicationId': '-1',
        'vendorId': '-1',
        'project': '',
    }
    assert stream.request_params(**inputs) == expected_params


def test_parse_response(patch_base_class):
    stream = UhrsStream()
    stream.download_datetime = "2022-02-03 12:55:34"
    inputs = {
        "response": MagicMock(
            url="https://example.com?startDate=2022-02-02&endDate=2022-02-02",
            json=lambda: [{"key": "val"}]
        )
    }
    expected_parsed_object = {
        "key": "val",
        "pst_report_dt": "2022-02-02",
        "utc_download_dttm": "2022-02-03 12:55:34",
    }
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = UhrsStream()
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {"Accept": "application/json"}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = UhrsStream()
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = UhrsStream()
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = UhrsStream()
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
