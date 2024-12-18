from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from source_uhrs.source import SourceUhrs


@pytest.mark.parametrize(
    ("http_status", "response_text", "expected_result"),
    [
        (HTTPStatus.OK, "", (True, None)),
        (HTTPStatus.BAD_REQUEST, "Wierd", (False, "Wierd")),
    ],
)
def test_check_connection(mocker, http_status, response_text, expected_result):
    mocker.patch.object(SourceUhrs, "get_access_token", lambda *x: "")
    with patch("requests.get") as mock_request:
        mock_request.return_value.status_code = http_status
        mock_request.return_value.text = response_text
        source = SourceUhrs()
        logger_mock, config_mock = MagicMock(), MagicMock()
        assert source.check_connection(logger_mock, config_mock) == expected_result


def test_streams(mocker):
    mocker.patch.object(SourceUhrs, "get_access_token", lambda *x: "")
    source = SourceUhrs()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    expected_streams_number = 3
    assert len(streams) == expected_streams_number
