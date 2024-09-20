#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_victoria_metrics.source import SourceVictoriaMetrics


def test_streams(mocker):
    source = SourceVictoriaMetrics()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
