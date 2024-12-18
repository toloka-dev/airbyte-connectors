import pytest
from pytest import fixture
from source_uhrs.source import HitAppDisableEventsReport, HitAppJudgeReport, HitAppReport, IncrementalUhrsStream


@fixture
def patch_incremental_base_class(mocker):
    mocker.patch.object(IncrementalUhrsStream, "path", "v0/example_endpoint")
    # mocker.patch.object(IncrementalUhrsStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalUhrsStream, "__abstractmethods__", set())


@pytest.mark.parametrize(
    ("class_", "expected_cursor_field"),
    [
        (HitAppReport, "pst_report_dt"),
        (HitAppJudgeReport, "pst_report_dt"),
        (HitAppDisableEventsReport, "pst_report_dt"),
    ],
)
def test_cursor_field(patch_incremental_base_class, mocker, class_, expected_cursor_field):
    mocker.patch.object(class_, "__init__", lambda x: None)
    stream = class_()
    assert stream.cursor_field == expected_cursor_field


@pytest.mark.parametrize(
    ("current_report_dt", "last_report_dt", "expected"),
    [
        (None, "2022-02-23", "2022-02-23"),
        ("2022-02-22", "2022-02-23", "2022-02-23"),
        ("2022-02-23", None, "2022-02-23"),
    ],
)
def test_get_updated_state(patch_incremental_base_class, mocker, current_report_dt, last_report_dt, expected):
    mocker.patch.object(IncrementalUhrsStream, "__init__", lambda x: None)
    stream = IncrementalUhrsStream()
    inputs = {
        "current_stream_state": {"pst_report_dt": current_report_dt},
        "latest_record": {"pst_report_dt": last_report_dt},
    }
    expected_state = {"pst_report_dt": expected}
    assert stream.get_updated_state(**inputs) == expected_state


@pytest.mark.parametrize(
    ("start_dt", "end_dt", "state_dt", "expected"),
    [
        ("2022-01-02", "2022-01-02", "2022-01-01", ["2022-01-02"]),
        ("2022-01-01", "2022-01-02", "2022-01-02", ["2022-01-02"]),
        ("2022-01-02", "2022-01-02", "2022-01-07", []),
        ("2022-01-01", "2022-01-02", None, ["2022-01-01", "2022-01-02"]),
        ("2022-01-01", "2022-01-03", "2022-01-02", ["2022-01-02", "2022-01-03"]),
        ("2022-01-02", "2022-01-01", "2022-01-02", [])
    ],
)
def test_stream_slices(patch_incremental_base_class, start_dt, end_dt, state_dt, expected):
    stream = IncrementalUhrsStream(start_dt, end_dt)
    inputs = {"sync_mode": None, "cursor_field": [], "stream_state": {"pst_report_dt": state_dt}}
    expected_stream_slice = [{"pst_report_dt": e} for e in expected]
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_supports_incremental(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalUhrsStream, "cursor_field", "dummy_field")
    mocker.patch.object(IncrementalUhrsStream, "__init__", lambda x: None)
    stream = IncrementalUhrsStream()
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalUhrsStream, "__init__", lambda x: None)
    stream = IncrementalUhrsStream()
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalUhrsStream, "__init__", lambda x: None)
    stream = IncrementalUhrsStream()
    # TODO: replace this with your expected checkpoint interval
    expected_checkpoint_interval = None
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
