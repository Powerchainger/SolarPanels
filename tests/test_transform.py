"""Tests for the transform pipeline — pure functions, no IO."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from transform.schemas import EnphaseRow, GrowattRow
from transform.pipeline import parse_enphase, parse_growatt


# ---------------------------------------------------------------------------
# Fixtures (synthesized from PLAN.md + legacy code shapes)
# ---------------------------------------------------------------------------

ENPHASE_RAW = [
    {
        "micro_inverters": [
            {
                "last_report_date": "2026-07-17T14:00:00+02:00",
                "power_produced": {"value": 150.5},
            },
            {
                "last_report_date": "2026-07-17T14:00:00+02:00",
                "power_produced": {"value": 200.3},
            },
        ]
    }
]

GROWATT_DEVICE_RESPONSES: list[tuple[str, dict]] = [
    ("SN001", {"pac": "1500", "ppv": "3200", "bdc1Soc": "85"}),
    ("SN002", {"pac": "800", "ppv": "1100", "bdc1Soc": "60"}),
]


# ---------------------------------------------------------------------------
# parse_enphase
# ---------------------------------------------------------------------------


def test_parse_enphase_shape():
    rows = parse_enphase(ENPHASE_RAW, account_id="acct")
    assert len(rows) == 1
    row = rows[0]
    assert isinstance(row, EnphaseRow)
    assert row.account_id == "acct"
    assert not hasattr(row, "user_value")  # site name dropped (GDPR minimize)
    assert row.inverters == {"inverter_1": 150.5, "inverter_2": 200.3}
    assert row.total_power == pytest.approx(350.8)


def test_parse_enphase_timestamp_is_utc():
    rows = parse_enphase(ENPHASE_RAW, account_id="acct")
    ts = rows[0].timestamp
    assert ts.tzinfo is not None
    assert ts.utcoffset().total_seconds() == 0  # UTC
    # Original was 14:00+02:00 → 12:00 UTC
    assert ts.hour == 12


def test_parse_enphase_empty_body_raises():
    with pytest.raises(ValueError, match="empty"):
        parse_enphase([], account_id="acct")


def test_parse_enphase_missing_inverters_raises():
    with pytest.raises(ValueError, match="micro_inverters"):
        parse_enphase([{}], account_id="acct")


def test_parse_enphase_dict_input_accepted():
    """Accept a single dict instead of a list (API sometimes returns that)."""
    rows = parse_enphase(ENPHASE_RAW[0], account_id="acct")
    assert len(rows) == 1


def test_parse_enphase_inverter_without_report_date_skipped():
    body = [
        {
            "micro_inverters": [
                {"last_report_date": "2026-07-17T14:00:00+02:00",
                 "power_produced": {"value": 100.0}},
                {"power_produced": {"value": 50.0}},  # no last_report_date
            ]
        }
    ]
    rows = parse_enphase(body, account_id="acct")
    assert rows[0].inverters == {"inverter_1": 100.0}
    assert rows[0].total_power == pytest.approx(100.0)


def _inverter(serial, report, power):
    return {"serial_number": serial, "last_report_date": report,
            "power_produced": power}


def test_parse_enphase_labels_inverters_by_serial_number():
    body = [{"micro_inverters": [
        _inverter("SN-B", "2026-07-17T12:00:00+00:00", {"value": 10.0}),
        _inverter("SN-A", "2026-07-17T12:00:00+00:00", {"value": 20.0}),
    ]}]
    assert parse_enphase(body, account_id="acct")[0].inverters == {"SN-B": 10.0, "SN-A": 20.0}


def test_parse_enphase_timestamp_is_newest_report():
    """One inverter offline for days must not pin the record to its last report."""
    body = [{"micro_inverters": [
        _inverter("SN-1", "2026-07-14T12:00:00+00:00", {"value": 0.0}),
        _inverter("SN-2", "2026-07-17T12:00:00+00:00", {"value": 20.0}),
    ]}]
    assert parse_enphase(body, account_id="acct")[0].timestamp == datetime(
        2026, 7, 17, 12, 0, tzinfo=UTC
    )


@pytest.mark.parametrize("power", [None, {"value": None}, {}])
def test_parse_enphase_null_power_is_null_not_a_crash(power):
    body = [{"micro_inverters": [
        _inverter("SN-1", "2026-07-17T12:00:00+00:00", power),
        _inverter("SN-2", "2026-07-17T12:00:00+00:00", {"value": 20.0}),
    ]}]
    row = parse_enphase(body, account_id="acct")[0]
    assert row.inverters == {"SN-1": None, "SN-2": 20.0}
    assert row.total_power == pytest.approx(20.0)


def test_parse_enphase_total_is_null_when_no_inverter_reported_power():
    body = [{"micro_inverters": [
        _inverter("SN-1", "2026-07-17T12:00:00+00:00", None),
    ]}]
    row = parse_enphase(body, account_id="acct")[0]
    assert row.total_power is None
    # The record stays serializable for the output API.
    assert row.model_dump(mode="json")["inverters"] == {"SN-1": None}


# ---------------------------------------------------------------------------
# parse_growatt
# ---------------------------------------------------------------------------


def test_parse_growatt_shape():
    ts = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
    rows = parse_growatt(GROWATT_DEVICE_RESPONSES, timestamp=ts, account_id="acct")
    assert len(rows) == 2

    assert isinstance(rows[0], GrowattRow)
    assert {r.account_id for r in rows} == {"acct"}
    assert rows[0].device_sn == "SN001"
    assert rows[0].ac_power_watts == pytest.approx(1500.0)
    assert rows[0].pv_power_watts == pytest.approx(3200.0)
    assert rows[0].soc_percent == 85

    assert rows[1].device_sn == "SN002"
    assert rows[1].ac_power_watts == pytest.approx(800.0)
    assert rows[1].pv_power_watts == pytest.approx(1100.0)
    assert rows[1].soc_percent == 60


def test_parse_growatt_timestamps_are_utc():
    ts = datetime(2026, 7, 17, 14, 0, 0)  # naive — should be treated as UTC
    rows = parse_growatt(GROWATT_DEVICE_RESPONSES, timestamp=ts, account_id="acct")
    for row in rows:
        assert row.timestamp.tzinfo is not None
        assert row.timestamp.utcoffset().total_seconds() == 0


def test_parse_growatt_coerces_string_values():
    """The Growatt API returns numeric fields as strings."""
    responses: list[tuple[str, dict]] = [
        ("SN001", {"pac": "1500.5", "ppv": "3200.7", "bdc1Soc": "85"}),
    ]
    ts = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
    rows = parse_growatt(responses, timestamp=ts, account_id="acct")
    assert rows[0].ac_power_watts == pytest.approx(1500.5)
    assert rows[0].pv_power_watts == pytest.approx(3200.7)


def test_parse_growatt_empty_responses():
    ts = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
    rows = parse_growatt([], timestamp=ts, account_id="acct")
    assert rows == []


@pytest.mark.parametrize("raw", [{}, {"pac": None, "ppv": None, "bdc1Soc": None},
                                 {"pac": "", "ppv": "", "bdc1Soc": ""}])
def test_parse_growatt_missing_values_are_null(raw):
    """No data is not zero: missing, null and empty values become None."""
    ts = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
    rows = parse_growatt([("SN001", raw)], timestamp=ts, account_id="acct")
    assert rows[0].ac_power_watts is None
    assert rows[0].pv_power_watts is None
    assert rows[0].soc_percent is None
