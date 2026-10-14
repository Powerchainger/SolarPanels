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


def test_parse_growatt_missing_fields_default_zero():
    responses: list[tuple[str, dict]] = [
        ("SN001", {}),  # all fields missing
    ]
    ts = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
    rows = parse_growatt(responses, timestamp=ts, account_id="acct")
    assert rows[0].ac_power_watts == 0.0
    assert rows[0].pv_power_watts == 0.0
    assert rows[0].soc_percent == 0
