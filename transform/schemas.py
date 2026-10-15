"""Pydantic models for processed output rows.

Each model serves double duty:
  1. Validation target when the transform pipeline parses raw API responses.
  2. Wire-format body for POST to the output API (and `record` field in
     `processed_results` in Mongo — see db/models.py ProcessedRecord).

Timestamps are always UTC-aware. Convert from local timezones at the boundary;
never store local-tz datetimes in Mongo or POST bodies.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from db.models import ConfigDict


class EnphaseRow(BaseModel):
    """One row per EnPhase cycle.

    `inverters` is a dict keyed by inverter serial number (position label
    "inverter_N" only for an entry without one), values in watts, None for an
    inverter that reported no power.  `total_power` is the sum of the
    reported values, None if no inverter reported any.

    Note: the legacy CSV format carried a `user_value` (the site name, a
    customer name under GDPR). It is deliberately left out to minimize
    personal-data exposure — see the GDPR notes.
    """

    model_config = ConfigDict(extra="forbid")

    account_id: str               # as seeded with manage_keys.py
    timestamp: datetime                   # UTC, newest last_report_date
    inverters: dict[str, float | None]    # {"<serial_number>": 150.5, …}
    total_power: float | None             # sum of the non-null values


class GrowattRow(BaseModel):
    """One row per Growatt device per cycle.

    All three fields are converted from the string values returned by
    api.min_energy() — the Growatt API returns them as strings even though
    they are numeric. A value the API leaves missing, null or empty is None.
    """

    model_config = ConfigDict(extra="forbid")

    account_id: str       # as seeded with manage_keys.py
    timestamp: datetime   # UTC, injected by the scheduler at cycle start
    device_sn: str
    ac_power_watts: float | None   # from `pac`
    pv_power_watts: float | None   # from `ppv`
    soc_percent: int | None        # from `bdc1Soc`
