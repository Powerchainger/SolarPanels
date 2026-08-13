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

    `inverters` is a dict keyed by inverter label ("inverter_1", …), values in
    watts.  `total_power` is the sum of all inverter powers for the cycle.

    Note: the legacy CSV format carried a `user_value` (the site name, a
    customer name under GDPR). It was dropped on 2026-08-13 to minimize
    personal-data exposure — see the GDPR notes.
    """

    model_config = ConfigDict(extra="forbid")

    timestamp: datetime           # UTC, from last_report_date of first inverter
    inverters: dict[str, float]   # {"inverter_1": 150.5, …}
    total_power: float            # sum of inverters values


class GrowattRow(BaseModel):
    """One row per Growatt device per cycle.

    All three fields are converted from the string values returned by
    api.min_energy() — the Growatt API returns them as strings even though
    they are numeric.
    """

    model_config = ConfigDict(extra="forbid")

    timestamp: datetime   # UTC, injected by the scheduler at cycle start
    device_sn: str
    ac_power_watts: float   # from `pac`
    pv_power_watts: float   # from `ppv`
    soc_percent: int        # from `bdc1Soc`
