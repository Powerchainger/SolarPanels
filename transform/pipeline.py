"""Pure transform functions: raw API response → list of Row models.

No IO, no DB, no network.  Each function takes a raw dict (already json.loads'd)
and returns a list of validated Row models.  Errors raise rather than returning
None — the caller (scheduler job) decides whether to log-and-skip or retry.

Legacy parsing logic:
  EnPhase: EnPhase/cloud_measurements.py:18-50
  Growatt: Growatt/measurements.py:39-53
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from transform.schemas import EnphaseRow, GrowattRow

logger = logging.getLogger(__name__)


def _optional[T](cast: Callable[[Any], T], value: Any) -> T | None:
    """`cast(value)`, or None when the provider sent no value (missing, null, "")."""
    if value is None or value == "":
        return None
    return cast(value)


def parse_enphase(raw_body: list | dict, *, account_id: str) -> list[EnphaseRow]:
    """Parse EnPhase /api/v4/systems/inverters_summary_by_envoy_or_site response.

    The API returns a JSON array; element 0 contains a `micro_inverters` list.
    Each inverter has `last_report_date` (ISO-8601 string) and
    `power_produced.value` (float, in watts).

    Legacy converted timestamps to Europe/Amsterdam — we convert to UTC here.

    Raises KeyError/IndexError on malformed input so the caller can log it
    as a raw-response-level failure.
    """
    if isinstance(raw_body, dict):
        raw_body = [raw_body]

    if not raw_body:
        raise ValueError("empty EnPhase response body")

    envelope = raw_body[0]
    micro_inverters = envelope.get("micro_inverters")
    if not micro_inverters:
        raise ValueError("missing or empty micro_inverters in EnPhase response")

    inverters: dict[str, float | None] = {}
    timestamp: datetime | None = None

    for i, inv in enumerate(micro_inverters):
        report_date_raw = inv.get("last_report_date")
        if report_date_raw is None:
            continue

        # Parse the ISO timestamp — API returns timezone-aware strings.
        dt = datetime.fromisoformat(report_date_raw)
        # Convert to UTC for consistent storage.
        if dt.tzinfo is None:
            # Legacy sometimes produced naive datetimes; treat as UTC if tz-naive.
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)

        # The newest report is the cycle timestamp: one inverter that has been
        # offline for days must not pin the whole record to its last report.
        if timestamp is None or dt > timestamp:
            timestamp = dt

        # Keyed by serial number, which stays put if EnPhase reorders the
        # list; the position is only a fallback for an entry without one.
        label = inv.get("serial_number") or f"inverter_{i + 1}"
        # A null power (non-reporting inverter) stays null rather than 0, so
        # the consumer can tell "no data" from "producing nothing".
        inverters[label] = _optional(float, (inv.get("power_produced") or {}).get("value"))

    if timestamp is None:
        raise ValueError("no valid inverter timestamps in EnPhase response")

    reported = [p for p in inverters.values() if p is not None]
    total_power = sum(reported) if reported else None

    return [
        EnphaseRow(
            account_id=account_id,
            timestamp=timestamp,
            inverters=inverters,
            total_power=total_power,
        )
    ]


def parse_growatt(
    device_responses: list[tuple[str, dict]],
    timestamp: datetime,
    *,
    account_id: str,
) -> list[GrowattRow]:
    """Parse per-device min_energy responses into GrowattRows.

    Args:
        device_responses: List of (device_sn, raw_response_dict) tuples.
                          The upstream client filters type==7 before calling this.
        timestamp:        UTC timestamp for this cycle (injected by caller).
        account_id:       The account the responses belong to; copied into
                          every row so the output API can tell accounts apart.

    Legacy field mapping (Growatt/measurements.py:39-53):
        pac     → ac_power_watts (float)
        ppv     → pv_power_watts (float)
        bdc1Soc → soc_percent   (int)

    All three arrive as strings from the API; we coerce here. A missing,
    null or empty value becomes None: no data is not the same as zero.
    """
    # Ensure timestamp is UTC-aware.
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    elif timestamp.tzinfo != UTC:
        timestamp = timestamp.astimezone(UTC)

    rows: list[GrowattRow] = []
    for device_sn, raw in device_responses:
        rows.append(
            GrowattRow(
                account_id=account_id,
                timestamp=timestamp,
                device_sn=device_sn,
                ac_power_watts=_optional(float, raw.get("pac")),
                pv_power_watts=_optional(float, raw.get("ppv")),
                soc_percent=_optional(int, raw.get("bdc1Soc")),
            )
        )

    return rows
