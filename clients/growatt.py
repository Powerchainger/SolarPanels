"""Async Growatt API client.

Wraps the synchronous `growattServer.OpenApiV1` SDK via `asyncio.to_thread` so
the event loop stays responsive.  Each method creates a fresh `OpenApiV1`
instance (it holds requests.Session state that is not thread- or async-safe).

The API returns string-encoded numerics even for clearly numeric fields — the
transform pipeline (`transform/pipeline.py`) coerces them; this client returns
the raw SDK output unchanged.

Legacy: Growatt/measurements.py:25-52
The 2-second inter-device sleep is preserved from the original code to avoid
rate-limit / timeout issues (IMPL.md Step 5, PLAN.md item 1).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from growattServer import OpenApiV1
from growattServer.exceptions import GrowattV1ApiError

logger = logging.getLogger(__name__)

# Growatt device type of the MIN/TLX hybrid inverters. `fetch_cycle` reads each
# device with `min_energy`, an endpoint only these devices serve, so this is a
# property of the fetch code, not a per-account setting. Other device types
# (dataloggers, ...) are dropped by `list_devices`.
GROWATT_MIN_DEVICE_TYPE = 7

# Type alias for clarity in callers.
GrowattPlant = dict[str, Any]
GrowattDevice = dict[str, Any]
GrowattEnergy = dict[str, Any]


async def list_plants(api_token: str) -> list[GrowattPlant]:
    """Return all plants visible to this token.

    Raises RuntimeError on SDK errors (network, auth, malformed response).
    """

    def _call() -> list[GrowattPlant]:
        api = OpenApiV1(api_token)
        try:
            resp = api.plant_list()
        except GrowattV1ApiError as e:
            raise RuntimeError(
                f"plant_list failed: code={e.error_code} msg={e.error_msg}"
            ) from e
        plants = resp.get("plants")
        if plants is None:
            raise RuntimeError(f"plant_list returned unexpected shape: {resp!r}")
        return plants

    return await asyncio.to_thread(_call)


async def list_devices(
    api_token: str,
    plant_id: str,
    *,
    device_type: int = GROWATT_MIN_DEVICE_TYPE,
) -> list[GrowattDevice]:
    """Return devices of `device_type` in `plant_id`.

    Filters server-side devices to only `type == device_type` (default
    `GROWATT_MIN_DEVICE_TYPE`).  Other device types (dataloggers, etc.) are
    dropped silently.

    Raises RuntimeError on SDK errors.
    """

    def _call() -> list[GrowattDevice]:
        api = OpenApiV1(api_token)
        try:
            resp = api.device_list(plant_id)
        except GrowattV1ApiError as e:
            raise RuntimeError(
                f"device_list failed: code={e.error_code} msg={e.error_msg}"
            ) from e
        devices = resp.get("devices")
        if devices is None:
            raise RuntimeError(f"device_list returned unexpected shape: {resp!r}")
        return [d for d in devices if d.get("type") == device_type]

    return await asyncio.to_thread(_call)


async def device_energy(
    api_token: str,
    device_sn: str,
) -> GrowattEnergy:
    """Return raw min_energy response for a single device.

    The SDK returns string values for pac, ppv, bdc1Soc etc. — callers must
    coerce (see transform/pipeline.py).

    Raises RuntimeError on SDK errors.
    """

    def _call() -> GrowattEnergy:
        api = OpenApiV1(api_token)
        try:
            resp = api.min_energy(device_sn=device_sn)
        except GrowattV1ApiError as e:
            raise RuntimeError(
                f"min_energy failed: code={e.error_code} msg={e.error_msg}"
            ) from e
        if not isinstance(resp, dict):
            raise RuntimeError(f"min_energy returned unexpected shape: {resp!r}")
        return resp

    return await asyncio.to_thread(_call)


async def fetch_cycle(
    api_token: str,
    *,
    device_type: int = GROWATT_MIN_DEVICE_TYPE,
    inter_device_sleep: float = 2.0,
    cached_plant_id: str | None = None,
    cached_devices: list[dict[str, Any]] | None = None,
) -> tuple[list[tuple[str, GrowattEnergy]], str, list[dict[str, Any]]]:
    """Run a full fetch cycle for one account.

    Returns (results, plant_id, devices) where:
      - results: list of (device_sn, energy_dict) tuples
      - plant_id: the plant ID used (for caching)
      - devices: the device list used (for caching)

    If cached_plant_id and cached_devices are provided, skips the plant_list
    and device_list API calls (rate-limit optimization).
    """
    if cached_plant_id and cached_devices:
        plant_id = cached_plant_id
        devices = cached_devices
        logger.debug("using cached plant_id=%s, %d devices", plant_id, len(devices))
    else:
        plants = await list_plants(api_token)
        if not plants:
            logger.warning("no Growatt plants found")
            return [], "", []
        plant_id = str(plants[0].get("plant_id", ""))
        devices = await list_devices(api_token, plant_id, device_type=device_type)

    results: list[tuple[str, GrowattEnergy]] = []
    for i, device in enumerate(devices):
        sn = str(device.get("device_sn", ""))
        if not sn:
            logger.warning("device without device_sn at index %d, skipping", i)
            continue

        if i > 0 and inter_device_sleep > 0:
            await asyncio.sleep(inter_device_sleep)

        energy = await device_energy(api_token, sn)
        results.append((sn, energy))

    return results, plant_id, devices
