"""Tests for clients/growatt.py — Growatt API client.

All calls to growattServer.OpenApiV1 are mocked. No network, no real API.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from growattServer.exceptions import GrowattV1ApiError

from clients.growatt import (
    device_energy,
    fetch_cycle,
    list_devices,
    list_plants,
)


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _make_mock_api(
    *,
    plant_list_return: dict | None = None,
    device_list_return: dict | None = None,
    min_energy_return: dict | None = None,
) -> MagicMock:
    mock = MagicMock()
    if plant_list_return is not None:
        mock.plant_list.return_value = plant_list_return
    if device_list_return is not None:
        mock.device_list.return_value = device_list_return
    if min_energy_return is not None:
        mock.min_energy.return_value = min_energy_return
    return mock


SAMPLE_PLANTS = {"plants": [{"plant_id": "12345", "name": "Home"}]}

SAMPLE_DEVICES = {
    "devices": [
        {"device_sn": "SN001", "type": 7},
        {"device_sn": "SN002", "type": 3},   # datalogger — should be filtered
        {"device_sn": "SN003", "type": 7},
    ]
}

SAMPLE_ENERGY_SN001 = {"pac": "1500", "ppv": "3200", "bdc1Soc": "85"}
SAMPLE_ENERGY_SN003 = {"pac": "800", "ppv": "1100", "bdc1Soc": "60"}


# ---------------------------------------------------------------------------
# list_plants
# ---------------------------------------------------------------------------


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_plants_returns_plants(mock_cls):
    mock_cls.return_value = _make_mock_api(plant_list_return=SAMPLE_PLANTS)
    result = await list_plants("fake-token")
    assert result == [{"plant_id": "12345", "name": "Home"}]
    mock_cls.return_value.plant_list.assert_called_once()


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_plants_raises_on_bad_shape(mock_cls):
    mock_cls.return_value = _make_mock_api(plant_list_return={"error": "unauth"})
    with pytest.raises(RuntimeError, match="unexpected shape"):
        await list_plants("fake-token")


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_plants_surfaces_api_error(mock_cls):
    mock_cls.return_value.plant_list.side_effect = GrowattV1ApiError(
        "Error during getting plant list", error_code=10011, error_msg="No privilege access"
    )
    with pytest.raises(RuntimeError, match="code=10011 msg=No privilege access"):
        await list_plants("fake-token")


# ---------------------------------------------------------------------------
# list_devices
# ---------------------------------------------------------------------------


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_devices_filters_type7(mock_cls):
    mock_cls.return_value = _make_mock_api(device_list_return=SAMPLE_DEVICES)
    devices = await list_devices("fake-token", "12345")
    # Only type-7 devices survive the filter.
    assert len(devices) == 2
    assert {d["device_sn"] for d in devices} == {"SN001", "SN003"}


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_devices_custom_type(mock_cls):
    mock_cls.return_value = _make_mock_api(device_list_return=SAMPLE_DEVICES)
    devices = await list_devices("fake-token", "12345", device_type=3)
    assert len(devices) == 1
    assert devices[0]["device_sn"] == "SN002"


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_devices_raises_on_bad_shape(mock_cls):
    mock_cls.return_value = _make_mock_api(device_list_return={"plants": []})
    with pytest.raises(RuntimeError, match="unexpected shape"):
        await list_devices("fake-token", "12345")


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_list_devices_surfaces_api_error(mock_cls):
    mock_cls.return_value.device_list.side_effect = GrowattV1ApiError(
        "Error during getting device list", error_code=10012, error_msg="Rate limited"
    )
    with pytest.raises(RuntimeError, match="code=10012 msg=Rate limited"):
        await list_devices("fake-token", "12345")


# ---------------------------------------------------------------------------
# device_energy
# ---------------------------------------------------------------------------


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_device_energy_returns_raw_dict(mock_cls):
    mock_cls.return_value = _make_mock_api(min_energy_return=SAMPLE_ENERGY_SN001)
    result = await device_energy("fake-token", "SN001")
    assert result == SAMPLE_ENERGY_SN001
    mock_cls.return_value.min_energy.assert_called_once_with(device_sn="SN001")


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_device_energy_raises_on_bad_shape(mock_cls):
    mock_cls.return_value = _make_mock_api(min_energy_return="not-a-dict")
    with pytest.raises(RuntimeError, match="unexpected shape"):
        await device_energy("fake-token", "SN001")


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_device_energy_surfaces_api_error(mock_cls):
    mock_cls.return_value.min_energy.side_effect = GrowattV1ApiError(
        "Error during getting min energy", error_code=10001, error_msg="System error"
    )
    with pytest.raises(RuntimeError, match="code=10001 msg=System error"):
        await device_energy("fake-token", "SN001")


# ---------------------------------------------------------------------------
# fetch_cycle
# ---------------------------------------------------------------------------


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_fetch_cycle_returns_all_type7_devices(mock_cls):
    mock_api = _make_mock_api(
        plant_list_return=SAMPLE_PLANTS,
        device_list_return=SAMPLE_DEVICES,
    )
    # min_energy returns different data per call
    mock_api.min_energy.side_effect = [SAMPLE_ENERGY_SN001, SAMPLE_ENERGY_SN003]
    mock_cls.return_value = mock_api

    results, plant_id, devices = await fetch_cycle("fake-token", inter_device_sleep=0)

    assert len(results) == 2
    assert results[0] == ("SN001", SAMPLE_ENERGY_SN001)
    assert results[1] == ("SN003", SAMPLE_ENERGY_SN003)
    assert plant_id == "12345"
    assert len(devices) == 2


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_fetch_cycle_sleeps_between_devices(mock_cls):
    mock_api = _make_mock_api(
        plant_list_return=SAMPLE_PLANTS,
        device_list_return=SAMPLE_DEVICES,
    )
    mock_api.min_energy.side_effect = [SAMPLE_ENERGY_SN001, SAMPLE_ENERGY_SN003]
    mock_cls.return_value = mock_api

    with patch("clients.growatt.asyncio.sleep") as mock_sleep:
        await fetch_cycle("fake-token", inter_device_sleep=2.0)
        # Sleep called once between two devices (not before the first).
        mock_sleep.assert_called_once_with(2.0)


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_fetch_cycle_empty_plants(mock_cls):
    mock_cls.return_value = _make_mock_api(plant_list_return={"plants": []})
    results, plant_id, devices = await fetch_cycle("fake-token")
    assert results == []
    assert plant_id == ""
    assert devices == []


@patch("clients.growatt.OpenApiV1", autospec=False)
async def test_fetch_cycle_no_type7_devices(mock_cls):
    mock_devices = {
        "devices": [
            {"device_sn": "SN002", "type": 3},
            {"device_sn": "SN004", "type": 1},
        ]
    }
    mock_api = _make_mock_api(
        plant_list_return=SAMPLE_PLANTS,
        device_list_return=mock_devices,
    )
    mock_cls.return_value = mock_api

    results, plant_id, devices = await fetch_cycle("fake-token")
    assert results == []
    assert plant_id == "12345"
    assert len(devices) == 0
