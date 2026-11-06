from __future__ import annotations

import asyncio

import pytest

from app.inventory.store import InventoryReservationRequest, InventoryStore


@pytest.mark.asyncio
async def test_reservation_is_idempotent_and_release_is_idempotent():
    store = InventoryStore()
    request = InventoryReservationRequest(
        order_id=123,
        saga_id="saga-123",
        quantity=5,
    )

    first = await store.reserve(7, request)
    repeated = await store.reserve(7, request)

    assert repeated == first
    assert await store.available_quantity(7) == 37

    released = await store.release(first["reservation_id"])
    repeated_release = await store.release(first["reservation_id"])

    assert released["status"] == "released"
    assert repeated_release["status"] == "released"
    assert await store.available_quantity(7) == 42

    with pytest.raises(ValueError, match="already consumed"):
        await store.reserve(7, request)


@pytest.mark.asyncio
async def test_inventory_store_rejects_oversubscription():
    store = InventoryStore()
    request = InventoryReservationRequest(
        order_id=123,
        saga_id="saga-too-large",
        quantity=43,
    )

    with pytest.raises(ValueError, match="Insufficient inventory"):
        await store.reserve(7, request)

    assert await store.available_quantity(7) == 42


@pytest.mark.asyncio
async def test_concurrent_reservations_never_oversell_inventory():
    store = InventoryStore()

    async def reserve(index: int):
        request = InventoryReservationRequest(
            order_id=index,
            saga_id=f"saga-{index}",
            quantity=5,
        )
        try:
            return await store.reserve(7, request)
        except ValueError:
            return None

    results = await asyncio.gather(*(reserve(index) for index in range(10)))

    assert sum(result is not None for result in results) == 8
    assert await store.available_quantity(7) == 2
