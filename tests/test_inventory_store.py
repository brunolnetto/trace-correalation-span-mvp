from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from app.inventory.store import (
    InventoryAdjustmentRequest,
    InventoryReceiptRequest,
    InventoryReservationRequest,
    InventoryStore,
    ProductCreate,
    ProductUpdate,
)


async def create_stocked_product(
    store: InventoryStore, *, sku: str = "SKU-001", quantity: int = 42
) -> int:
    product = await store.create_product(
        ProductCreate(
            sku=sku,
            name="Test product",
            category="test",
            unit_price=Decimal("12.50"),
        )
    )
    product_id = product["product_id"]
    await store.receive_stock(
        product_id,
        InventoryReceiptRequest(
            quantity=quantity,
            idempotency_key=f"opening-{sku}",
            reference="test-opening-balance",
        ),
    )
    return product_id


@pytest.mark.asyncio
async def test_product_catalog_rejects_duplicate_skus_and_unknown_products():
    store = InventoryStore()
    product_id = await create_stocked_product(store)

    assert (await store.get_product(product_id))["sku"] == "SKU-001"
    with pytest.raises(ValueError, match="already exists"):
        await store.create_product(
            ProductCreate(
                sku="sku-001",
                name="Duplicate",
                category="test",
                unit_price=Decimal("1.00"),
            )
        )
    with pytest.raises(KeyError, match="does not exist"):
        await store.inventory_snapshot(999)


@pytest.mark.asyncio
async def test_product_can_be_deactivated_but_existing_stock_is_preserved():
    store = InventoryStore()
    product_id = await create_stocked_product(store)

    product = await store.update_product(
        product_id, ProductUpdate(is_active=False)
    )
    assert product["is_active"] is False
    assert await store.available_quantity(product_id) == 42

    with pytest.raises(ValueError, match="inactive"):
        await store.reserve(
            product_id,
            InventoryReservationRequest(
                order_id=1, saga_id="inactive-saga", quantity=1
            ),
        )


@pytest.mark.asyncio
async def test_receipt_is_idempotent_and_movement_ledger_is_append_only():
    store = InventoryStore()
    product_id = await create_stocked_product(store, quantity=5)
    request = InventoryReceiptRequest(
        quantity=10,
        idempotency_key="supplier-delivery-001",
        reference="PO-001",
    )

    first = await store.receive_stock(product_id, request)
    repeated = await store.receive_stock(product_id, request)

    assert first == repeated
    assert (await store.inventory_snapshot(product_id))["on_hand"] == 15
    assert len(await store.list_movements(product_id)) == 2

    with pytest.raises(ValueError, match="idempotency key was reused"):
        await store.receive_stock(
            product_id,
            InventoryReceiptRequest(
                quantity=11,
                idempotency_key="supplier-delivery-001",
                reference="PO-001",
            ),
        )


@pytest.mark.asyncio
async def test_reservation_release_and_commit_keep_balances_consistent():
    store = InventoryStore()
    product_id = await create_stocked_product(store)
    request = InventoryReservationRequest(
        order_id=123, saga_id="saga-123", quantity=5
    )

    reservation = await store.reserve(product_id, request)
    repeated = await store.reserve(product_id, request)
    assert repeated == reservation

    snapshot = await store.inventory_snapshot(product_id)
    assert (snapshot["on_hand"], snapshot["reserved"], snapshot["available"]) == (
        42,
        5,
        37,
    )

    released = await store.release(reservation["reservation_id"])
    assert released["status"] == "released"
    assert (await store.inventory_snapshot(product_id))["available"] == 42
    assert (await store.release(reservation["reservation_id"]))["status"] == "released"

    with pytest.raises(ValueError, match="already consumed"):
        await store.reserve(product_id, request)

    second = await store.reserve(
        product_id,
        InventoryReservationRequest(
            order_id=124, saga_id="saga-124", quantity=7
        ),
    )
    committed = await store.commit(second["reservation_id"])
    assert committed["status"] == "committed"
    assert (await store.commit(second["reservation_id"]))["status"] == "committed"

    snapshot = await store.inventory_snapshot(product_id)
    assert (snapshot["on_hand"], snapshot["reserved"], snapshot["available"]) == (
        35,
        0,
        35,
    )
    assert (await store.release(second["reservation_id"]))["status"] == "committed"


@pytest.mark.asyncio
async def test_adjustment_cannot_consume_reserved_stock_or_make_stock_negative():
    store = InventoryStore()
    product_id = await create_stocked_product(store, quantity=10)
    await store.reserve(
        product_id,
        InventoryReservationRequest(
            order_id=1, saga_id="saga-adjustment", quantity=6
        ),
    )

    with pytest.raises(ValueError, match="below reserved"):
        await store.adjust_stock(
            product_id,
            InventoryAdjustmentRequest(
                quantity_delta=-5,
                reason="damaged goods",
                idempotency_key="damaged-001",
            ),
        )

    with pytest.raises(ValueError, match="negative"):
        await store.adjust_stock(
            product_id,
            InventoryAdjustmentRequest(
                quantity_delta=-11,
                reason="stock correction",
                idempotency_key="correction-001",
            ),
        )

    assert (await store.inventory_snapshot(product_id))["on_hand"] == 10


@pytest.mark.asyncio
async def test_concurrent_reservations_never_oversell_inventory():
    store = InventoryStore()
    product_id = await create_stocked_product(store, quantity=42)

    async def reserve(index: int):
        request = InventoryReservationRequest(
            order_id=index,
            saga_id=f"saga-{index}",
            quantity=5,
        )
        try:
            return await store.reserve(product_id, request)
        except ValueError:
            return None

    results = await asyncio.gather(*(reserve(index) for index in range(10)))

    assert sum(result is not None for result in results) == 8
    snapshot = await store.inventory_snapshot(product_id)
    assert snapshot["on_hand"] == 42
    assert snapshot["reserved"] == 40
    assert snapshot["available"] == 2



@pytest.mark.asyncio
async def test_stock_adjustment_is_idempotent():
    store = InventoryStore()
    product_id = await create_stocked_product(store, quantity=10)
    request = InventoryAdjustmentRequest(
        quantity_delta=3,
        reason="count correction",
        idempotency_key="cycle-count-001",
    )

    first = await store.adjust_stock(product_id, request)
    repeated = await store.adjust_stock(product_id, request)

    assert repeated == first
    assert (await store.inventory_snapshot(product_id))["on_hand"] == 13

    with pytest.raises(ValueError, match="idempotency key was reused"):
        await store.adjust_stock(
            product_id,
            InventoryAdjustmentRequest(
                quantity_delta=4,
                reason="count correction",
                idempotency_key="cycle-count-001",
            ),
        )
