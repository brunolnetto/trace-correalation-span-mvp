from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from app.inventory.rules import (
    adjusted_on_hand,
    normalize_product_changes,
    normalize_product_fields,
    validate_commit_balance,
    validate_reservation,
)
from app.inventory.store import (
    DEMO_PRODUCT_ID,
    DEMO_STOCK,
    InventoryAdjustmentRequest,
    InventoryReceiptRequest,
    InventoryReservationRequest,
    ProductCreate,
    ProductUpdate,
)
from app.persistence.database import DatabaseManager


def _iso(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _product(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "product_id": row["product_id"],
        "sku": row["sku"],
        "name": row["name"],
        "category": row["category"],
        "unit_price": row["unit_price"],
        "is_active": row["is_active"],
        "created_at": _iso(row["created_at"]),
        "updated_at": _iso(row["updated_at"]),
    }


def _movement(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "movement_id": str(row["movement_id"]),
        "product_id": row["product_id"],
        "movement_type": row["movement_type"],
        "quantity_delta_on_hand": row["quantity_delta_on_hand"],
        "quantity_delta_reserved": row["quantity_delta_reserved"],
        "reference": row["reference"],
        "idempotency_key": row["idempotency_key"],
        "occurred_at": _iso(row["occurred_at"]),
    }


def _reservation(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "reservation_id": str(row["reservation_id"]),
        "order_id": row["order_id"],
        "product_id": row["product_id"],
        "saga_id": row["saga_id"],
        "quantity": row["quantity"],
        "status": row["status"],
    }


def _request_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _get_idempotency(
    connection: AsyncConnection,
    *,
    scope: str,
    key: str,
    request_hash: str,
) -> dict[str, Any] | None:
    cursor = await connection.execute(
        """
        SELECT request_hash, response
        FROM idempotency_records
        WHERE scope = %s AND idempotency_key = %s
        """,
        (scope, key),
    )
    existing = await cursor.fetchone()
    if existing is None:
        return None
    if existing["request_hash"].strip() != request_hash:
        raise ValueError("Idempotency key was reused with a different request")
    return existing["response"]


async def _store_idempotency(
    connection: AsyncConnection,
    *,
    scope: str,
    key: str,
    request_hash: str,
    response: dict[str, Any],
) -> None:
    await connection.execute(
        """
        INSERT INTO idempotency_records
            (scope, idempotency_key, request_hash, response)
        VALUES (%s, %s, %s, %s)
        """,
        (scope, key, request_hash, Jsonb(response)),
    )


class PostgresInventoryStore:
    """PostgreSQL implementation of the InventoryStore application contract.

    Each public write is one database transaction. Row locks here protect the
    projection/journal pair from lost updates; #22 remains responsible for the
    explicit multi-process concurrency contract and its contention test suite.
    """

    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    async def create_product(self, request: ProductCreate) -> dict[str, Any]:
        sku, name, category = normalize_product_fields(
            request.sku,
            request.name,
            request.category,
        )

        try:
            async with self.database.connection() as connection:
                cursor = await connection.execute(
                    """
                    INSERT INTO products (sku, name, category, unit_price)
                    VALUES (%s, %s, %s, %s)
                    RETURNING *
                    """,
                    (sku, name, category, request.unit_price),
                )
                row = await cursor.fetchone()
                await connection.execute(
                    """
                    INSERT INTO inventory_balances (product_id, on_hand, reserved)
                    VALUES (%s, 0, 0)
                    """,
                    (row["product_id"],),
                )
                return _product(row)
        except UniqueViolation as exc:
            raise ValueError(f"SKU {sku} already exists") from exc

    async def list_products(
        self, *, include_inactive: bool = False
    ) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM products
                WHERE (%s OR is_active)
                ORDER BY product_id
                """,
                (include_inactive,),
            )
            return [_product(row) for row in await cursor.fetchall()]

    async def get_product(self, product_id: int) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM products WHERE product_id = %s",
                (product_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(f"Product {product_id} does not exist")
            return _product(row)

    async def update_product(
        self, product_id: int, request: ProductUpdate
    ) -> dict[str, Any]:
        changes = normalize_product_changes(
            request.model_dump(exclude_unset=True)
        )
        if not changes:
            return await self.get_product(product_id)

        allowed_fields = {"sku", "name", "category", "unit_price", "is_active"}
        if not set(changes).issubset(allowed_fields):
            raise ValueError("Unsupported product field")

        assignments = ", ".join(f"{field} = %({field})s" for field in changes)
        parameters = dict(changes)
        parameters["product_id"] = product_id
        try:
            async with self.database.connection() as connection:
                cursor = await connection.execute(
                    f"""
                    UPDATE products
                    SET {assignments}, updated_at = now()
                    WHERE product_id = %(product_id)s
                    RETURNING *
                    """,
                    parameters,
                )
                row = await cursor.fetchone()
                if row is None:
                    raise KeyError(f"Product {product_id} does not exist")
                return _product(row)
        except UniqueViolation as exc:
            raise ValueError(f"SKU {changes.get('sku')} already exists") from exc

    async def inventory_snapshot(self, product_id: int) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT b.on_hand, b.reserved
                FROM products AS p
                JOIN inventory_balances AS b USING (product_id)
                WHERE p.product_id = %s
                """,
                (product_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(f"Product {product_id} does not exist")
            available = row["on_hand"] - row["reserved"]
            return {
                "product_id": product_id,
                "on_hand": row["on_hand"],
                "reserved": row["reserved"],
                "available": available > 0,
                "available_quantity": available,
                "quantity": available,
            }

    async def available_quantity(self, product_id: int) -> int:
        snapshot = await self.inventory_snapshot(product_id)
        return snapshot["available_quantity"]

    async def receive_stock(
        self, product_id: int, request: InventoryReceiptRequest
    ) -> dict[str, Any]:
        scope = f"receipt:{product_id}"
        request_hash = _request_hash(
            {
                "product_id": product_id,
                "quantity": request.quantity,
                "reference": request.reference,
            }
        )
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT on_hand
                FROM inventory_balances
                WHERE product_id = %s
                FOR UPDATE
                """,
                (product_id,),
            )
            balance = await cursor.fetchone()
            if balance is None:
                raise KeyError(f"Product {product_id} does not exist")

            existing = await _get_idempotency(
                connection,
                scope=scope,
                key=request.idempotency_key,
                request_hash=request_hash,
            )
            if existing is not None:
                return existing

            movement_id = uuid4()
            await connection.execute(
                """
                UPDATE inventory_balances
                SET on_hand = on_hand + %s, updated_at = now()
                WHERE product_id = %s
                """,
                (request.quantity, product_id),
            )
            await connection.execute(
                """
                INSERT INTO inventory_movements (
                    movement_id, product_id, movement_type,
                    quantity_delta_on_hand, quantity_delta_reserved,
                    reference, idempotency_key
                )
                VALUES (%s, %s, 'receipt', %s, 0, %s, %s)
                """,
                (
                    movement_id,
                    product_id,
                    request.quantity,
                    request.reference,
                    request.idempotency_key,
                ),
            )
            response = {
                "receipt_id": str(movement_id),
                "product_id": product_id,
                "quantity": request.quantity,
                "reference": request.reference,
                "idempotency_key": request.idempotency_key,
                "status": "received",
            }
            await _store_idempotency(
                connection,
                scope=scope,
                key=request.idempotency_key,
                request_hash=request_hash,
                response=response,
            )
            return response

    async def adjust_stock(
        self, product_id: int, request: InventoryAdjustmentRequest
    ) -> dict[str, Any]:
        scope = f"adjustment:{product_id}"
        request_hash = _request_hash(
            {
                "product_id": product_id,
                "quantity_delta": request.quantity_delta,
                "reason": request.reason,
            }
        )
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT on_hand, reserved
                FROM inventory_balances
                WHERE product_id = %s
                FOR UPDATE
                """,
                (product_id,),
            )
            balance = await cursor.fetchone()
            if balance is None:
                raise KeyError(f"Product {product_id} does not exist")

            existing = await _get_idempotency(
                connection,
                scope=scope,
                key=request.idempotency_key,
                request_hash=request_hash,
            )
            if existing is not None:
                return existing

            adjusted = adjusted_on_hand(
                on_hand=balance["on_hand"],
                reserved=balance["reserved"],
                quantity_delta=request.quantity_delta,
            )

            movement_id = uuid4()
            await connection.execute(
                """
                UPDATE inventory_balances
                SET on_hand = %s, updated_at = now()
                WHERE product_id = %s
                """,
                (adjusted, product_id),
            )
            cursor = await connection.execute(
                """
                INSERT INTO inventory_movements (
                    movement_id, product_id, movement_type,
                    quantity_delta_on_hand, quantity_delta_reserved,
                    reference, idempotency_key
                )
                VALUES (%s, %s, 'adjustment', %s, 0, %s, %s)
                RETURNING *
                """,
                (
                    movement_id,
                    product_id,
                    request.quantity_delta,
                    request.reason,
                    request.idempotency_key,
                ),
            )
            response = _movement(await cursor.fetchone())
            await _store_idempotency(
                connection,
                scope=scope,
                key=request.idempotency_key,
                request_hash=request_hash,
                response=response,
            )
            return response

    async def reserve(
        self, product_id: int, request: InventoryReservationRequest
    ) -> dict[str, Any]:
        reservation_id = uuid5(
            NAMESPACE_URL,
            f"inventory-reservation:{request.saga_id}:{product_id}",
        )
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT is_active FROM products WHERE product_id = %s",
                (product_id,),
            )
            product = await cursor.fetchone()
            if product is None:
                raise KeyError(f"Product {product_id} does not exist")
            cursor = await connection.execute(
                """
                SELECT *
                FROM reservations
                WHERE reservation_id = %s
                FOR UPDATE
                """,
                (reservation_id,),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                same_request = (
                    existing["order_id"] == request.order_id
                    and existing["product_id"] == product_id
                    and existing["quantity"] == request.quantity
                )
                if not same_request or existing["status"] != "reserved":
                    raise ValueError("Reservation idempotency key is already consumed")
                return _reservation(existing)

            cursor = await connection.execute(
                """
                SELECT on_hand, reserved
                FROM inventory_balances
                WHERE product_id = %s
                FOR UPDATE
                """,
                (product_id,),
            )
            balance = await cursor.fetchone()
            if balance is None:
                raise KeyError(f"Inventory balance for product {product_id} is missing")
            available = balance["on_hand"] - balance["reserved"]
            validate_reservation(
                product_is_active=product["is_active"],
                quantity=request.quantity,
                available_quantity=available,
            )

            cursor = await connection.execute(
                """
                INSERT INTO reservations (
                    reservation_id, order_id, product_id, saga_id, quantity, status
                )
                VALUES (%s, %s, %s, %s, %s, 'reserved')
                RETURNING *
                """,
                (
                    reservation_id,
                    request.order_id,
                    product_id,
                    request.saga_id,
                    request.quantity,
                ),
            )
            row = await cursor.fetchone()
            await connection.execute(
                """
                UPDATE inventory_balances
                SET reserved = reserved + %s, updated_at = now()
                WHERE product_id = %s
                """,
                (request.quantity, product_id),
            )
            await connection.execute(
                """
                INSERT INTO inventory_movements (
                    movement_id, product_id, movement_type,
                    quantity_delta_on_hand, quantity_delta_reserved,
                    reference, reservation_id, order_id, saga_id
                )
                VALUES (%s, %s, 'reservation_created', 0, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(),
                    product_id,
                    request.quantity,
                    str(reservation_id),
                    reservation_id,
                    request.order_id,
                    request.saga_id,
                ),
            )
            return _reservation(row)

    async def release(self, reservation_id: str) -> dict[str, Any]:
        try:
            reservation_uuid = UUID(reservation_id)
        except ValueError:
            return {"reservation_id": reservation_id, "status": "not_found"}

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM reservations
                WHERE reservation_id = %s
                FOR UPDATE
                """,
                (reservation_uuid,),
            )
            row = await cursor.fetchone()
            if row is None:
                return {"reservation_id": reservation_id, "status": "not_found"}

            if row["status"] == "reserved":
                cursor = await connection.execute(
                    """
                    SELECT reserved
                    FROM inventory_balances
                    WHERE product_id = %s
                    FOR UPDATE
                    """,
                    (row["product_id"],),
                )
                balance = await cursor.fetchone()
                if balance is None or balance["reserved"] < row["quantity"]:
                    raise ValueError("Inventory balance is inconsistent")

                await connection.execute(
                    """
                    UPDATE inventory_balances
                    SET reserved = reserved - %s, updated_at = now()
                    WHERE product_id = %s
                    """,
                    (row["quantity"], row["product_id"]),
                )
                await connection.execute(
                    """
                    UPDATE reservations
                    SET status = 'released', updated_at = now()
                    WHERE reservation_id = %s
                    """,
                    (reservation_uuid,),
                )
                await connection.execute(
                    """
                    INSERT INTO inventory_movements (
                        movement_id, product_id, movement_type,
                        quantity_delta_on_hand, quantity_delta_reserved,
                        reference, reservation_id, order_id, saga_id
                    )
                    VALUES (%s, %s, 'reservation_released', 0, %s, %s, %s, %s, %s)
                    """,
                    (
                        uuid4(),
                        row["product_id"],
                        -row["quantity"],
                        str(reservation_uuid),
                        reservation_uuid,
                        row["order_id"],
                        row["saga_id"],
                    ),
                )
                row["status"] = "released"

            return _reservation(row)

    async def commit(self, reservation_id: str) -> dict[str, Any]:
        try:
            reservation_uuid = UUID(reservation_id)
        except ValueError as exc:
            raise KeyError(f"Reservation {reservation_id} does not exist") from exc

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM reservations
                WHERE reservation_id = %s
                FOR UPDATE
                """,
                (reservation_uuid,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(f"Reservation {reservation_id} does not exist")
            if row["status"] == "committed":
                return _reservation(row)
            if row["status"] != "reserved":
                raise ValueError("Only active reservations can be committed")

            cursor = await connection.execute(
                """
                SELECT on_hand, reserved
                FROM inventory_balances
                WHERE product_id = %s
                FOR UPDATE
                """,
                (row["product_id"],),
            )
            balance = await cursor.fetchone()
            quantity = row["quantity"]
            if balance is None:
                raise ValueError("Inventory balance is inconsistent")
            validate_commit_balance(
                on_hand=balance["on_hand"],
                reserved=balance["reserved"],
                quantity=quantity,
            )

            await connection.execute(
                """
                UPDATE inventory_balances
                SET on_hand = on_hand - %s,
                    reserved = reserved - %s,
                    updated_at = now()
                WHERE product_id = %s
                """,
                (quantity, quantity, row["product_id"]),
            )
            await connection.execute(
                """
                UPDATE reservations
                SET status = 'committed', updated_at = now()
                WHERE reservation_id = %s
                """,
                (reservation_uuid,),
            )
            await connection.execute(
                """
                INSERT INTO inventory_movements (
                    movement_id, product_id, movement_type,
                    quantity_delta_on_hand, quantity_delta_reserved,
                    reference, reservation_id, order_id, saga_id
                )
                VALUES (%s, %s, 'reservation_committed', %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(),
                    row["product_id"],
                    -quantity,
                    -quantity,
                    str(reservation_uuid),
                    reservation_uuid,
                    row["order_id"],
                    row["saga_id"],
                ),
            )
            row["status"] = "committed"
            return _reservation(row)

    async def list_movements(
        self, product_id: int, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM products WHERE product_id = %s",
                (product_id,),
            )
            if await cursor.fetchone() is None:
                raise KeyError(f"Product {product_id} does not exist")

            cursor = await connection.execute(
                """
                SELECT *
                FROM inventory_movements
                WHERE product_id = %s
                ORDER BY occurred_at DESC, movement_id DESC
                LIMIT %s
                """,
                (product_id, limit),
            )
            return [_movement(row) for row in await cursor.fetchall()]

    async def reconcile(self, product_id: int) -> dict[str, int] | None:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    b.on_hand,
                    b.reserved,
                    COALESCE(sum(m.quantity_delta_on_hand), 0)::BIGINT
                        AS journal_on_hand,
                    COALESCE(sum(m.quantity_delta_reserved), 0)::BIGINT
                        AS journal_reserved
                FROM inventory_balances AS b
                LEFT JOIN inventory_movements AS m USING (product_id)
                WHERE b.product_id = %s
                GROUP BY b.product_id, b.on_hand, b.reserved
                """,
                (product_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return {
                "on_hand": row["on_hand"],
                "reserved": row["reserved"],
                "journal_on_hand": row["journal_on_hand"],
                "journal_reserved": row["journal_reserved"],
            }

    async def seed_demo_data(self) -> None:
        """Insert the deterministic demo product only when the DB is truly empty."""
        async with self.database.connection() as connection:
            await connection.execute(
                """
                INSERT INTO products (
                    product_id, sku, name, category, unit_price, is_active
                )
                VALUES (%s, 'DEMO-007', 'Demo product', 'demo', %s, TRUE)
                ON CONFLICT DO NOTHING
                """,
                (DEMO_PRODUCT_ID, Decimal("99.90")),
            )
            cursor = await connection.execute(
                "SELECT * FROM products WHERE product_id = %s FOR UPDATE",
                (DEMO_PRODUCT_ID,),
            )
            product = await cursor.fetchone()
            if product is None or product["sku"] != "DEMO-007":
                raise RuntimeError(
                    "Demo bootstrap cannot claim product ID 7; refusing to overwrite data"
                )

            await connection.execute(
                """
                INSERT INTO inventory_balances (product_id, on_hand, reserved)
                VALUES (%s, 0, 0)
                ON CONFLICT DO NOTHING
                """,
                (DEMO_PRODUCT_ID,),
            )
            cursor = await connection.execute(
                """
                SELECT on_hand, reserved
                FROM inventory_balances
                WHERE product_id = %s
                FOR UPDATE
                """,
                (DEMO_PRODUCT_ID,),
            )
            balance = await cursor.fetchone()
            cursor = await connection.execute(
                """
                SELECT
                    count(*) AS movement_count,
                    COALESCE(sum(quantity_delta_on_hand), 0)::BIGINT AS journal_on_hand,
                    COALESCE(sum(quantity_delta_reserved), 0)::BIGINT AS journal_reserved
                FROM inventory_movements
                WHERE product_id = %s
                """,
                (DEMO_PRODUCT_ID,),
            )
            journal = await cursor.fetchone()

            if journal["movement_count"] == 0:
                if balance["on_hand"] != 0 or balance["reserved"] != 0:
                    raise RuntimeError(
                        "Demo inventory has a balance without a movement journal"
                    )
                await connection.execute(
                    """
                    UPDATE inventory_balances
                    SET on_hand = %s, updated_at = now()
                    WHERE product_id = %s
                    """,
                    (DEMO_STOCK, DEMO_PRODUCT_ID),
                )
                await connection.execute(
                    """
                    INSERT INTO inventory_movements (
                        movement_id, product_id, movement_type,
                        quantity_delta_on_hand, quantity_delta_reserved, reference
                    )
                    VALUES (%s, %s, 'opening_balance', %s, 0, 'demo-seed')
                    """,
                    (uuid4(), DEMO_PRODUCT_ID, DEMO_STOCK),
                )
            elif (
                journal["journal_on_hand"] != balance["on_hand"]
                or journal["journal_reserved"] != balance["reserved"]
            ):
                raise RuntimeError(
                    "Demo inventory balance does not reconcile with its movement journal"
                )

            await connection.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('products', 'product_id'),
                    GREATEST((SELECT max(product_id) FROM products), 1),
                    TRUE
                )
                """
            )
