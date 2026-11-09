from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

from app.persistence.database import DatabaseManager


def _order(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": row["order_id"],
        "requested_product_id": row["requested_product_id"],
        "product_id": row["product_id"],
        "quantity": row["quantity"],
        "unit_price": row["unit_price"],
        "total_amount": row["total_amount"],
        "saga_id": row["saga_id"],
        "reservation_id": (
            str(row["reservation_id"]) if row["reservation_id"] else None
        ),
        "payment_id": row["payment_id"],
        "status": row["status"],
        "failure_category": row["failure_category"],
        "correlation_id": row["correlation_id"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }


class PostgresOrderRepository:
    """Persist the latest durable business outcome for each order identifier."""

    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    async def save_outcome(self, record: dict[str, Any]) -> dict[str, Any]:
        unit_price = record.get("unit_price")
        total_amount = record.get("total_amount")
        reservation_id = record.get("reservation_id")
        if reservation_id is not None and not isinstance(reservation_id, UUID):
            reservation_id = UUID(str(reservation_id))
        if unit_price is not None and not isinstance(unit_price, Decimal):
            unit_price = Decimal(str(unit_price))
        if total_amount is not None and not isinstance(total_amount, Decimal):
            total_amount = Decimal(str(total_amount))

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO orders (
                    order_id,
                    requested_product_id,
                    product_id,
                    quantity,
                    unit_price,
                    total_amount,
                    saga_id,
                    reservation_id,
                    payment_id,
                    status,
                    failure_category,
                    correlation_id
                )
                VALUES (
                    %(order_id)s,
                    %(requested_product_id)s,
                    %(product_id)s,
                    %(quantity)s,
                    %(unit_price)s,
                    %(total_amount)s,
                    %(saga_id)s,
                    %(reservation_id)s,
                    %(payment_id)s,
                    %(status)s,
                    %(failure_category)s,
                    %(correlation_id)s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    requested_product_id = EXCLUDED.requested_product_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    total_amount = EXCLUDED.total_amount,
                    saga_id = EXCLUDED.saga_id,
                    reservation_id = EXCLUDED.reservation_id,
                    payment_id = EXCLUDED.payment_id,
                    status = EXCLUDED.status,
                    failure_category = EXCLUDED.failure_category,
                    correlation_id = EXCLUDED.correlation_id,
                    updated_at = now()
                WHERE orders.saga_id IS NOT DISTINCT FROM EXCLUDED.saga_id
                RETURNING *
                """,
                {
                    **record,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "reservation_id": reservation_id,
                },
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError(
                    f"Order {record['order_id']} is already owned by another Saga"
                )
            return _order(row)

    async def get(self, order_id: int) -> dict[str, Any] | None:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM orders WHERE order_id = %s",
                (order_id,),
            )
            row = await cursor.fetchone()
            return _order(row) if row is not None else None
