from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
from psycopg import AsyncConnection
from psycopg.types.json import Jsonb

from app.persistence.database import DatabaseManager
from app.supplier_simulator.store import ReorderPolicy


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _offering(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "supplier_sku": row["supplier_sku"],
        "sku": row["sku"],
        "name": row["name"],
        "category": row["category"],
        "unit_price": str(row["unit_price"]),
        "reorder_point": row["reorder_point"],
        "target_stock": row["target_stock"],
        "lead_time_seconds": row["lead_time_seconds"],
    }


def _policy(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "product_id": row["product_id"],
        "reorder_point": row["reorder_point"],
        "target_stock": row["target_stock"],
        "lead_time_seconds": row["lead_time_seconds"],
        "version": row["version"],
        "updated_at": _iso(row["updated_at"]),
    }


def _purchase_order(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "purchase_order_id": str(row["purchase_order_id"]),
        "product_id": row["product_id"],
        "supplier_sku": row["supplier_sku"],
        "quantity": row["quantity"],
        "unit_cost": (
            str(row["unit_cost"]) if row.get("unit_cost") is not None else None
        ),
        "status": row["status"],
        "reorder_point": row["reorder_point"],
        "target_stock": row["target_stock"],
        "inventory_position_at_order": row["inventory_position_at_order"],
        "on_hand_at_order": row["on_hand_at_order"],
        "reserved_at_order": row["reserved_at_order"],
        "on_order_at_order": row["on_order_at_order"],
        "lead_time_seconds": row["lead_time_seconds"],
        "policy_version": row["policy_version"],
        "created_at": _iso(row["created_at"]),
        "ordered_at": _iso(row["ordered_at"]),
        "due_at": _iso(row["due_at"]),
        "receiving_started_at": _iso(row["receiving_started_at"]),
        "received_at": _iso(row["received_at"]),
        "cancelled_at": _iso(row["cancelled_at"]),
        "receipt_id": row["receipt_id"],
    }


class PostgresSupplierSimulation:
    """Durable supplier lifecycle adapter.

    PostgreSQL owns offering/onboarding references, reorder policies, PO
    headers/lines, transition history, and the simulation clock. Inventory API
    remains the only authority allowed to mutate stock.

    External Inventory calls are never made while holding a database
    transaction. Receiving first commits the PO's `receiving` state, then
    calls Inventory with the stable `supplier-po:<purchase_order_id>`
    idempotency key, and finally commits `received`. A process crash or lost
    HTTP response leaves a retryable `receiving` PO; replaying the Inventory
    request cannot credit stock twice.
    """

    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    async def open(self) -> None:
        """Initialize the persisted simulation clock once, without resetting it."""
        async with self.database.connection() as connection:
            await connection.execute(
                """
                INSERT INTO supplier_simulation_clock (
                    clock_id, current_time, updated_at
                )
                VALUES (1, now(), now())
                ON CONFLICT (clock_id) DO NOTHING
                """
            )

    async def _clock_time(
        self, connection: AsyncConnection, *, for_update: bool = False
    ) -> datetime:
        suffix = " FOR UPDATE" if for_update else ""
        cursor = await connection.execute(
            f"""
            SELECT current_time
            FROM supplier_simulation_clock
            WHERE clock_id = 1{suffix}
            """
        )
        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("Supplier simulation clock has not been initialized")
        return row["current_time"]

    async def clock(self) -> dict[str, Any]:
        async with self.database.connection() as connection:
            now = await self._clock_time(connection)
            return {"now": now.isoformat()}

    async def advance_clock(self, seconds: int) -> dict[str, Any]:
        if seconds <= 0:
            raise ValueError("Clock advance must be positive")
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE supplier_simulation_clock
                SET current_time = current_time + %s,
                    updated_at = now()
                WHERE clock_id = 1
                RETURNING current_time
                """,
                (timedelta(seconds=seconds),),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Supplier simulation clock has not been initialized")
            return {
                "now": row["current_time"].isoformat(),
                "advanced_seconds": seconds,
            }

    async def list_offerings(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM supplier_offerings ORDER BY supplier_sku"
            )
            return [_offering(row) for row in await cursor.fetchall()]

    async def _find_inventory_product(
        self, client: httpx.AsyncClient, sku: str, correlation_id: str
    ) -> dict[str, Any] | None:
        response = await client.get(
            "/products", headers={"X-Correlation-ID": correlation_id}
        )
        response.raise_for_status()
        return next(
            (item for item in response.json() if item.get("sku") == sku),
            None,
        )

    async def _create_or_recover_product(
        self,
        offering: dict[str, Any],
        client: httpx.AsyncClient,
        correlation_id: str,
        *,
        existing_product_id: int | None = None,
    ) -> dict[str, Any]:
        headers = {"X-Correlation-ID": correlation_id}
        if existing_product_id is not None:
            response = await client.get(
                f"/products/{existing_product_id}", headers=headers
            )
            if response.status_code != 404:
                response.raise_for_status()
                return response.json()
            recovered = await self._find_inventory_product(
                client, offering["sku"], correlation_id
            )
            if recovered is None:
                raise RuntimeError(
                    "Persisted supplier onboarding points to a missing Inventory product"
                )
            return recovered

        response = await client.post(
            "/products",
            json={
                "sku": offering["sku"],
                "name": offering["name"],
                "category": offering["category"],
                "unit_price": offering["unit_price"],
            },
            headers=headers,
        )
        if response.status_code == 409:
            recovered = await self._find_inventory_product(
                client, offering["sku"], correlation_id
            )
            if recovered is None:
                response.raise_for_status()
                raise RuntimeError(
                    "Inventory rejected onboarding without a matching product SKU"
                )
            return recovered
        response.raise_for_status()
        return response.json()

    async def onboard_product(
        self,
        supplier_sku: str,
        client: httpx.AsyncClient,
        correlation_id: str,
    ) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM supplier_offerings WHERE supplier_sku = %s",
                (supplier_sku,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(f"Supplier SKU {supplier_sku} does not exist")
        offering = _offering(row)

        product = await self._create_or_recover_product(
            offering,
            client,
            correlation_id,
            existing_product_id=row["product_id"],
        )
        product_id = int(product["product_id"])

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM supplier_offerings
                WHERE supplier_sku = %s
                FOR UPDATE
                """,
                (supplier_sku,),
            )
            current = await cursor.fetchone()
            if current is None:
                raise KeyError(f"Supplier SKU {supplier_sku} does not exist")
            if (
                current["product_id"] is not None
                and current["product_id"] != product_id
            ):
                raise RuntimeError(
                    "Supplier offering is already linked to a different Inventory product"
                )

            now = await self._clock_time(connection)
            await connection.execute(
                """
                UPDATE supplier_offerings
                SET product_id = %s,
                    onboarded_at = COALESCE(onboarded_at, %s),
                    updated_at = now()
                WHERE supplier_sku = %s
                """,
                (product_id, now, supplier_sku),
            )
            await connection.execute(
                """
                INSERT INTO supplier_reorder_policies (
                    product_id, supplier_sku, reorder_point, target_stock,
                    lead_time_seconds, version, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (
                    product_id,
                    supplier_sku,
                    offering["reorder_point"],
                    offering["target_stock"],
                    offering["lead_time_seconds"],
                    now,
                    now,
                ),
            )
            cursor = await connection.execute(
                """
                SELECT *
                FROM supplier_reorder_policies
                WHERE product_id = %s
                """,
                (product_id,),
            )
            policy_row = await cursor.fetchone()
            cursor = await connection.execute(
                """
                SELECT onboarded_at
                FROM supplier_offerings
                WHERE supplier_sku = %s
                """,
                (supplier_sku,),
            )
            onboarded = await cursor.fetchone()

        return {
            "supplier_sku": supplier_sku,
            "product": product,
            "reorder_policy": _policy(policy_row),
            "onboarded_at": _iso(onboarded["onboarded_at"]),
        }

    async def set_policy(self, policy: ReorderPolicy) -> dict[str, Any]:
        if policy.target_stock <= policy.reorder_point:
            raise ValueError("target_stock must be greater than reorder_point")
        async with self.database.connection() as connection:
            now = await self._clock_time(connection)
            cursor = await connection.execute(
                """
                SELECT supplier_sku
                FROM supplier_offerings
                WHERE product_id = %s
                """,
                (policy.product_id,),
            )
            offering = await cursor.fetchone()
            supplier_sku = offering["supplier_sku"] if offering else None
            cursor = await connection.execute(
                """
                INSERT INTO supplier_reorder_policies (
                    product_id, supplier_sku, reorder_point, target_stock,
                    lead_time_seconds, version, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET supplier_sku = COALESCE(
                        EXCLUDED.supplier_sku,
                        supplier_reorder_policies.supplier_sku
                    ),
                    reorder_point = EXCLUDED.reorder_point,
                    target_stock = EXCLUDED.target_stock,
                    lead_time_seconds = EXCLUDED.lead_time_seconds,
                    version = supplier_reorder_policies.version + 1,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                (
                    policy.product_id,
                    supplier_sku,
                    policy.reorder_point,
                    policy.target_stock,
                    policy.lead_time_seconds,
                    now,
                    now,
                ),
            )
            return _policy(await cursor.fetchone())

    async def list_policies(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM supplier_reorder_policies
                ORDER BY product_id
                """
            )
            return [_policy(row) for row in await cursor.fetchall()]

    async def _load_purchase_order(
        self, connection: AsyncConnection, purchase_order_id: UUID
    ) -> dict[str, Any] | None:
        cursor = await connection.execute(
            """
            SELECT
                po.*,
                line.quantity,
                line.supplier_sku,
                line.unit_cost
            FROM supplier_purchase_orders AS po
            JOIN supplier_purchase_order_lines AS line
              ON line.purchase_order_id = po.purchase_order_id
             AND line.line_number = 1
            WHERE po.purchase_order_id = %s
            """,
            (purchase_order_id,),
        )
        row = await cursor.fetchone()
        return _purchase_order(row) if row is not None else None

    async def evaluate_replenishment(
        self,
        client: httpx.AsyncClient,
        correlation_id: str,
        *,
        product_id: int | None = None,
    ) -> list[dict[str, Any]]:
        policies = await self.list_policies()
        decisions: list[dict[str, Any]] = []
        for listed_policy in policies:
            current_product_id = listed_policy["product_id"]
            if product_id is not None and current_product_id != product_id:
                continue

            async with self.database.connection() as connection:
                cursor = await connection.execute(
                    """
                    SELECT inventory_revision
                    FROM supplier_reorder_policies
                    WHERE product_id = %s
                    """,
                    (current_product_id,),
                )
                revision_row = await cursor.fetchone()
                if revision_row is None:
                    continue
                inventory_revision = revision_row["inventory_revision"]

            inventory_response = await client.get(
                f"/inventory/{current_product_id}",
                headers={"X-Correlation-ID": correlation_id},
            )
            inventory_response.raise_for_status()
            inventory = inventory_response.json()
            on_hand = int(inventory["on_hand"])
            reserved = int(inventory["reserved"])
            available = on_hand - reserved

            async with self.database.connection() as connection:
                cursor = await connection.execute(
                    """
                    SELECT *
                    FROM supplier_reorder_policies
                    WHERE product_id = %s
                    FOR UPDATE
                    """,
                    (current_product_id,),
                )
                policy_row = await cursor.fetchone()
                if policy_row is None:
                    continue
                current_policy = _policy(policy_row)
                if policy_row["inventory_revision"] != inventory_revision:
                    # A receipt may have committed after the first Inventory
                    # read. Re-read while holding the policy lock so a stale
                    # pre-receipt balance cannot create a duplicate PO after
                    # the prior PO transitions out of the active state.
                    inventory_response = await client.get(
                        f"/inventory/{current_product_id}",
                        headers={"X-Correlation-ID": correlation_id},
                    )
                    inventory_response.raise_for_status()
                    inventory = inventory_response.json()
                    on_hand = int(inventory["on_hand"])
                    reserved = int(inventory["reserved"])
                    available = on_hand - reserved

                cursor = await connection.execute(
                    """
                    SELECT
                        po.*,
                        line.quantity,
                        line.supplier_sku,
                        line.unit_cost
                    FROM supplier_purchase_orders AS po
                    JOIN supplier_purchase_order_lines AS line
                      ON line.purchase_order_id = po.purchase_order_id
                     AND line.line_number = 1
                    WHERE po.product_id = %s
                      AND po.status IN ('created', 'ordered', 'receiving')
                    ORDER BY po.created_at, po.purchase_order_id
                    FOR UPDATE OF po
                    """,
                    (current_product_id,),
                )
                active_rows = await cursor.fetchall()
                active_orders = [_purchase_order(row) for row in active_rows]
                on_order = sum(order["quantity"] for order in active_orders)
                inventory_position = available + on_order
                active_ids = [order["purchase_order_id"] for order in active_orders]

                if active_orders:
                    decisions.append(
                        {
                            "product_id": current_product_id,
                            "decision": "open_purchase_order_exists",
                            "on_hand": on_hand,
                            "reserved": reserved,
                            "available_quantity": available,
                            "on_order": on_order,
                            "inventory_position": inventory_position,
                            "purchase_order_ids": active_ids,
                        }
                    )
                    continue

                if inventory_position > current_policy["reorder_point"]:
                    decisions.append(
                        {
                            "product_id": current_product_id,
                            "decision": "no_reorder",
                            "on_hand": on_hand,
                            "reserved": reserved,
                            "available_quantity": available,
                            "on_order": 0,
                            "inventory_position": inventory_position,
                            "purchase_order_ids": [],
                        }
                    )
                    continue

                quantity = current_policy["target_stock"] - inventory_position
                if quantity <= 0:
                    decisions.append(
                        {
                            "product_id": current_product_id,
                            "decision": "no_reorder",
                            "on_hand": on_hand,
                            "reserved": reserved,
                            "available_quantity": available,
                            "on_order": 0,
                            "inventory_position": inventory_position,
                            "purchase_order_ids": [],
                        }
                    )
                    continue

                cursor = await connection.execute(
                    """
                    SELECT supplier_sku, unit_price
                    FROM supplier_offerings
                    WHERE product_id = %s
                    """,
                    (current_product_id,),
                )
                offering = await cursor.fetchone()
                supplier_sku = offering["supplier_sku"] if offering else None
                unit_cost = offering["unit_price"] if offering else None
                now = await self._clock_time(connection)
                purchase_order_uuid = uuid4()

                await connection.execute(
                    """
                    INSERT INTO supplier_purchase_orders (
                        purchase_order_id, product_id, policy_version, status,
                        reorder_point, target_stock, inventory_position_at_order,
                        on_hand_at_order, reserved_at_order, on_order_at_order,
                        lead_time_seconds, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, 'created', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        purchase_order_uuid,
                        current_product_id,
                        current_policy["version"],
                        current_policy["reorder_point"],
                        current_policy["target_stock"],
                        inventory_position,
                        on_hand,
                        reserved,
                        0,
                        current_policy["lead_time_seconds"],
                        now,
                        now,
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO supplier_purchase_order_lines (
                        purchase_order_id, line_number, product_id,
                        supplier_sku, quantity, unit_cost
                    )
                    VALUES (%s, 1, %s, %s, %s, %s)
                    """,
                    (
                        purchase_order_uuid,
                        current_product_id,
                        supplier_sku,
                        quantity,
                        unit_cost,
                    ),
                )
                await self._insert_transition(
                    connection,
                    purchase_order_uuid,
                    None,
                    "created",
                    now,
                    correlation_id,
                    {
                        "policy_version": current_policy["version"],
                        "inventory_position": inventory_position,
                        "quantity": quantity,
                    },
                )
                purchase_order = await self._load_purchase_order(
                    connection, purchase_order_uuid
                )
                decisions.append(
                    {
                        "product_id": current_product_id,
                        "decision": "purchase_order_created",
                        "on_hand": on_hand,
                        "reserved": reserved,
                        "available_quantity": available,
                        "on_order": 0,
                        "inventory_position": inventory_position,
                        "purchase_order": purchase_order,
                    }
                )
        return decisions

    async def list_purchase_orders(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    po.*,
                    line.quantity,
                    line.supplier_sku,
                    line.unit_cost
                FROM supplier_purchase_orders AS po
                JOIN supplier_purchase_order_lines AS line
                  ON line.purchase_order_id = po.purchase_order_id
                 AND line.line_number = 1
                ORDER BY po.created_at DESC, po.purchase_order_id
                """
            )
            return [_purchase_order(row) for row in await cursor.fetchall()]

    async def get_purchase_order(self, purchase_order_id: str) -> dict[str, Any]:
        try:
            purchase_order_uuid = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            result = await self._load_purchase_order(
                connection, purchase_order_uuid
            )
            if result is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            return result

    async def _insert_transition(
        self,
        connection: AsyncConnection,
        purchase_order_id: UUID,
        from_status: str | None,
        to_status: str,
        occurred_at: datetime,
        correlation_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO supplier_purchase_order_transitions (
                purchase_order_id, from_status, to_status,
                occurred_at, correlation_id, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                purchase_order_id,
                from_status,
                to_status,
                occurred_at,
                correlation_id or None,
                Jsonb(metadata or {}),
            ),
        )

    async def list_purchase_order_transitions(
        self, purchase_order_id: str
    ) -> list[dict[str, Any]]:
        try:
            purchase_order_uuid = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    transition_id, purchase_order_id, from_status,
                    to_status, occurred_at, correlation_id, metadata
                FROM supplier_purchase_order_transitions
                WHERE purchase_order_id = %s
                ORDER BY transition_id
                """,
                (purchase_order_uuid,),
            )
            rows = await cursor.fetchall()
            if not rows:
                cursor = await connection.execute(
                    """
                    SELECT 1
                    FROM supplier_purchase_orders
                    WHERE purchase_order_id = %s
                    """,
                    (purchase_order_uuid,),
                )
                if await cursor.fetchone() is None:
                    raise KeyError(
                        f"Purchase order {purchase_order_id} does not exist"
                    )
            return [
                {
                    "transition_id": row["transition_id"],
                    "purchase_order_id": str(row["purchase_order_id"]),
                    "from_status": row["from_status"],
                    "to_status": row["to_status"],
                    "occurred_at": _iso(row["occurred_at"]),
                    "correlation_id": row["correlation_id"],
                    "metadata": row["metadata"],
                }
                for row in rows
            ]

    async def order_purchase_order(
        self, purchase_order_id: str, correlation_id: str = ""
    ) -> dict[str, Any]:
        try:
            purchase_order_uuid = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (purchase_order_uuid,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "ordered":
                result = await self._load_purchase_order(
                    connection, purchase_order_uuid
                )
                return result
            if order["status"] != "created":
                raise ValueError("Only created purchase orders can be ordered")

            now = await self._clock_time(connection)
            due_at = now + timedelta(seconds=order["lead_time_seconds"])
            await connection.execute(
                """
                UPDATE supplier_purchase_orders
                SET status = 'ordered',
                    ordered_at = %s,
                    due_at = %s,
                    updated_at = now()
                WHERE purchase_order_id = %s
                """,
                (now, due_at, purchase_order_uuid),
            )
            await self._insert_transition(
                connection,
                purchase_order_uuid,
                "created",
                "ordered",
                now,
                correlation_id,
                {"due_at": due_at.isoformat()},
            )
            return await self._load_purchase_order(
                connection, purchase_order_uuid
            )

    async def receive_purchase_order(
        self,
        purchase_order_id: str,
        client: httpx.AsyncClient,
        correlation_id: str,
    ) -> dict[str, Any]:
        try:
            purchase_order_uuid = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (purchase_order_uuid,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "received":
                return await self._load_purchase_order(
                    connection, purchase_order_uuid
                )
            if order["status"] == "ordered":
                now = await self._clock_time(connection)
                if order["due_at"] is None or now < order["due_at"]:
                    raise ValueError("Supplier lead time has not elapsed")
                await connection.execute(
                    """
                    UPDATE supplier_purchase_orders
                    SET status = 'receiving',
                        receiving_started_at = %s,
                        updated_at = now()
                    WHERE purchase_order_id = %s
                    """,
                    (now, purchase_order_uuid),
                )
                await self._insert_transition(
                    connection,
                    purchase_order_uuid,
                    "ordered",
                    "receiving",
                    now,
                    correlation_id,
                    {"idempotency_key": f"supplier-po:{purchase_order_id}"},
                )
            elif order["status"] != "receiving":
                raise ValueError("Only ordered or receiving purchase orders can be received")

            product_id = order["product_id"]
            quantity_cursor = await connection.execute(
                """
                SELECT quantity
                FROM supplier_purchase_order_lines
                WHERE purchase_order_id = %s AND line_number = 1
                """,
                (purchase_order_uuid,),
            )
            line = await quantity_cursor.fetchone()
            if line is None:
                raise RuntimeError("Purchase order has no persisted line")
            quantity = line["quantity"]

        response = await client.post(
            f"/inventory/{product_id}/receipts",
            json={
                "quantity": quantity,
                "idempotency_key": f"supplier-po:{purchase_order_id}",
                "reference": purchase_order_id,
            },
            headers={"X-Correlation-ID": correlation_id},
        )
        response.raise_for_status()
        receipt = response.json()
        receipt_id = str(receipt["receipt_id"])

        async with self.database.connection() as connection:
            # Evaluation locks policy -> PO. Receipt finalization uses the same
            # order to avoid a policy/PO deadlock under concurrent evaluation.
            cursor = await connection.execute(
                """
                SELECT product_id
                FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                """,
                (purchase_order_uuid,),
            )
            identity = await cursor.fetchone()
            if identity is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            cursor = await connection.execute(
                """
                SELECT product_id
                FROM supplier_reorder_policies
                WHERE product_id = %s
                FOR UPDATE
                """,
                (identity["product_id"],),
            )
            if await cursor.fetchone() is None:
                raise RuntimeError("Purchase order has no persisted reorder policy")
            cursor = await connection.execute(
                """
                SELECT status
                FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (purchase_order_uuid,),
            )
            current = await cursor.fetchone()
            if current is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if current["status"] == "received":
                return await self._load_purchase_order(
                    connection, purchase_order_uuid
                )
            if current["status"] != "receiving":
                raise RuntimeError(
                    "Purchase order left receiving state while Inventory receipt was in flight"
                )

            now = await self._clock_time(connection)
            await connection.execute(
                """
                UPDATE supplier_purchase_orders
                SET status = 'received',
                    received_at = %s,
                    receipt_id = %s,
                    updated_at = now()
                WHERE purchase_order_id = %s
                """,
                (now, receipt_id, purchase_order_uuid),
            )
            await connection.execute(
                """
                UPDATE supplier_reorder_policies
                SET inventory_revision = inventory_revision + 1,
                    updated_at = %s
                WHERE product_id = (
                    SELECT product_id
                    FROM supplier_purchase_orders
                    WHERE purchase_order_id = %s
                )
                """,
                (now, purchase_order_uuid),
            )
            await self._insert_transition(
                connection,
                purchase_order_uuid,
                "receiving",
                "received",
                now,
                correlation_id,
                {"receipt_id": receipt_id, "quantity": quantity},
            )
            return await self._load_purchase_order(
                connection, purchase_order_uuid
            )

    async def cancel_purchase_order(
        self, purchase_order_id: str, correlation_id: str = ""
    ) -> dict[str, Any]:
        try:
            purchase_order_uuid = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT status
                FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (purchase_order_uuid,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "cancelled":
                return await self._load_purchase_order(
                    connection, purchase_order_uuid
                )
            if order["status"] != "created":
                raise ValueError("Only created purchase orders can be cancelled")

            now = await self._clock_time(connection)
            await connection.execute(
                """
                UPDATE supplier_purchase_orders
                SET status = 'cancelled',
                    cancelled_at = %s,
                    updated_at = now()
                WHERE purchase_order_id = %s
                """,
                (now, purchase_order_uuid),
            )
            await self._insert_transition(
                connection,
                purchase_order_uuid,
                "created",
                "cancelled",
                now,
                correlation_id,
            )
            return await self._load_purchase_order(
                connection, purchase_order_uuid
            )
