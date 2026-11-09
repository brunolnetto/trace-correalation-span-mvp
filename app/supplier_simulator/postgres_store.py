from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
from psycopg.types.json import Jsonb

from app.persistence.database import DatabaseManager
from app.supplier_simulator.store import ReorderPolicy, SUPPLIER_CATALOG


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _policy(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "product_id": row["product_id"],
        "supplier_sku": row["supplier_sku"],
        "reorder_point": row["reorder_point"],
        "target_stock": row["target_stock"],
        "lead_time_seconds": row["lead_time_seconds"],
        "version": row["version"],
        "updated_at": _iso(row["updated_at"]),
    }


class PostgresSupplierSimulation:
    """Durable Supplier Simulator state; Inventory API remains stock authority.

    Supplier state and state transitions are committed in PostgreSQL. Inventory
    balances are only read through Inventory API and receipts are only applied
    through its idempotent receipt endpoint.
    """

    ACTIVE_STATUSES = ("created", "ordered", "receiving")

    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    async def initialize(self) -> None:
        """Seed immutable fixture offerings and the persisted simulation clock."""
        async with self.database.connection() as connection:
            await connection.execute(
                """
                INSERT INTO supplier_simulation_clock (singleton, current_time)
                VALUES (TRUE, now())
                ON CONFLICT (singleton) DO NOTHING
                """
            )
            for offering in SUPPLIER_CATALOG.values():
                await connection.execute(
                    """
                    INSERT INTO supplier_offerings (
                        supplier_sku, inventory_sku, name, category, unit_price,
                        reorder_point, target_stock, lead_time_seconds
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (supplier_sku) DO NOTHING
                    """,
                    (
                        offering["supplier_sku"],
                        offering["sku"],
                        offering["name"],
                        offering["category"],
                        Decimal(str(offering["unit_price"])),
                        offering["reorder_point"],
                        offering["target_stock"],
                        offering["lead_time_seconds"],
                    ),
                )

    async def clock(self) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT current_time
                FROM supplier_simulation_clock
                WHERE singleton = TRUE
                """
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Supplier simulation clock is not initialized")
            return {"now": _iso(row["current_time"])}

    async def advance_clock(self, seconds: int) -> dict[str, Any]:
        if seconds <= 0:
            raise ValueError("Clock advance must be positive")
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE supplier_simulation_clock
                SET current_time = current_time + (%s * interval '1 second')
                WHERE singleton = TRUE
                RETURNING current_time
                """,
                (seconds,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Supplier simulation clock is not initialized")
            return {"now": _iso(row["current_time"]), "advanced_seconds": seconds}

    async def list_offerings(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM supplier_offerings ORDER BY supplier_sku"
            )
            return [
                {
                    "supplier_sku": row["supplier_sku"],
                    "sku": row["inventory_sku"],
                    "name": row["name"],
                    "category": row["category"],
                    "unit_price": str(row["unit_price"]),
                    "reorder_point": row["reorder_point"],
                    "target_stock": row["target_stock"],
                    "lead_time_seconds": row["lead_time_seconds"],
                }
                for row in await cursor.fetchall()
            ]

    async def _get_offering(self, supplier_sku: str) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM supplier_offerings WHERE supplier_sku = %s",
                (supplier_sku,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(f"Supplier SKU {supplier_sku} does not exist")
            return row

    async def get_onboarding(self, supplier_sku: str) -> dict[str, Any] | None:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT o.supplier_sku, o.product_id, o.product_snapshot,
                       o.onboarded_at, p.reorder_point, p.target_stock,
                       p.lead_time_seconds, p.version, p.updated_at,
                       p.supplier_sku AS policy_supplier_sku
                FROM supplier_product_onboardings AS o
                JOIN supplier_reorder_policies AS p USING (product_id)
                WHERE o.supplier_sku = %s
                """,
                (supplier_sku,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return {
                "supplier_sku": row["supplier_sku"],
                "product": row["product_snapshot"],
                "reorder_policy": {
                    "product_id": row["product_id"],
                    "supplier_sku": row["policy_supplier_sku"],
                    "reorder_point": row["reorder_point"],
                    "target_stock": row["target_stock"],
                    "lead_time_seconds": row["lead_time_seconds"],
                    "version": row["version"],
                    "updated_at": _iso(row["updated_at"]),
                },
                "onboarded_at": _iso(row["onboarded_at"]),
            }

    async def save_onboarding(
        self, supplier_sku: str, product: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM supplier_offerings WHERE supplier_sku = %s",
                (supplier_sku,),
            )
            offering = await cursor.fetchone()
            if offering is None:
                raise KeyError(f"Supplier SKU {supplier_sku} does not exist")

            product_id = int(product["product_id"])
            await connection.execute(
                """
                INSERT INTO supplier_product_onboardings (
                    supplier_sku, product_id, product_snapshot
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (supplier_sku) DO NOTHING
                """,
                (supplier_sku, product_id, Jsonb(product)),
            )
            cursor = await connection.execute(
                """
                SELECT product_id, product_snapshot
                FROM supplier_product_onboardings
                WHERE supplier_sku = %s
                FOR UPDATE
                """,
                (supplier_sku,),
            )
            onboarding = await cursor.fetchone()
            if onboarding is None or onboarding["product_id"] != product_id:
                raise RuntimeError(
                    "Supplier onboarding conflicts with the persisted product reference"
                )

            await connection.execute(
                """
                INSERT INTO supplier_reorder_policies (
                    product_id, supplier_sku, reorder_point, target_stock,
                    lead_time_seconds, version
                )
                VALUES (%s, %s, %s, %s, %s, 1)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (
                    product_id,
                    supplier_sku,
                    offering["reorder_point"],
                    offering["target_stock"],
                    offering["lead_time_seconds"],
                ),
            )
            cursor = await connection.execute(
                """
                SELECT * FROM supplier_reorder_policies
                WHERE product_id = %s
                """,
                (product_id,),
            )
            policy_row = await cursor.fetchone()
            cursor = await connection.execute(
                """
                SELECT onboarded_at FROM supplier_product_onboardings
                WHERE supplier_sku = %s
                """,
                (supplier_sku,),
            )
            onboarded = await cursor.fetchone()
            return {
                "supplier_sku": supplier_sku,
                "product": onboarding["product_snapshot"],
                "reorder_policy": _policy(policy_row),
                "onboarded_at": _iso(onboarded["onboarded_at"]),
            }

    async def onboard_product(
        self,
        supplier_sku: str,
        client: httpx.AsyncClient,
        correlation_id: str,
    ) -> dict[str, Any]:
        offering = await self._get_offering(supplier_sku)
        existing = await self.get_onboarding(supplier_sku)
        if existing is not None:
            return existing

        product_payload = {
            "sku": offering["inventory_sku"],
            "name": offering["name"],
            "category": offering["category"],
            "unit_price": str(offering["unit_price"]),
        }
        response = await client.post(
            "/products",
            json=product_payload,
            headers={"X-Correlation-ID": correlation_id},
        )
        if response.status_code == 409:
            # Inventory may have committed product creation before Supplier
            # Simulator crashed. Recover the canonical product by its SKU.
            products_response = await client.get(
                "/products",
                params={"include_inactive": "true"},
                headers={"X-Correlation-ID": correlation_id},
            )
            products_response.raise_for_status()
            product = next(
                (
                    item
                    for item in products_response.json()
                    if item["sku"] == offering["inventory_sku"]
                ),
                None,
            )
            if product is None:
                response.raise_for_status()
                raise RuntimeError("Catalog rejected onboarding without a matching SKU")
        else:
            response.raise_for_status()
            product = response.json()
        return await self.save_onboarding(supplier_sku, product)

    async def set_policy(self, policy: ReorderPolicy) -> dict[str, Any]:
        if policy.target_stock <= policy.reorder_point:
            raise ValueError("target_stock must be greater than reorder_point")
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO supplier_reorder_policies (
                    product_id, reorder_point, target_stock,
                    lead_time_seconds, version
                )
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (product_id) DO UPDATE
                SET reorder_point = EXCLUDED.reorder_point,
                    target_stock = EXCLUDED.target_stock,
                    lead_time_seconds = EXCLUDED.lead_time_seconds,
                    version = supplier_reorder_policies.version + 1,
                    updated_at = now()
                RETURNING *
                """,
                (
                    policy.product_id,
                    policy.reorder_point,
                    policy.target_stock,
                    policy.lead_time_seconds,
                ),
            )
            return _policy(await cursor.fetchone())

    async def list_policies(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM supplier_reorder_policies ORDER BY product_id"
            )
            return [_policy(row) for row in await cursor.fetchall()]

    async def evaluate_replenishment(
        self,
        client: httpx.AsyncClient,
        correlation_id: str,
        *,
        product_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Serialize each Inventory snapshot with its persisted PO decision.

        The policy-row lock is held while the bounded Inventory API read is
        performed. Receipt completion takes the same lock before marking a PO
        received, preventing a stale pre-receipt snapshot from creating a new PO.
        """
        policies = await self.list_policies()
        if product_id is not None:
            policies = [
                policy for policy in policies if policy["product_id"] == product_id
            ]

        decisions: list[dict[str, Any]] = []
        for listed_policy in policies:
            current_product_id = listed_policy["product_id"]
            async with self.database.connection() as connection:
                cursor = await connection.execute(
                    """
                    SELECT * FROM supplier_reorder_policies
                    WHERE product_id = %s
                    FOR UPDATE
                    """,
                    (current_product_id,),
                )
                policy = await cursor.fetchone()
                if policy is None:
                    raise KeyError(
                        f"Reorder policy for product {current_product_id} does not exist"
                    )

                response = await client.get(
                    f"/inventory/{current_product_id}",
                    headers={"X-Correlation-ID": correlation_id},
                )
                response.raise_for_status()
                decisions.append(
                    await self._evaluate_product_in_transaction(
                        connection, policy, response.json()
                    )
                )
        return decisions

    async def _load_purchase_order(
        self, connection: Any, purchase_order_id: UUID
    ) -> dict[str, Any] | None:
        cursor = await connection.execute(
            """
            SELECT * FROM supplier_purchase_orders
            WHERE purchase_order_id = %s
            """,
            (purchase_order_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        cursor = await connection.execute(
            """
            SELECT line_number, product_id, supplier_sku,
                   inventory_sku_snapshot, description_snapshot,
                   quantity, unit_price
            FROM supplier_purchase_order_lines
            WHERE purchase_order_id = %s
            ORDER BY line_number
            """,
            (purchase_order_id,),
        )
        lines = [
            {
                "line_number": item["line_number"],
                "product_id": item["product_id"],
                "supplier_sku": item["supplier_sku"],
                "sku": item["inventory_sku_snapshot"],
                "description": item["description_snapshot"],
                "quantity": item["quantity"],
                "unit_price": (
                    str(item["unit_price"])
                    if item["unit_price"] is not None
                    else None
                ),
            }
            for item in await cursor.fetchall()
        ]
        return {
            "purchase_order_id": str(row["purchase_order_id"]),
            "product_id": row["product_id"],
            "supplier_sku": row["supplier_sku"],
            "quantity": row["quantity"],
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
            "lines": lines,
        }

    async def list_purchase_orders(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT purchase_order_id FROM supplier_purchase_orders
                ORDER BY created_at, purchase_order_id
                """
            )
            identifiers = [row["purchase_order_id"] for row in await cursor.fetchall()]
            result = []
            for identifier in identifiers:
                order = await self._load_purchase_order(connection, identifier)
                if order is not None:
                    result.append(order)
            return result

    async def get_purchase_order(self, purchase_order_id: str) -> dict[str, Any]:
        try:
            identifier = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            order = await self._load_purchase_order(connection, identifier)
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            return order

    async def list_purchase_order_transitions(
        self, purchase_order_id: str
    ) -> list[dict[str, Any]]:
        try:
            identifier = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT from_status, to_status, occurred_at, details
                FROM supplier_purchase_order_transitions
                WHERE purchase_order_id = %s
                ORDER BY transition_id
                """,
                (identifier,),
            )
            rows = await cursor.fetchall()
            if not rows:
                cursor = await connection.execute(
                    "SELECT 1 FROM supplier_purchase_orders WHERE purchase_order_id = %s",
                    (identifier,),
                )
                if await cursor.fetchone() is None:
                    raise KeyError(
                        f"Purchase order {purchase_order_id} does not exist"
                    )
            return [
                {
                    "from_status": row["from_status"],
                    "to_status": row["to_status"],
                    "occurred_at": _iso(row["occurred_at"]),
                    "details": row["details"],
                }
                for row in rows
            ]

    async def _active_orders(
        self, connection: Any, product_id: int
    ) -> list[dict[str, Any]]:
        cursor = await connection.execute(
            """
            SELECT * FROM supplier_purchase_orders
            WHERE product_id = %s
              AND status IN ('created', 'ordered', 'receiving')
            ORDER BY created_at, purchase_order_id
            """,
            (product_id,),
        )
        return await cursor.fetchall()

    async def evaluate_product(
        self, product_id: int, inventory: dict[str, Any]
    ) -> dict[str, Any]:
        """Serialize one product's replenishment decision in PostgreSQL."""
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM supplier_reorder_policies
                WHERE product_id = %s
                FOR UPDATE
                """,
                (product_id,),
            )
            policy = await cursor.fetchone()
            if policy is None:
                raise KeyError(f"Reorder policy for product {product_id} does not exist")
            return await self._evaluate_product_in_transaction(
                connection, policy, inventory
            )

    async def _evaluate_product_in_transaction(
        self,
        connection: Any,
        policy: dict[str, Any],
        inventory: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a PO or return the current decision inside a policy lock."""
        product_id = policy["product_id"]
        active_orders = await self._active_orders(connection, product_id)
        available = int(inventory["on_hand"]) - int(inventory["reserved"])
        on_order = sum(int(order["quantity"]) for order in active_orders)
        inventory_position = available + on_order
        active_order_ids = [str(order["purchase_order_id"]) for order in active_orders]

        common = {
            "product_id": product_id,
            "on_hand": inventory["on_hand"],
            "reserved": inventory["reserved"],
            "available_quantity": available,
            "on_order": on_order,
            "inventory_position": inventory_position,
        }
        if active_orders:
            return {
                **common,
                "decision": "open_purchase_order_exists",
                "purchase_order_ids": active_order_ids,
            }
        if inventory_position > policy["reorder_point"]:
            return {
                **common,
                "decision": "no_reorder",
                "purchase_order_ids": [],
            }

        quantity = int(policy["target_stock"]) - inventory_position
        if quantity <= 0:
            return {**common, "decision": "no_reorder", "purchase_order_ids": []}

        cursor = await connection.execute(
            """
            SELECT current_time FROM supplier_simulation_clock
            WHERE singleton = TRUE
            """
        )
        clock = await cursor.fetchone()
        if clock is None:
            raise RuntimeError("Supplier simulation clock is not initialized")

        purchase_order_id = uuid4()
        cursor = await connection.execute(
            """
            INSERT INTO supplier_purchase_orders (
                purchase_order_id, product_id, supplier_sku, policy_version,
                reorder_point, target_stock, inventory_position_at_order,
                on_hand_at_order, reserved_at_order, on_order_at_order,
                quantity, lead_time_seconds, status, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'created', %s)
            ON CONFLICT (product_id)
                WHERE status IN ('created', 'ordered', 'receiving')
            DO NOTHING
            RETURNING *
            """,
            (
                purchase_order_id,
                product_id,
                policy["supplier_sku"],
                policy["version"],
                policy["reorder_point"],
                policy["target_stock"],
                inventory_position,
                inventory["on_hand"],
                inventory["reserved"],
                on_order,
                quantity,
                policy["lead_time_seconds"],
                clock["current_time"],
            ),
        )
        created = await cursor.fetchone()
        if created is None:
            # The partial unique index is the final guard even if a future
            # caller bypasses the policy-row serialization convention.
            active_orders = await self._active_orders(connection, product_id)
            on_order = sum(int(order["quantity"]) for order in active_orders)
            return {
                **common,
                "decision": "open_purchase_order_exists",
                "on_order": on_order,
                "inventory_position": available + on_order,
                "purchase_order_ids": [
                    str(order["purchase_order_id"]) for order in active_orders
                ],
            }

        cursor = await connection.execute(
            """
            SELECT supplier_sku, inventory_sku, name, unit_price
            FROM supplier_offerings
            WHERE supplier_sku = %s
            """,
            (policy["supplier_sku"],),
        )
        offering = await cursor.fetchone()
        await connection.execute(
            """
            INSERT INTO supplier_purchase_order_lines (
                purchase_order_id, line_number, product_id, supplier_sku,
                inventory_sku_snapshot, description_snapshot, quantity, unit_price
            )
            VALUES (%s, 1, %s, %s, %s, %s, %s, %s)
            """,
            (
                purchase_order_id,
                product_id,
                offering["supplier_sku"] if offering else None,
                offering["inventory_sku"] if offering else None,
                offering["name"] if offering else None,
                quantity,
                offering["unit_price"] if offering else None,
            ),
        )
        await connection.execute(
            """
            INSERT INTO supplier_purchase_order_transitions (
                purchase_order_id, from_status, to_status, details
            )
            VALUES (%s, NULL, 'created', %s)
            """,
            (
                purchase_order_id,
                Jsonb({"quantity": quantity, "policy_version": policy["version"]}),
            ),
        )
        order = await self._load_purchase_order(connection, purchase_order_id)
        return {
            **common,
            "decision": "purchase_order_created",
            "purchase_order": order,
        }

    async def order_purchase_order(self, purchase_order_id: str) -> dict[str, Any]:
        try:
            identifier = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (identifier,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "ordered":
                result = await self._load_purchase_order(connection, identifier)
                return result
            if order["status"] != "created":
                raise ValueError("Only created purchase orders can be ordered")

            cursor = await connection.execute(
                """
                SELECT current_time FROM supplier_simulation_clock
                WHERE singleton = TRUE
                """
            )
            clock = await cursor.fetchone()
            if clock is None:
                raise RuntimeError("Supplier simulation clock is not initialized")
            ordered_at = clock["current_time"]
            due_at = ordered_at + timedelta(seconds=order["lead_time_seconds"])
            await connection.execute(
                """
                UPDATE supplier_purchase_orders
                SET status = 'ordered', ordered_at = %s, due_at = %s
                WHERE purchase_order_id = %s
                """,
                (ordered_at, due_at, identifier),
            )
            await connection.execute(
                """
                INSERT INTO supplier_purchase_order_transitions (
                    purchase_order_id, from_status, to_status, details
                )
                VALUES (%s, 'created', 'ordered', %s)
                """,
                (
                    identifier,
                    Jsonb({"due_at": due_at.isoformat()}),
                ),
            )
            return await self._load_purchase_order(connection, identifier)

    async def begin_receipt(self, purchase_order_id: str) -> dict[str, Any]:
        """Persist receiving before the Inventory API side effect is attempted."""
        try:
            identifier = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (identifier,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "received":
                return await self._load_purchase_order(connection, identifier)
            if order["status"] == "ordered":
                cursor = await connection.execute(
                    """
                    SELECT current_time FROM supplier_simulation_clock
                    WHERE singleton = TRUE
                    """
                )
                clock = await cursor.fetchone()
                if clock is None:
                    raise RuntimeError("Supplier simulation clock is not initialized")
                if order["due_at"] is None or clock["current_time"] < order["due_at"]:
                    raise ValueError("Supplier lead time has not elapsed")
                await connection.execute(
                    """
                    UPDATE supplier_purchase_orders
                    SET status = 'receiving', receiving_started_at = %s
                    WHERE purchase_order_id = %s
                    """,
                    (clock["current_time"], identifier),
                )
                await connection.execute(
                    """
                    INSERT INTO supplier_purchase_order_transitions (
                        purchase_order_id, from_status, to_status, details
                    )
                    VALUES (%s, 'ordered', 'receiving', '{}'::jsonb)
                    """,
                    (identifier,),
                )
            elif order["status"] != "receiving":
                raise ValueError("Only ordered or receiving purchase orders can be received")
            return await self._load_purchase_order(connection, identifier)

    async def complete_receipt(
        self, purchase_order_id: str, receipt_id: str
    ) -> dict[str, Any]:
        identifier = UUID(purchase_order_id)
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT product_id FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                """,
                (identifier,),
            )
            reference = await cursor.fetchone()
            if reference is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )

            # Match the policy -> PO lock order used by replenishment evaluation.
            # This prevents a stale pre-receipt Inventory snapshot from racing
            # with the PO leaving the active set.
            cursor = await connection.execute(
                """
                SELECT product_id FROM supplier_reorder_policies
                WHERE product_id = %s
                FOR UPDATE
                """,
                (reference["product_id"],),
            )
            if await cursor.fetchone() is None:
                raise RuntimeError(
                    "Purchase order has no persisted reorder policy"
                )

            cursor = await connection.execute(
                """
                SELECT status FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (identifier,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "received":
                return await self._load_purchase_order(connection, identifier)
            if order["status"] != "receiving":
                raise ValueError("Purchase order is not awaiting receipt completion")
            cursor = await connection.execute(
                """
                SELECT current_time FROM supplier_simulation_clock
                WHERE singleton = TRUE
                """
            )
            clock = await cursor.fetchone()
            if clock is None:
                raise RuntimeError("Supplier simulation clock is not initialized")
            await connection.execute(
                """
                UPDATE supplier_purchase_orders
                SET status = 'received', received_at = %s, receipt_id = %s
                WHERE purchase_order_id = %s
                """,
                (clock["current_time"], receipt_id, identifier),
            )
            await connection.execute(
                """
                INSERT INTO supplier_purchase_order_transitions (
                    purchase_order_id, from_status, to_status, details
                )
                VALUES (%s, 'receiving', 'received', %s)
                """,
                (identifier, Jsonb({"receipt_id": receipt_id})),
            )
            return await self._load_purchase_order(connection, identifier)

    async def receive_purchase_order(
        self,
        purchase_order_id: str,
        client: httpx.AsyncClient,
        correlation_id: str,
    ) -> dict[str, Any]:
        order = await self.begin_receipt(purchase_order_id)
        if order["status"] == "received":
            return order

        product_id = order["product_id"]
        response = await client.post(
            f"/inventory/{product_id}/receipts",
            json={
                "quantity": order["quantity"],
                "idempotency_key": f"supplier-po:{purchase_order_id}",
                "reference": purchase_order_id,
            },
            headers={"X-Correlation-ID": correlation_id},
        )
        response.raise_for_status()
        receipt = response.json()
        receipt_id = receipt.get("receipt_id")
        if not isinstance(receipt_id, str) or not receipt_id:
            raise RuntimeError("Inventory API receipt response has no receipt_id")
        return await self.complete_receipt(purchase_order_id, receipt_id)

    async def cancel_purchase_order(
        self, purchase_order_id: str
    ) -> dict[str, Any]:
        try:
            identifier = UUID(purchase_order_id)
        except ValueError as exc:
            raise KeyError(
                f"Purchase order {purchase_order_id} does not exist"
            ) from exc
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT status FROM supplier_purchase_orders
                WHERE purchase_order_id = %s
                FOR UPDATE
                """,
                (identifier,),
            )
            order = await cursor.fetchone()
            if order is None:
                raise KeyError(
                    f"Purchase order {purchase_order_id} does not exist"
                )
            if order["status"] == "cancelled":
                return await self._load_purchase_order(connection, identifier)
            if order["status"] != "created":
                raise ValueError("Only created purchase orders can be cancelled")
            cursor = await connection.execute(
                """
                SELECT current_time FROM supplier_simulation_clock
                WHERE singleton = TRUE
                """
            )
            clock = await cursor.fetchone()
            if clock is None:
                raise RuntimeError("Supplier simulation clock is not initialized")
            await connection.execute(
                """
                UPDATE supplier_purchase_orders
                SET status = 'cancelled', cancelled_at = %s
                WHERE purchase_order_id = %s
                """,
                (clock["current_time"], identifier),
            )
            await connection.execute(
                """
                INSERT INTO supplier_purchase_order_transitions (
                    purchase_order_id, from_status, to_status, details
                )
                VALUES (%s, 'created', 'cancelled', '{}'::jsonb)
                """,
                (identifier,),
            )
            return await self._load_purchase_order(connection, identifier)
