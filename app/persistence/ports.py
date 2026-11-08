from __future__ import annotations

from typing import Any, Protocol


class ProductRepository(Protocol):
    async def create(self, product: dict[str, Any]) -> dict[str, Any]: ...

    async def get(self, product_id: int) -> dict[str, Any] | None: ...

    async def list(self, *, include_inactive: bool = False) -> list[dict[str, Any]]: ...

    async def update(
        self, product_id: int, changes: dict[str, Any]
    ) -> dict[str, Any] | None: ...


class InventoryRepository(Protocol):
    async def get_balance(self, product_id: int) -> dict[str, Any] | None: ...

    async def list_movements(
        self, product_id: int, *, limit: int = 100
    ) -> list[dict[str, Any]]: ...

    async def reconcile(self, product_id: int) -> dict[str, int] | None: ...


class ReservationRepository(Protocol):
    async def get(self, reservation_id: str) -> dict[str, Any] | None: ...

    async def create(self, reservation: dict[str, Any]) -> dict[str, Any]: ...

    async def update_status(
        self, reservation_id: str, status: str
    ) -> dict[str, Any] | None: ...


class IdempotencyRepository(Protocol):
    async def get(
        self, scope: str, key: str
    ) -> dict[str, Any] | None: ...

    async def store(
        self,
        *,
        scope: str,
        key: str,
        request_hash: str,
        response: dict[str, Any],
    ) -> None: ...


class OrderRepository(Protocol):
    async def create(self, order: dict[str, Any]) -> dict[str, Any]: ...

    async def get(self, order_id: int) -> dict[str, Any] | None: ...

    async def update_outcome(
        self, order_id: int, outcome: dict[str, Any]
    ) -> dict[str, Any] | None: ...
