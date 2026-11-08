from __future__ import annotations


def normalize_product_fields(
    sku: str,
    name: str,
    category: str,
) -> tuple[str, str, str]:
    normalized = (sku.strip().upper(), name.strip(), category.strip())
    if not all(normalized):
        raise ValueError("Product fields cannot contain only whitespace")
    return normalized


def normalize_product_changes(changes: dict) -> dict:
    normalized = dict(changes)
    if any(value is None for value in normalized.values()):
        raise ValueError("Product fields cannot be null")

    if "sku" in normalized:
        normalized["sku"] = normalized["sku"].strip().upper()
        if not normalized["sku"]:
            raise ValueError("SKU cannot contain only whitespace")

    for field in ("name", "category"):
        if field in normalized:
            normalized[field] = normalized[field].strip()
            if not normalized[field]:
                raise ValueError(f"{field} cannot contain only whitespace")

    return normalized


def adjusted_on_hand(
    *,
    on_hand: int,
    reserved: int,
    quantity_delta: int,
) -> int:
    result = on_hand + quantity_delta
    if result < 0:
        raise ValueError("Adjustment cannot make on-hand stock negative")
    if result < reserved:
        raise ValueError("Adjustment cannot reduce on-hand below reserved quantity")
    return result


def validate_reservation(
    *,
    product_is_active: bool,
    quantity: int,
    available_quantity: int,
) -> None:
    if not product_is_active:
        raise ValueError("Product is inactive")
    if quantity > available_quantity:
        raise ValueError("Insufficient inventory")


def validate_commit_balance(
    *,
    on_hand: int,
    reserved: int,
    quantity: int,
) -> None:
    if on_hand < quantity or reserved < quantity:
        raise ValueError("Inventory balance is inconsistent")
