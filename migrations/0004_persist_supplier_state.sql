-- Persist Supplier Simulator state without crossing Inventory's ownership boundary.
CREATE TABLE supplier_offerings (
    supplier_sku VARCHAR(64) PRIMARY KEY,
    inventory_sku VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(120) NOT NULL,
    category VARCHAR(80) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL CHECK (target_stock > reorder_point),
    lead_time_seconds BIGINT NOT NULL CHECK (
        lead_time_seconds >= 0 AND lead_time_seconds <= 31536000
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE supplier_product_onboardings (
    supplier_sku VARCHAR(64) PRIMARY KEY
        REFERENCES supplier_offerings(supplier_sku) ON DELETE RESTRICT,
    product_id BIGINT NOT NULL UNIQUE
        REFERENCES products(product_id) ON DELETE RESTRICT,
    product_snapshot JSONB NOT NULL,
    onboarded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT supplier_onboardings_supplier_product_unique
        UNIQUE (supplier_sku, product_id)
);

CREATE TABLE supplier_reorder_policies (
    product_id BIGINT PRIMARY KEY
        REFERENCES products(product_id) ON DELETE RESTRICT,
    supplier_sku VARCHAR(64),
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL CHECK (target_stock > reorder_point),
    lead_time_seconds BIGINT NOT NULL CHECK (
        lead_time_seconds >= 0 AND lead_time_seconds <= 31536000
    ),
    version INTEGER NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT supplier_policy_onboarding_fk
        FOREIGN KEY (supplier_sku, product_id)
        REFERENCES supplier_product_onboardings(supplier_sku, product_id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE supplier_purchase_orders (
    purchase_order_id UUID PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    supplier_sku VARCHAR(64)
        REFERENCES supplier_offerings(supplier_sku) ON DELETE RESTRICT,
    policy_version INTEGER NOT NULL CHECK (policy_version > 0),
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL CHECK (target_stock > reorder_point),
    inventory_position_at_order BIGINT NOT NULL,
    on_hand_at_order BIGINT NOT NULL CHECK (on_hand_at_order >= 0),
    reserved_at_order BIGINT NOT NULL CHECK (reserved_at_order >= 0),
    on_order_at_order BIGINT NOT NULL CHECK (on_order_at_order >= 0),
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    lead_time_seconds BIGINT NOT NULL CHECK (
        lead_time_seconds >= 0 AND lead_time_seconds <= 31536000
    ),
    status VARCHAR(16) NOT NULL CHECK (
        status IN ('created', 'ordered', 'receiving', 'received', 'cancelled')
    ),
    created_at TIMESTAMPTZ NOT NULL,
    ordered_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    receiving_started_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    receipt_id VARCHAR(128),
    CONSTRAINT supplier_po_receipt_id_unique UNIQUE (receipt_id)
);

CREATE INDEX supplier_purchase_orders_product_created_idx
    ON supplier_purchase_orders (product_id, created_at DESC, purchase_order_id);

CREATE UNIQUE INDEX supplier_purchase_orders_one_active_per_product_idx
    ON supplier_purchase_orders (product_id)
    WHERE status IN ('created', 'ordered', 'receiving');

CREATE TABLE supplier_purchase_order_lines (
    purchase_order_id UUID NOT NULL
        REFERENCES supplier_purchase_orders(purchase_order_id) ON DELETE RESTRICT,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    supplier_sku VARCHAR(64)
        REFERENCES supplier_offerings(supplier_sku) ON DELETE RESTRICT,
    inventory_sku_snapshot VARCHAR(64),
    description_snapshot VARCHAR(120),
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) CHECK (unit_price IS NULL OR unit_price > 0),
    PRIMARY KEY (purchase_order_id, line_number)
);

CREATE TABLE supplier_purchase_order_transitions (
    transition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    purchase_order_id UUID NOT NULL
        REFERENCES supplier_purchase_orders(purchase_order_id) ON DELETE RESTRICT,
    from_status VARCHAR(16),
    to_status VARCHAR(16) NOT NULL CHECK (
        to_status IN ('created', 'ordered', 'receiving', 'received', 'cancelled')
    ),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT supplier_po_transition_from_status_check CHECK (
        from_status IS NULL OR from_status IN (
            'created', 'ordered', 'receiving', 'received', 'cancelled'
        )
    )
);

CREATE INDEX supplier_po_transitions_order_idx
    ON supplier_purchase_order_transitions (purchase_order_id, transition_id);

CREATE FUNCTION reject_supplier_po_transition_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'supplier_purchase_order_transitions is append-only';
END;
$$;

CREATE TRIGGER supplier_po_transitions_no_update
    BEFORE UPDATE OR DELETE ON supplier_purchase_order_transitions
    FOR EACH ROW EXECUTE FUNCTION reject_supplier_po_transition_mutation();

CREATE TABLE supplier_simulation_clock (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    current_time TIMESTAMPTZ NOT NULL
);
