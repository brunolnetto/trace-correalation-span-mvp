-- Durable supplier catalog references, replenishment policies, purchase orders,
-- state transitions, and deterministic simulation clock (issue #23).

CREATE TABLE supplier_offerings (
    supplier_sku VARCHAR(64) PRIMARY KEY,
    sku VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(120) NOT NULL,
    category VARCHAR(80) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL CHECK (target_stock > reorder_point),
    lead_time_seconds BIGINT NOT NULL CHECK (lead_time_seconds >= 0),
    product_id BIGINT UNIQUE REFERENCES products(product_id) ON DELETE RESTRICT,
    onboarded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT supplier_offerings_sku_normalized CHECK (sku = upper(btrim(sku))),
    CONSTRAINT supplier_offerings_name_nonblank CHECK (length(btrim(name)) > 0),
    CONSTRAINT supplier_offerings_category_nonblank CHECK (length(btrim(category)) > 0),
    CONSTRAINT supplier_offerings_onboarding_pair CHECK (
        (product_id IS NULL AND onboarded_at IS NULL)
        OR (product_id IS NOT NULL AND onboarded_at IS NOT NULL)
    )
);

INSERT INTO supplier_offerings (
    supplier_sku, sku, name, category, unit_price,
    reorder_point, target_stock, lead_time_seconds
)
VALUES
    ('BEANS-250', 'COFFEE-BEANS-250G', 'Whole bean coffee 250g', 'grocery', 24.90, 12, 48, 3600),
    ('FILTER-100', 'COFFEE-FILTER-100', 'Paper coffee filters 100 pack', 'grocery', 12.50, 20, 80, 1800),
    ('MUG-350', 'CERAMIC-MUG-350', 'Ceramic coffee mug 350ml', 'kitchen', 32.00, 8, 32, 7200)
ON CONFLICT (supplier_sku) DO NOTHING;

CREATE TABLE supplier_reorder_policies (
    product_id BIGINT PRIMARY KEY REFERENCES products(product_id) ON DELETE RESTRICT,
    supplier_sku VARCHAR(64) REFERENCES supplier_offerings(supplier_sku) ON DELETE RESTRICT,
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL CHECK (target_stock > reorder_point),
    lead_time_seconds BIGINT NOT NULL CHECK (
        lead_time_seconds >= 0 AND lead_time_seconds <= 31536000
    ),
    version BIGINT NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE supplier_purchase_orders (
    purchase_order_id UUID PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    policy_version BIGINT NOT NULL CHECK (policy_version > 0),
    status VARCHAR(16) NOT NULL CHECK (
        status IN ('created', 'ordered', 'receiving', 'received', 'cancelled')
    ),
    reorder_point BIGINT NOT NULL CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL CHECK (target_stock > reorder_point),
    inventory_position_at_order BIGINT NOT NULL CHECK (inventory_position_at_order >= 0),
    on_hand_at_order BIGINT NOT NULL CHECK (on_hand_at_order >= 0),
    reserved_at_order BIGINT NOT NULL CHECK (reserved_at_order >= 0),
    on_order_at_order BIGINT NOT NULL CHECK (on_order_at_order >= 0),
    lead_time_seconds BIGINT NOT NULL CHECK (
        lead_time_seconds >= 0 AND lead_time_seconds <= 31536000
    ),
    created_at TIMESTAMPTZ NOT NULL,
    ordered_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    receiving_started_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    receipt_id VARCHAR(128),
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT supplier_purchase_orders_ordered_timestamp CHECK (
        status IN ('created', 'cancelled') OR ordered_at IS NOT NULL
    ),
    CONSTRAINT supplier_purchase_orders_receiving_timestamp CHECK (
        status NOT IN ('receiving', 'received') OR receiving_started_at IS NOT NULL
    ),
    CONSTRAINT supplier_purchase_orders_received_timestamp CHECK (
        (status = 'received' AND received_at IS NOT NULL AND receipt_id IS NOT NULL)
        OR (status <> 'received' AND received_at IS NULL AND receipt_id IS NULL)
    ),
    CONSTRAINT supplier_purchase_orders_cancelled_timestamp CHECK (
        (status = 'cancelled' AND cancelled_at IS NOT NULL)
        OR (status <> 'cancelled' AND cancelled_at IS NULL)
    )
);

-- One active PO per product/policy. The database is the final concurrency guard;
-- the adapter also locks the policy row before deciding whether to create a PO.
CREATE UNIQUE INDEX supplier_purchase_orders_one_active_per_product_idx
    ON supplier_purchase_orders (product_id)
    WHERE status IN ('created', 'ordered', 'receiving');

CREATE INDEX supplier_purchase_orders_product_created_idx
    ON supplier_purchase_orders (product_id, created_at DESC);

CREATE TABLE supplier_purchase_order_lines (
    purchase_order_id UUID NOT NULL
        REFERENCES supplier_purchase_orders(purchase_order_id) ON DELETE RESTRICT,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    supplier_sku VARCHAR(64) REFERENCES supplier_offerings(supplier_sku) ON DELETE RESTRICT,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    unit_cost NUMERIC(12, 2) CHECK (unit_cost IS NULL OR unit_cost > 0),
    PRIMARY KEY (purchase_order_id, line_number)
);

CREATE INDEX supplier_purchase_order_lines_product_idx
    ON supplier_purchase_order_lines (product_id);

CREATE TABLE supplier_purchase_order_transitions (
    transition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    purchase_order_id UUID NOT NULL
        REFERENCES supplier_purchase_orders(purchase_order_id) ON DELETE RESTRICT,
    from_status VARCHAR(16),
    to_status VARCHAR(16) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id VARCHAR(128),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT supplier_purchase_order_transition_statuses CHECK (
        (from_status IS NULL AND to_status = 'created')
        OR (from_status = 'created' AND to_status IN ('ordered', 'cancelled'))
        OR (from_status = 'ordered' AND to_status = 'receiving')
        OR (from_status = 'receiving' AND to_status = 'received')
    ),
    CONSTRAINT supplier_purchase_order_transition_once UNIQUE (
        purchase_order_id, to_status
    )
);

CREATE INDEX supplier_purchase_order_transitions_history_idx
    ON supplier_purchase_order_transitions (purchase_order_id, transition_id);

CREATE TABLE supplier_simulation_clock (
    clock_id SMALLINT PRIMARY KEY CHECK (clock_id = 1),
    current_time TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION reject_supplier_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER supplier_purchase_order_lines_no_update
    BEFORE UPDATE OR DELETE ON supplier_purchase_order_lines
    FOR EACH ROW EXECUTE FUNCTION reject_supplier_history_mutation();

CREATE TRIGGER supplier_purchase_order_lines_no_truncate
    BEFORE TRUNCATE ON supplier_purchase_order_lines
    FOR EACH STATEMENT EXECUTE FUNCTION reject_supplier_history_mutation();

CREATE TRIGGER supplier_purchase_order_transitions_no_update
    BEFORE UPDATE OR DELETE ON supplier_purchase_order_transitions
    FOR EACH ROW EXECUTE FUNCTION reject_supplier_history_mutation();

CREATE TRIGGER supplier_purchase_order_transitions_no_truncate
    BEFORE TRUNCATE ON supplier_purchase_order_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_supplier_history_mutation();
