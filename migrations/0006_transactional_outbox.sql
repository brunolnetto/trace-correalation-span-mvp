-- Transactional outbox for durable business events (issue #25).
-- Each producer trigger runs in the same PostgreSQL transaction as its domain
-- mutation. Outbox delivery is at-least-once; the Collector deduplicates event_id.

CREATE TABLE outbox_events (
    outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    event_name VARCHAR(256) NOT NULL,
    service VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(256) NOT NULL,
    correlation_id VARCHAR(128),
    event_payload JSONB NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'leased', 'delivered', 'blocked')
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner VARCHAR(128),
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    blocked_at TIMESTAMPTZ,
    CONSTRAINT outbox_lease_state_check CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    ),
    CONSTRAINT outbox_delivery_state_check CHECK (
        (status = 'delivered' AND delivered_at IS NOT NULL)
        OR
        (status <> 'delivered' AND delivered_at IS NULL)
    ),
    CONSTRAINT outbox_blocked_state_check CHECK (
        (status = 'blocked' AND blocked_at IS NOT NULL)
        OR
        (status <> 'blocked' AND blocked_at IS NULL)
    )
);

CREATE INDEX outbox_events_claim_idx
    ON outbox_events (available_at, created_at, outbox_id)
    WHERE status IN ('pending', 'leased');

CREATE INDEX outbox_events_blocked_idx
    ON outbox_events (created_at DESC, outbox_id)
    WHERE status = 'blocked';

CREATE OR REPLACE FUNCTION enqueue_outbox_event(
    p_service TEXT,
    p_event_name TEXT,
    p_aggregate_type TEXT,
    p_aggregate_id TEXT,
    p_attributes JSONB,
    p_correlation_id TEXT DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_id UUID := gen_random_uuid();
    v_occurred_at TIMESTAMPTZ := clock_timestamp();
    v_correlation_id TEXT := COALESCE(
        NULLIF(p_correlation_id, ''),
        NULLIF(current_setting('app.correlation_id', true), '')
    );
    v_trace_id TEXT := NULLIF(current_setting('app.trace_id', true), '');
    v_span_id TEXT := NULLIF(current_setting('app.span_id', true), '');
    v_attributes JSONB := COALESCE(p_attributes, '{}'::jsonb);
    v_source JSONB;
    v_event JSONB;
BEGIN
    v_source := jsonb_build_object(
        'timestamp', v_occurred_at,
        'level', 'INFO',
        'service', p_service,
        'logger', 'app.outbox',
        'message', p_event_name,
        'correlation_id', v_correlation_id,
        'trace_id', v_trace_id,
        'span_id', v_span_id,
        'attributes', v_attributes
    );
    v_event := jsonb_build_object(
        'schema_version', '1.0',
        'event_id', v_event_id::TEXT,
        'occurred_at', v_occurred_at,
        'service', p_service,
        'logger', 'app.outbox',
        'level', 'INFO',
        'event_name', p_event_name,
        'correlation_id', v_correlation_id,
        'trace_id', v_trace_id,
        'span_id', v_span_id,
        'attributes', v_attributes,
        'source', v_source
    );

    INSERT INTO outbox_events (
        event_id, event_name, service, aggregate_type, aggregate_id,
        correlation_id, event_payload
    )
    VALUES (
        v_event_id, p_event_name, p_service, p_aggregate_type, p_aggregate_id,
        v_correlation_id, v_event
    );
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION outbox_product_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_name TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_event_name := 'product.created';
    ELSE
        IF ROW(OLD.sku, OLD.name, OLD.category, OLD.unit_price, OLD.is_active)
           IS NOT DISTINCT FROM
           ROW(NEW.sku, NEW.name, NEW.category, NEW.unit_price, NEW.is_active) THEN
            RETURN NEW;
        END IF;
        v_event_name := 'product.updated';
    END IF;

    PERFORM enqueue_outbox_event(
        'inventory-api',
        v_event_name,
        'product',
        NEW.product_id::TEXT,
        jsonb_build_object(
            'product_id', NEW.product_id,
            'sku', NEW.sku,
            'name', NEW.name,
            'category', NEW.category,
            'unit_price', NEW.unit_price,
            'is_active', NEW.is_active
        )
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER products_outbox_insert
    AFTER INSERT ON products
    FOR EACH ROW EXECUTE FUNCTION outbox_product_change();

CREATE TRIGGER products_outbox_update
    AFTER UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION outbox_product_change();

CREATE OR REPLACE FUNCTION outbox_inventory_movement()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_name TEXT;
    v_correlation_id TEXT;
BEGIN
    v_event_name := CASE NEW.movement_type
        WHEN 'receipt' THEN 'inventory.stock.received'
        WHEN 'adjustment' THEN 'inventory.stock.adjusted'
        WHEN 'reservation_created' THEN 'inventory.reservation.created'
        WHEN 'reservation_released' THEN 'inventory.reservation.released'
        WHEN 'reservation_committed' THEN 'inventory.reservation.committed'
        ELSE NULL
    END;
    IF v_event_name IS NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.order_id IS NOT NULL THEN
        SELECT correlation_id
        INTO v_correlation_id
        FROM orders
        WHERE order_id = NEW.order_id;
    END IF;

    PERFORM enqueue_outbox_event(
        'inventory-api',
        v_event_name,
        'inventory_movement',
        NEW.movement_id::TEXT,
        jsonb_build_object(
            'movement_id', NEW.movement_id,
            'product_id', NEW.product_id,
            'movement_type', NEW.movement_type,
            'quantity_delta_on_hand', NEW.quantity_delta_on_hand,
            'quantity_delta_reserved', NEW.quantity_delta_reserved,
            'reference', NEW.reference,
            'idempotency_key', NEW.idempotency_key,
            'reservation_id', NEW.reservation_id,
            'order_id', NEW.order_id,
            'saga_id', NEW.saga_id
        ),
        v_correlation_id
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER inventory_movements_outbox_insert
    AFTER INSERT ON inventory_movements
    FOR EACH ROW EXECUTE FUNCTION outbox_inventory_movement();

CREATE OR REPLACE FUNCTION outbox_order_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_name TEXT;
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status IS NOT DISTINCT FROM NEW.status THEN
        RETURN NEW;
    END IF;

    v_event_name := CASE NEW.status
        WHEN 'pending' THEN 'order.process.started'
        WHEN 'processed' THEN 'order.process.completed'
        ELSE 'order.process.failed'
    END;

    PERFORM enqueue_outbox_event(
        'order-api',
        v_event_name,
        'order',
        NEW.order_id::TEXT,
        jsonb_build_object(
            'order_id', NEW.order_id,
            'requested_product_id', NEW.requested_product_id,
            'product_id', NEW.product_id,
            'quantity', NEW.quantity,
            'unit_price', NEW.unit_price,
            'total_amount', NEW.total_amount,
            'saga_id', NEW.saga_id,
            'reservation_id', NEW.reservation_id,
            'payment_id', NEW.payment_id,
            'status', NEW.status,
            'failure_category', NEW.failure_category
        ),
        NEW.correlation_id
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER orders_outbox_insert
    AFTER INSERT ON orders
    FOR EACH ROW EXECUTE FUNCTION outbox_order_change();

CREATE TRIGGER orders_outbox_update
    AFTER UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION outbox_order_change();

CREATE OR REPLACE FUNCTION outbox_supplier_onboarding()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_offering supplier_offerings%ROWTYPE;
BEGIN
    SELECT * INTO v_offering
    FROM supplier_offerings
    WHERE supplier_sku = NEW.supplier_sku;

    PERFORM enqueue_outbox_event(
        'supplier-simulator',
        'supplier.product.onboarded',
        'supplier_product',
        NEW.product_id::TEXT,
        jsonb_build_object(
            'supplier_sku', NEW.supplier_sku,
            'product_id', NEW.product_id,
            'product', NEW.product_snapshot,
            'reorder_point', v_offering.reorder_point,
            'target_stock', v_offering.target_stock,
            'lead_time_seconds', v_offering.lead_time_seconds
        )
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER supplier_onboarding_outbox_insert
    AFTER INSERT ON supplier_product_onboardings
    FOR EACH ROW EXECUTE FUNCTION outbox_supplier_onboarding();

CREATE OR REPLACE FUNCTION outbox_supplier_policy_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM enqueue_outbox_event(
        'supplier-simulator',
        'supplier.reorder_policy.updated',
        'supplier_reorder_policy',
        NEW.product_id::TEXT,
        jsonb_build_object(
            'product_id', NEW.product_id,
            'supplier_sku', NEW.supplier_sku,
            'reorder_point', NEW.reorder_point,
            'target_stock', NEW.target_stock,
            'lead_time_seconds', NEW.lead_time_seconds,
            'policy_version', NEW.version
        )
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER supplier_reorder_policy_outbox_insert
    AFTER INSERT ON supplier_reorder_policies
    FOR EACH ROW EXECUTE FUNCTION outbox_supplier_policy_change();

CREATE TRIGGER supplier_reorder_policy_outbox_update
    AFTER UPDATE ON supplier_reorder_policies
    FOR EACH ROW
    WHEN (
        OLD.reorder_point IS DISTINCT FROM NEW.reorder_point
        OR OLD.target_stock IS DISTINCT FROM NEW.target_stock
        OR OLD.lead_time_seconds IS DISTINCT FROM NEW.lead_time_seconds
        OR OLD.supplier_sku IS DISTINCT FROM NEW.supplier_sku
        OR OLD.version IS DISTINCT FROM NEW.version
    )
    EXECUTE FUNCTION outbox_supplier_policy_change();

CREATE OR REPLACE FUNCTION outbox_supplier_purchase_order_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_name TEXT;
    v_order supplier_purchase_orders%ROWTYPE;
BEGIN
    v_event_name := CASE NEW.to_status
        WHEN 'created' THEN 'supplier.purchase_order.created'
        WHEN 'ordered' THEN 'supplier.purchase_order.ordered'
        WHEN 'received' THEN 'supplier.purchase_order.received'
        WHEN 'cancelled' THEN 'supplier.purchase_order.cancelled'
        ELSE NULL
    END;
    IF v_event_name IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT * INTO v_order
    FROM supplier_purchase_orders
    WHERE purchase_order_id = NEW.purchase_order_id;

    PERFORM enqueue_outbox_event(
        'supplier-simulator',
        v_event_name,
        'supplier_purchase_order',
        NEW.purchase_order_id::TEXT,
        jsonb_build_object(
            'purchase_order_id', NEW.purchase_order_id,
            'product_id', v_order.product_id,
            'supplier_sku', v_order.supplier_sku,
            'from_status', NEW.from_status,
            'to_status', NEW.to_status,
            'status', v_order.status,
            'quantity', v_order.quantity,
            'receipt_id', v_order.receipt_id,
            'due_at', v_order.due_at,
            'ordered_at', v_order.ordered_at,
            'received_at', v_order.received_at,
            'details', NEW.details
        )
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER supplier_purchase_order_transition_outbox_insert
    AFTER INSERT ON supplier_purchase_order_transitions
    FOR EACH ROW EXECUTE FUNCTION outbox_supplier_purchase_order_transition();
