-- Expand Order records to retain accepted and rejected business outcomes.
-- The requested product ID is retained even when the catalog lookup fails.
ALTER TABLE orders
    ALTER COLUMN product_id DROP NOT NULL,
    ALTER COLUMN saga_id DROP NOT NULL,
    ALTER COLUMN unit_price DROP NOT NULL,
    ALTER COLUMN total_amount DROP NOT NULL;

ALTER TABLE orders
    ADD COLUMN requested_product_id BIGINT NOT NULL,
    ADD COLUMN correlation_id VARCHAR(128);

ALTER TABLE orders
    DROP CONSTRAINT orders_status_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_status_check CHECK (
        status IN (
            'pending',
            'processed',
            'product_not_found',
            'product_inactive',
            'product_catalog_rejected',
            'product_catalog_unavailable',
            'payment_declined',
            'inventory_rejected',
            'inventory_service_error',
            'inventory_unavailable',
            'timeout',
            'processing_error',
            'compensation_incomplete',
            'failed'
        )
    );

CREATE INDEX orders_requested_product_created_idx
    ON orders (requested_product_id, created_at DESC);
