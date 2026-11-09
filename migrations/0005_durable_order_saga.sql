-- Durable execution ledger for the Order Saga (issue #24).
-- The application owns execution checkpoints; Sagaz 1.5.0's declarative
-- storage path does not checkpoint individual steps or compensation progress.

CREATE TABLE order_saga_executions (
    saga_id VARCHAR(128) PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE REFERENCES orders(order_id),
    saga_name VARCHAR(128) NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version > 0),
    request_fingerprint CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (
        status IN (
            'running',
            'compensating',
            'completed',
            'rolled_back',
            'reconciliation_required'
        )
    ),
    context JSONB NOT NULL,
    failure_category VARCHAR(64),
    last_error TEXT,
    recovery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (recovery_attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT order_saga_execution_order_saga_unique
        UNIQUE (order_id, saga_id)
);

CREATE INDEX order_saga_executions_recovery_idx
    ON order_saga_executions (status, created_at)
    WHERE status IN ('running', 'compensating');

CREATE TABLE order_saga_steps (
    saga_id VARCHAR(128) NOT NULL
        REFERENCES order_saga_executions(saga_id) ON DELETE RESTRICT,
    step_name VARCHAR(64) NOT NULL,
    forward_status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (
        forward_status IN ('pending', 'running', 'completed', 'failed')
    ),
    compensation_status VARCHAR(16) NOT NULL DEFAULT 'not_required' CHECK (
        compensation_status IN (
            'not_required',
            'pending',
            'running',
            'completed',
            'failed'
        )
    ),
    result JSONB,
    forward_error TEXT,
    compensation_error TEXT,
    forward_attempts INTEGER NOT NULL DEFAULT 0 CHECK (forward_attempts >= 0),
    compensation_attempts INTEGER NOT NULL DEFAULT 0 CHECK (
        compensation_attempts >= 0
    ),
    forward_started_at TIMESTAMPTZ,
    forward_completed_at TIMESTAMPTZ,
    compensation_started_at TIMESTAMPTZ,
    compensation_completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, step_name)
);

CREATE INDEX order_saga_steps_forward_idx
    ON order_saga_steps (saga_id, forward_status);

CREATE TABLE order_payments (
    payment_id VARCHAR(128) PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    saga_id VARCHAR(128) NOT NULL UNIQUE
        REFERENCES order_saga_executions(saga_id) ON DELETE RESTRICT,
    amount NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    request_fingerprint CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (
        status IN ('declined', 'charged', 'refunded')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX order_payments_order_idx
    ON order_payments (order_id, created_at DESC);
