-- StreamForge - Initial Database Schema
-- Module 1: Base tables for local development

-- ── Extensions ──────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Enum types ───────────────────────────────────────────────────────────────
CREATE TYPE subscription_status AS ENUM (
    'active',
    'expired',
    'cancelled',
    'pending_renewal',
    'suspended'
);

CREATE TYPE event_type AS ENUM (
    'new_subscription',
    'renewal',
    'cancellation',
    'refund',
    'expiry',
    'extension'
);

CREATE TYPE transaction_status AS ENUM (
    'success',
    'failed',
    'refunded',
    'pending'
);

-- ── Customers ────────────────────────────────────────────────────────────────
CREATE TABLE customers (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255),
    country         VARCHAR(10) DEFAULT 'IN',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Subscriptions ────────────────────────────────────────────────────────────
CREATE TABLE subscriptions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id     UUID NOT NULL REFERENCES customers(id),
    plan            VARCHAR(50) NOT NULL,          -- 'basic', 'premium', 'enterprise'
    status          subscription_status NOT NULL DEFAULT 'active',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL,
    cancelled_at    TIMESTAMPTZ,
    amount_usd      NUMERIC(10, 2) NOT NULL,
    auto_renew      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Transactions ─────────────────────────────────────────────────────────────
CREATE TABLE transactions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    customer_id     UUID NOT NULL REFERENCES customers(id),
    event_type      event_type NOT NULL,
    status          transaction_status NOT NULL DEFAULT 'success',
    amount_usd      NUMERIC(10, 2),
    idempotency_key VARCHAR(255) UNIQUE,            -- prevents duplicate processing
    kafka_offset    BIGINT,
    kafka_partition INT,
    kafka_topic     VARCHAR(255),
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'
);

-- ── Lifecycle Event Log ───────────────────────────────────────────────────────
CREATE TABLE lifecycle_events (
    id              BIGSERIAL PRIMARY KEY,
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    customer_id     UUID NOT NULL REFERENCES customers(id),
    event_type      event_type NOT NULL,
    previous_status subscription_status,
    new_status      subscription_status,
    performed_by    VARCHAR(100) DEFAULT 'system',  -- 'system' | 'cs_agent:<id>'
    notes           TEXT,
    event_payload   JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── DLQ Tracking ─────────────────────────────────────────────────────────────
CREATE TABLE dlq_events (
    id              BIGSERIAL PRIMARY KEY,
    source_topic    VARCHAR(255) NOT NULL,
    event_payload   JSONB NOT NULL,
    error_reason    TEXT,
    retry_count     INT DEFAULT 0,
    resolved        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Indexes ──────────────────────────────────────────────────────────────────
CREATE INDEX idx_subscriptions_customer    ON subscriptions(customer_id);
CREATE INDEX idx_subscriptions_status      ON subscriptions(status);
CREATE INDEX idx_subscriptions_expires     ON subscriptions(expires_at);
CREATE INDEX idx_transactions_subscription ON transactions(subscription_id);
CREATE INDEX idx_transactions_idempotency  ON transactions(idempotency_key);
CREATE INDEX idx_lifecycle_subscription    ON lifecycle_events(subscription_id);
CREATE INDEX idx_lifecycle_created         ON lifecycle_events(created_at DESC);

-- ── Auto-update timestamps ───────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_subscriptions_updated_at
    BEFORE UPDATE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ── Seed Data (dev only) ─────────────────────────────────────────────────────
INSERT INTO customers (id, email, name, country) VALUES
    ('11111111-1111-1111-1111-111111111111', 'alice@example.com', 'Alice Kumar', 'IN'),
    ('22222222-2222-2222-2222-222222222222', 'bob@example.com',   'Bob Sharma',  'IN'),
    ('33333333-3333-3333-3333-333333333333', 'carol@example.com', 'Carol Singh', 'US');

INSERT INTO subscriptions (id, customer_id, plan, status, expires_at, amount_usd) VALUES
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', 'premium', 'active',    NOW() + INTERVAL '30 days', 9.99),
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '22222222-2222-2222-2222-222222222222', 'basic',   'active',    NOW() + INTERVAL '15 days', 4.99),
    ('cccccccc-cccc-cccc-cccc-cccccccccccc', '33333333-3333-3333-3333-333333333333', 'enterprise','active',  NOW() + INTERVAL '60 days', 29.99);

-- ── Views ────────────────────────────────────────────────────────────────────
CREATE VIEW v_subscription_summary AS
SELECT
    s.id,
    c.email,
    c.name,
    s.plan,
    s.status,
    s.expires_at,
    s.amount_usd,
    COUNT(le.id) AS event_count,
    MAX(le.created_at) AS last_event_at
FROM subscriptions s
JOIN customers c ON c.id = s.customer_id
LEFT JOIN lifecycle_events le ON le.subscription_id = s.id
GROUP BY s.id, c.email, c.name, s.plan, s.status, s.expires_at, s.amount_usd;
