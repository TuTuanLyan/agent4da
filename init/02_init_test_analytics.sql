CREATE SCHEMA IF NOT EXISTS app_context;
CREATE SCHEMA IF NOT EXISTS analytics_test;

CREATE TABLE IF NOT EXISTS app_context.chat_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    session_name TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_context.ai_query_logs (
    log_id BIGSERIAL PRIMARY KEY,
    session_id TEXT,
    user_question TEXT,
    generated_sql TEXT,
    execution_status TEXT,
    execution_time_ms BIGINT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_test.test_sales (
    event_date DATE,
    brand TEXT,
    category TEXT,
    total_purchases INT,
    total_revenue NUMERIC(12,2)
);

TRUNCATE TABLE analytics_test.test_sales;

INSERT INTO analytics_test.test_sales VALUES
('2025-05-01', 'apple', 'smartphone', 120, 50000.00),
('2025-05-01', 'samsung', 'smartphone', 80, 30000.00),
('2025-05-02', 'apple', 'smartphone', 140, 60000.00),
('2025-05-02', 'xiaomi', 'smartphone', 70, 18000.00),
('2025-05-03', 'samsung', 'tv', 40, 22000.00),
('2025-05-03', 'lg', 'tv', 35, 20000.00);
