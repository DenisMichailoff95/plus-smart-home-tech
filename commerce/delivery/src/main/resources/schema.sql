CREATE SCHEMA IF NOT EXISTS delivery_schema;

CREATE TABLE IF NOT EXISTS delivery_schema.deliveries (
    delivery_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    from_country VARCHAR(100) NOT NULL,
    from_city VARCHAR(100) NOT NULL,
    from_street VARCHAR(200) NOT NULL,
    from_house VARCHAR(50) NOT NULL,
    from_flat VARCHAR(50) NOT NULL,
    to_country VARCHAR(100) NOT NULL,
    to_city VARCHAR(100) NOT NULL,
    to_street VARCHAR(200) NOT NULL,
    to_house VARCHAR(50) NOT NULL,
    to_flat VARCHAR(50) NOT NULL,
    delivery_state VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_deliveries_order_id ON delivery_schema.deliveries(order_id);