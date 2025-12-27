CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.warehouse_items (
    id UUID PRIMARY KEY,
    product_id UUID NOT NULL UNIQUE,
    quantity INTEGER NOT NULL,
    width DECIMAL(10, 2),
    height DECIMAL(10, 2),
    depth DECIMAL(10, 2),
    weight DECIMAL(10, 2),
    fragile BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.order_bookings (
    order_booking_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    delivery_id UUID,
    completed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.warehouse_address (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(200) NOT NULL,
    house VARCHAR(50) NOT NULL,
    flat VARCHAR(50) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_warehouse_items_product_id ON warehouse.warehouse_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_bookings_order_id ON warehouse.order_bookings(order_id);