-- RAW
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.events (
    event_id SERIAL PRIMARY KEY,
    user_id TEXT,
    session_id TEXT,
    event_type TEXT,
    product_id INTEGER,
    product_name TEXT,
    product_category TEXT,
    order_id TEXT,
    quantity INTEGER,
    city TEXT,
    country TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    cost_usd NUMERIC(18,2),
    cost_eur NUMERIC(18,2),
    cost_rub NUMERIC(18,2),
    weather_temp NUMERIC(5,2),
    weather_desc TEXT,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.user_profiles (
    user_profile_id SERIAL PRIMARY KEY,
    user_id TEXT,
    name TEXT,
    email TEXT,
    birth_date DATE,
    gender TEXT,
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.user_devices (
    user_device_id SERIAL PRIMARY KEY,
    user_id TEXT,
    device_type TEXT,
    os TEXT,
    browser TEXT,
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.user_locations (
    user_location_id SERIAL PRIMARY KEY,
    user_id TEXT,
    city TEXT,
    country TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    raw_order_id SERIAL PRIMARY KEY,
    order_id TEXT,
    user_id TEXT,
    order_ts TIMESTAMP,
    total_usd NUMERIC(18,2),
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    raw_order_item_id SERIAL PRIMARY KEY,
    order_id TEXT,
    product_id INTEGER,
    quantity INTEGER,
    price_usd NUMERIC(18,2),
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.campaign_events (
    campaign_event_id SERIAL PRIMARY KEY,
    user_id TEXT,
    campaign_id TEXT,
    campaign_name TEXT,
    channel TEXT,
    action TEXT,
    cost NUMERIC(18,2),
    generated_at TIMESTAMP,
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_status_events (
    event_id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    status TEXT CHECK (status IN ('pending', 'success', 'failed', 'cancelled')),
    generated_at TIMESTAMP DEFAULT NOW()
);

--DDS
CREATE SCHEMA IF NOT EXISTS dds;


CREATE TABLE IF NOT EXISTS dds.users (
    user_id BIGINT PRIMARY KEY,
    name TEXT,
    email TEXT,
    birth_date DATE,
    gender TEXT CHECK (gender IN ('male','female')),
    city TEXT,
    country TEXT
);


CREATE TABLE IF NOT EXISTS dds.events (
    event_id     SERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES dds.users(user_id),
    event_type   TEXT NOT NULL,
    timestamp    TIMESTAMP NOT NULL,
    session_id   UUID,
    product_id   BIGINT,
    product_name TEXT,
    product_category TEXT,
    order_id     UUID,
    quantity     INT,
    cost_usd     NUMERIC,
    cost_eur     NUMERIC,
    cost_rub     NUMERIC,
    weather_temp NUMERIC,
    weather_desc TEXT,
    city         TEXT,
    country      TEXT,
    lat          NUMERIC,
    lon          NUMERIC
);


CREATE TABLE IF NOT EXISTS dds.orders (
    order_id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES dds.users(user_id),
    order_ts TIMESTAMP NOT NULL,
    total_usd NUMERIC(18,2) CHECK (total_usd >= 0),
    status TEXT CHECK (status IN ('pending', 'success', 'failed', 'cancelled')) DEFAULT 'pending'
);


CREATE TABLE IF NOT EXISTS dds.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES dds.orders(order_id),
    product_id INT NOT NULL,
    quantity INT CHECK (quantity > 0),
    price_usd NUMERIC(18,2) CHECK (price_usd >= 0)
);


CREATE TABLE IF NOT EXISTS dds.categories (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT UNIQUE
);


CREATE TABLE IF NOT EXISTS dds.products (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category_id INT REFERENCES dds.categories(category_id)
);


CREATE TABLE IF NOT EXISTS dds.sessions (
    session_id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES dds.users(user_id),
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    event_count INT
);


CREATE TABLE IF NOT EXISTS dds.user_devices (
    device_id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES dds.users(user_id),
    device_type TEXT,
    os TEXT,
    browser TEXT
);


CREATE TABLE IF NOT EXISTS dds.user_locations (
    location_id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES dds.users(user_id),
    city TEXT,
    country TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION
);


CREATE TABLE IF NOT EXISTS dds.campaigns (
    campaign_id TEXT PRIMARY KEY,
    campaign_name TEXT,
    channel TEXT
);

CREATE TABLE IF NOT EXISTS dds.campaign_events (
    campaign_event_id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES dds.users(user_id),
    campaign_id TEXT NOT NULL REFERENCES dds.campaigns(campaign_id),
    action TEXT,
    cost NUMERIC(18,2),
    generated_at TIMESTAMP
);