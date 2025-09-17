CREATE DATABASE IF NOT EXISTS data_mart;

USE data_mart;

-- 1. Ежедневная сводка по бизнесу
CREATE TABLE IF NOT EXISTS daily_business_summary
(
    event_date          Date,
    total_revenue       Float64,
    total_orders        UInt32,
    unique_customers    UInt32,
    avg_order_value     Float64,
    conversion_rate     Float64,
    new_users           UInt32,
    total_sessions      UInt32,
    successful_orders   UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- 2. Эффективность товаров и категорий
CREATE TABLE IF NOT EXISTS product_performance
(
    product_id          UInt32,
    product_name        String,
    category_name       String,
    total_quantity      UInt64,
    total_revenue       Float64,
    avg_price           Float64,
    orders_count        UInt32,
    unique_customers    UInt32,
    event_date          Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (category_name, product_id, event_date);

-- 3. Эффективность категорий
CREATE TABLE IF NOT EXISTS category_performance
(
    category_name       String,
    total_quantity      UInt64,
    total_revenue       Float64,
    orders_count        UInt32,
    unique_customers    UInt32,
    avg_order_value     Float64,
    event_date          Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (category_name, event_date);

-- 4. Анализ вовлечённости
CREATE TABLE IF NOT EXISTS user_engagement
(
    user_id             Int64,
    sessions_count      UInt32,
    total_events        UInt32,
    avg_session_duration_seconds Float64,
    avg_pages_per_session Float64,
    first_session_date  Date,
    last_session_date   Date,
    total_orders        UInt32,
    total_revenue       Float64,
    days_since_last_activity UInt32
)
ENGINE = MergeTree
ORDER BY user_id;

-- 5. Активные пользователи (7-дневное окно)
CREATE TABLE IF NOT EXISTS active_users_7d
(
    event_date          Date,
    active_users_count  UInt32,
    returning_users     UInt32,
    new_users           UInt32,
    churned_users       UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- 6. Сессионная аналитика
CREATE TABLE IF NOT EXISTS session_analytics
(
    session_id          UUID,
    user_id             Int64,
    start_time          DateTime,
    end_time            DateTime,
    duration_seconds    UInt32,
    events_count        UInt32,
    page_views_count    UInt32,
    orders_count        UInt32,
    conversion_flag     UInt8,
    device_type         String,
    browser             String,
    os                  String,
    city                String,
    country             String
)
ENGINE = MergeTree
ORDER BY (start_time, user_id);

-- 7. Маркетинговые кампании
CREATE TABLE IF NOT EXISTS campaign_performance
(
    campaign_id         String,
    campaign_name       String,
    start_date          Date,
    end_date            Date,
    total_sessions      UInt32,
    total_orders        UInt32,
    total_revenue       Float64,
    unique_users        UInt32,
    conversion_rate     Float64,
    avg_order_value     Float64,
    cost                Float64,
    roi                 Float64
)
ENGINE = MergeTree
ORDER BY (campaign_id, start_date);

-- 8. Географический анализ
CREATE TABLE IF NOT EXISTS geo_analysis
(
    city                String,
    country             String,
    total_users         UInt32,
    total_orders        UInt32,
    total_revenue       Float64,
    avg_order_value     Float64,
    conversion_rate     Float64,
    event_date          Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (country, city, event_date);

-- 9. Анализ устройств
CREATE TABLE IF NOT EXISTS device_analysis
(
    device_type         String,
    os                  String,
    browser             String,
    total_users         UInt32,
    total_sessions      UInt32,
    total_orders        UInt32,
    total_revenue       Float64,
    avg_session_duration Float64,
    conversion_rate     Float64,
    event_date          Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (device_type, os, browser, event_date);