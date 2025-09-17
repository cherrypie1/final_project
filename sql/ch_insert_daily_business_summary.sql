SELECT
    DATE(o.order_ts) AS event_date,
    SUM(oi.price_usd * oi.quantity) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.user_id) AS unique_customers,
    AVG(oi.price_usd * oi.quantity) AS avg_order_value,
    COUNT(CASE WHEN o.status = 'success' THEN 1 END)::float / NULLIF(COUNT(DISTINCT s.session_id), 0) AS conversion_rate,
    0 AS new_users,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    COUNT(CASE WHEN o.status = 'success' THEN 1 END) AS successful_orders
FROM dds.orders o
JOIN dds.order_items oi ON o.order_id = oi.order_id
LEFT JOIN dds.sessions s ON o.user_id::bigint = s.user_id::bigint
GROUP BY DATE(o.order_ts);