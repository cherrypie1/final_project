SELECT
    COALESCE(l.city, u.city) AS city,
    COALESCE(l.country, u.country) AS country,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(oi.price_usd * oi.quantity), 0) AS total_revenue,
    CASE
        WHEN COUNT(DISTINCT o.order_id) > 0
        THEN COALESCE(SUM(oi.price_usd * oi.quantity), 0) / COUNT(DISTINCT o.order_id)
        ELSE 0
    END AS avg_order_value,
    CASE
        WHEN COUNT(DISTINCT s.session_id) > 0
        THEN COUNT(DISTINCT o.order_id)::float / COUNT(DISTINCT s.session_id)
        ELSE 0
    END AS conversion_rate,
    DATE(s.start_ts) AS event_date
FROM dds.sessions s
JOIN dds.users u ON s.user_id = u.user_id
LEFT JOIN dds.user_locations l ON u.user_id = l.user_id
LEFT JOIN dds.orders o ON u.user_id = o.user_id
LEFT JOIN dds.order_items oi ON o.order_id = oi.order_id
GROUP BY COALESCE(l.city, u.city), COALESCE(l.country, u.country), DATE(s.start_ts);
