SELECT
    d.device_type,
    d.os,
    d.browser,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(oi.price_usd * oi.quantity), 0) AS total_revenue,
    DATE(s.start_ts) AS event_date
FROM dds.user_devices d
JOIN dds.users u ON d.user_id = u.user_id
LEFT JOIN dds.sessions s ON u.user_id = s.user_id
LEFT JOIN dds.orders o ON u.user_id = o.user_id
LEFT JOIN dds.order_items oi ON o.order_id = oi.order_id
GROUP BY d.device_type, d.os, d.browser, DATE(s.start_ts);
