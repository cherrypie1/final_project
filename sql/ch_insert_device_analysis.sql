SELECT
    d.device_type,
    d.os,
    d.browser,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_usd) AS total_revenue,
    AVG(EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))) AS avg_session_duration,
    COUNT(DISTINCT o.order_id)::float / NULLIF(COUNT(DISTINCT s.session_id), 0) AS conversion_rate,
    DATE(s.start_ts) AS event_date
FROM dds.users u
LEFT JOIN dds.sessions s ON u.user_id = s.user_id
LEFT JOIN dds.orders o ON u.user_id::bigint = o.user_id::bigint
LEFT JOIN dds.user_devices d ON u.user_id::bigint = d.user_id::bigint
GROUP BY d.device_type, d.os, d.browser, DATE(s.start_ts);
