SELECT
    COALESCE(ul.city, 'Unknown') AS city,
    COALESCE(ul.country, 'Unknown') AS country,
    COUNT(DISTINCT s.user_id) AS total_users,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_usd), 0) AS total_revenue,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) > 0 THEN
            COALESCE(SUM(o.total_usd), 0) / COUNT(DISTINCT o.order_id)
        ELSE 0 
    END AS avg_order_value,
    CASE 
        WHEN COUNT(DISTINCT s.session_id) > 0 THEN
            COUNT(DISTINCT o.order_id) * 1.0 / COUNT(DISTINCT s.session_id)
        ELSE 0 
    END AS conversion_rate,
    DATE(s.start_ts) AS event_date
FROM dds.sessions s
LEFT JOIN dds.user_locations ul ON s.user_id = ul.user_id
LEFT JOIN dds.orders o ON s.user_id = o.user_id 
    AND DATE(o.order_ts) = DATE(s.start_ts)
    AND o.status = 'success'
WHERE s.start_ts IS NOT NULL
GROUP BY 
    COALESCE(ul.city, 'Unknown'), 
    COALESCE(ul.country, 'Unknown'), 
    DATE(s.start_ts);