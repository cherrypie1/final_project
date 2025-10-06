SELECT
    u.user_id,
    COUNT(DISTINCT s.session_id) AS sessions_count,
    COUNT(e.*) AS total_events,
    AVG(EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))) AS avg_session_duration_seconds,
    CASE 
        WHEN COUNT(DISTINCT s.session_id) > 0 THEN
            COUNT(e.*) * 1.0 / COUNT(DISTINCT s.session_id)
        ELSE 0 
    END AS avg_pages_per_session,
    MIN(DATE(s.start_ts)) AS first_session_date,
    MAX(DATE(s.start_ts)) AS last_session_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_usd), 0) AS total_revenue,
    EXTRACT(DAYS FROM (NOW() - MAX(s.start_ts))) AS days_since_last_activity
FROM dds.users u
LEFT JOIN dds.sessions s ON u.user_id = s.user_id
LEFT JOIN dds.events e ON s.session_id = e.session_id
LEFT JOIN dds.orders o ON u.user_id = o.user_id AND o.status = 'success'
WHERE u.user_id IS NOT NULL
GROUP BY u.user_id;