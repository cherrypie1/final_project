SELECT
    u.user_id::bigint AS user_id,
    COUNT(DISTINCT s.session_id) AS sessions_count,
    SUM(s.event_count) AS total_events,
    AVG(EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))) AS avg_session_duration_seconds,
    AVG(s.event_count) AS avg_pages_per_session,
    MIN(s.start_ts)::date AS first_session_date,
    MAX(s.end_ts)::date AS last_session_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_usd), 0) AS total_revenue,
    EXTRACT(DAY FROM (NOW() - MAX(s.end_ts)))::int AS days_since_last_activity
FROM dds.users u
LEFT JOIN dds.sessions s ON u.user_id::bigint = s.user_id::bigint
LEFT JOIN dds.orders o ON u.user_id::bigint = o.user_id::bigint
GROUP BY u.user_id;
