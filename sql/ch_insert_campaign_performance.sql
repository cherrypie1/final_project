SELECT
    c.campaign_id,
    c.campaign_name,
    MIN(r.generated_at)::date AS start_date,
    MAX(r.generated_at)::date AS end_date,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_usd), 0) AS total_revenue,
    COUNT(DISTINCT r.user_id) AS unique_users,
    CASE
        WHEN COUNT(DISTINCT s.session_id) > 0
        THEN COUNT(DISTINCT o.order_id)::float / COUNT(DISTINCT s.session_id)
        ELSE 0
    END AS conversion_rate,
    CASE
        WHEN COUNT(DISTINCT o.order_id) > 0
        THEN COALESCE(SUM(o.total_usd), 0) / COUNT(DISTINCT o.order_id)
        ELSE 0
    END AS avg_order_value,
    COALESCE(SUM(r.cost), 0) AS cost,
    CASE
        WHEN COALESCE(SUM(r.cost), 0) > 0
        THEN (COALESCE(SUM(o.total_usd), 0) - COALESCE(SUM(r.cost), 0)) / COALESCE(SUM(r.cost), 0)
        ELSE 0
    END AS roi
FROM dds.campaigns c
JOIN raw.campaign_events r ON c.campaign_id = r.campaign_id
LEFT JOIN dds.sessions s ON r.user_id::bigint = s.user_id
LEFT JOIN dds.orders o ON r.user_id::bigint = o.user_id
GROUP BY c.campaign_id, c.campaign_name;