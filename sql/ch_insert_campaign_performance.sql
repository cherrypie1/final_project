SELECT
    c.campaign_id,
    c.campaign_name,
    MIN(DATE(ce.generated_at)) AS start_date,
    MAX(DATE(ce.generated_at)) AS end_date,
    COUNT(DISTINCT s.session_id) AS total_sessions,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(CASE WHEN o.status = 'success' THEN o.total_usd ELSE 0 END), 0) AS total_revenue,
    COUNT(DISTINCT ce.user_id) AS unique_users,
    CASE 
        WHEN COUNT(DISTINCT s.session_id) > 0 THEN
            COUNT(DISTINCT o.order_id) * 1.0 / COUNT(DISTINCT s.session_id)
        ELSE 0 
    END AS conversion_rate,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) > 0 THEN
            COALESCE(SUM(CASE WHEN o.status = 'success' THEN o.total_usd ELSE 0 END), 0) / COUNT(DISTINCT o.order_id)
        ELSE 0 
    END AS avg_order_value,
    SUM(ce.cost) AS cost,
    CASE 
        WHEN SUM(ce.cost) > 0 THEN 
            (COALESCE(SUM(CASE WHEN o.status = 'success' THEN o.total_usd ELSE 0 END), 0) - SUM(ce.cost)) / SUM(ce.cost)
        ELSE 0 
    END AS roi
FROM dds.campaigns c
JOIN dds.campaign_events ce ON c.campaign_id = ce.campaign_id
LEFT JOIN dds.sessions s ON ce.user_id = s.user_id 
    AND DATE(s.start_ts) = DATE(ce.generated_at)
LEFT JOIN dds.orders o ON ce.user_id = o.user_id 
    AND o.order_ts BETWEEN ce.generated_at AND ce.generated_at + INTERVAL '14 days'
    AND o.status = 'success'
GROUP BY c.campaign_id, c.campaign_name;