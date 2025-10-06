WITH user_first_dates AS (
    SELECT 
        user_id,
        DATE(MIN(order_ts)) AS first_order_date
    FROM dds.orders
    WHERE status = 'success'
    GROUP BY user_id
),
daily_sessions AS (
    SELECT 
        DATE(start_ts) AS event_date,
        COUNT(DISTINCT session_id) AS total_sessions
    FROM dds.sessions
    GROUP BY DATE(start_ts)
),
daily_orders AS (
    SELECT
        DATE(o.order_ts) AS event_date,
        SUM(CASE WHEN o.status = 'success' THEN o.total_usd ELSE 0 END) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT o.user_id) AS unique_customers,
        COUNT(DISTINCT CASE WHEN o.status = 'success' THEN o.order_id END) AS successful_orders,
        COUNT(DISTINCT CASE WHEN DATE(o.order_ts) = ufd.first_order_date THEN o.user_id END) AS new_users_from_orders
    FROM dds.orders o
    LEFT JOIN user_first_dates ufd ON o.user_id = ufd.user_id
    GROUP BY DATE(o.order_ts)
)
SELECT
    ds.event_date,
    COALESCE(d_o.total_revenue, 0.0) AS total_revenue,
    COALESCE(d_o.total_orders, 0) AS total_orders,
    COALESCE(d_o.unique_customers, 0) AS unique_customers,
    CASE 
        WHEN COALESCE(d_o.successful_orders, 0) > 0 THEN 
            COALESCE(d_o.total_revenue, 0.0) / COALESCE(d_o.successful_orders, 1)
        ELSE 0 
    END AS avg_order_value, 
    CASE 
        WHEN ds.total_sessions > 0 THEN 
            COALESCE(d_o.successful_orders, 0) * 1.0 / ds.total_sessions 
        ELSE 0 
    END AS conversion_rate, 
    COALESCE(d_o.new_users_from_orders, 0) AS new_users,
    ds.total_sessions,
    COALESCE(d_o.successful_orders, 0) AS successful_orders
FROM daily_sessions ds 
LEFT JOIN daily_orders d_o ON ds.event_date = d_o.event_date
ORDER BY ds.event_date;