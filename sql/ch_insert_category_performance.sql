SELECT
    c.category_name,
    SUM(oi.quantity) AS total_quantity,
    SUM(CASE 
        WHEN (oi.price_usd * oi.quantity) <= 10000 THEN  
            oi.price_usd * oi.quantity 
        ELSE 0 
    END) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS orders_count,
    COUNT(DISTINCT o.user_id) AS unique_customers,
    CASE 
        WHEN COUNT(DISTINCT oi.order_id) > 0 THEN
            SUM(CASE 
                WHEN (oi.price_usd * oi.quantity) <= 10000 THEN 
                    oi.price_usd * oi.quantity 
                ELSE 0 
            END) / COUNT(DISTINCT oi.order_id)
        ELSE 0 
    END AS avg_order_value,
    DATE(o.order_ts) AS event_date
FROM dds.order_items oi
JOIN dds.orders o ON oi.order_id = o.order_id AND o.status = 'success'  
LEFT JOIN dds.products p ON oi.product_id = p.product_id
LEFT JOIN dds.categories c ON p.category_id = c.category_id
GROUP BY c.category_name, DATE(o.order_ts);