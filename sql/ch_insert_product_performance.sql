SELECT
    p.product_id,
    p.product_name,
    c.category_name,
    SUM(oi.quantity) AS total_quantity,
    SUM(oi.price_usd * oi.quantity) AS total_revenue,
    AVG(oi.price_usd) AS avg_price,
    COUNT(DISTINCT oi.order_id) AS orders_count,  
    COUNT(DISTINCT o.user_id) AS unique_customers,
    DATE(o.order_ts) AS event_date
FROM dds.order_items oi
JOIN dds.orders o ON oi.order_id = o.order_id AND o.status = 'success'
JOIN dds.products p ON oi.product_id = p.product_id
JOIN dds.categories c ON p.category_id = c.category_id
GROUP BY p.product_id, p.product_name, c.category_name, DATE(o.order_ts);