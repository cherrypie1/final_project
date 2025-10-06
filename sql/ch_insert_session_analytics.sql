SELECT
    s.session_id::text AS session_id,
    s.user_id,
    s.start_ts AS start_time,
    s.end_ts AS end_time,
    GREATEST(EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))::int, 0) AS duration_seconds,
    s.event_count AS events_count,
    COUNT(CASE WHEN e.event_type = 'view_page' THEN 1 END) AS page_views_count,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.order_id END) AS orders_count,
    CASE WHEN COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.order_id END) > 0 THEN 1 ELSE 0 END AS conversion_flag,
    COALESCE(d.device_type, 'Unknown') AS device_type,
    COALESCE(d.browser, 'Unknown') AS browser,
    COALESCE(d.os, 'Unknown') AS os,
    COALESCE(l.city, 'Unknown') AS city,
    COALESCE(l.country, 'Unknown') AS country
FROM dds.sessions s
LEFT JOIN dds.events e ON s.session_id = e.session_id
LEFT JOIN dds.user_devices d ON s.user_id = d.user_id
LEFT JOIN dds.user_locations l ON s.user_id = l.user_id
WHERE s.start_ts IS NOT NULL AND s.end_ts IS NOT NULL
GROUP BY s.session_id, s.user_id, s.start_ts, s.end_ts, s.event_count, 
         d.device_type, d.browser, d.os, l.city, l.country;