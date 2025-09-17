SELECT
    s.session_id,
    s.user_id::bigint AS user_id,
    s.start_ts AS start_time,
    s.end_ts AS end_time,
    EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))::int AS duration_seconds,
    s.event_count AS events_count,
    SUM(CASE WHEN e.event_type = 'view_page' THEN 1 ELSE 0 END) AS page_views_count,
    SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS orders_count,
    MAX(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS conversion_flag,
    d.device_type,
    d.os,
    d.browser,
    l.city,
    l.country
FROM dds.sessions s
LEFT JOIN dds.events e ON s.session_id = e.session_id
LEFT JOIN dds.user_devices d ON s.user_id = d.user_id
LEFT JOIN dds.user_locations l ON s.user_id = l.user_id
GROUP BY s.session_id, s.user_id, s.start_ts, s.end_ts, s.event_count,
         d.device_type, d.os, d.browser, l.city, l.country;
