WITH activity AS (
    SELECT
        s.user_id,
        COUNT(*) AS session_count
    FROM dds.sessions s
    WHERE s.start_ts >= NOW() - INTERVAL '7 days'
    GROUP BY s.user_id
)
SELECT
    DATE(NOW()) AS event_date,
    COUNT(DISTINCT user_id) AS active_users_count,
    COUNT(DISTINCT CASE WHEN session_count > 1 THEN user_id END) AS returning_users,
    0 AS new_users, -- тут пока нули не успела дописать выявление новых пользователей
    0 AS churned_users -- требуется доработка
FROM activity;



