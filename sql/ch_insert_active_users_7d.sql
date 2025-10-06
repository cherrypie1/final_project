WITH user_first_session AS (
    SELECT
        user_id,
        MIN(DATE(start_ts)) AS first_session_date
    FROM dds.sessions
    WHERE start_ts IS NOT NULL
    GROUP BY user_id
),
daily_active_users AS (
    SELECT DISTINCT
        DATE(start_ts) AS event_date,
        user_id
    FROM dds.sessions
    WHERE start_ts IS NOT NULL
),
user_churn AS (
    SELECT
        dau.event_date,
        dau.user_id,
        CASE 
            WHEN NOT EXISTS (
                SELECT 1 FROM dds.sessions s2 
                WHERE s2.user_id = dau.user_id 
                AND DATE(s2.start_ts) BETWEEN dau.event_date - 6 AND dau.event_date - 1
            ) THEN 1 ELSE 0 
        END AS is_churned
    FROM daily_active_users dau
)
SELECT
    dau.event_date,
    COUNT(DISTINCT dau.user_id) AS active_users_count,
    COUNT(DISTINCT CASE WHEN ufs.first_session_date < dau.event_date THEN dau.user_id END) AS returning_users,
    COUNT(DISTINCT CASE WHEN ufs.first_session_date = dau.event_date THEN dau.user_id END) AS new_users,
    SUM(uc.is_churned) AS reactivated_users
FROM daily_active_users dau
JOIN user_first_session ufs ON dau.user_id = ufs.user_id
LEFT JOIN user_churn uc ON dau.user_id = uc.user_id AND dau.event_date = uc.event_date
GROUP BY dau.event_date
ORDER BY dau.event_date;