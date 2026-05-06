-- Vérifie que la couverture Understat est ≥ 95% sur les Big5 uniquement
-- (Understat ne couvre pas les Coupes ni les ligues secondaires)
WITH coverage AS (
    SELECT
        COUNT(*)                                        AS total,
        COUNT(CASE WHEN np_xg IS NOT NULL THEN 1 END)  AS matched
    FROM {{ source('gold', 'stg_backbone') }}
    WHERE league_source IN (
        'Ligue 1', 'Premier League', 'La Liga', 'Bundesliga', 'Serie A'
    )
)
SELECT total, matched,
    ROUND(100.0 * matched / NULLIF(total, 0), 2) AS coverage_pct
FROM coverage
WHERE ROUND(100.0 * matched / NULLIF(total, 0), 2) < 95