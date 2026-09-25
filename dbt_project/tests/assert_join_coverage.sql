-- Vérifie que la couverture Understat est ≥ 95% sur les Big5 uniquement
-- (Understat ne couvre pas les Coupes ni les ligues secondaires).
-- Refonte nommage : colonnes backbone renommées ; matchs non joués exclus
-- (pas d'xG attendu avant le match).
WITH coverage AS (
    SELECT
        COUNT(*)                                            AS total,
        COUNT(CASE WHEN dec_np_xg IS NOT NULL THEN 1 END)   AS matched
    FROM {{ ref('backbone') }}
    WHERE str_league_source IN (
        'Ligue 1', 'Premier League', 'La Liga', 'Bundesliga', 'Serie A'
    )
    AND str_result_1n2 IS NOT NULL
)
SELECT total, matched,
    ROUND(100.0 * matched / NULLIF(total, 0), 2) AS coverage_pct
FROM coverage
WHERE ROUND(100.0 * matched / NULLIF(total, 0), 2) < 95