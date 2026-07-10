-- Retourne les lignes où np_xg est hors limites physiologiques
SELECT match_id, team, np_xg
FROM {{ ref('backbone') }}
WHERE np_xg IS NOT NULL
  AND (np_xg < 0 OR np_xg > 10)