-- Retourne les lignes où np_xg est hors limites physiologiques
SELECT str_match_id, str_team_id, dec_np_xg
FROM {{ ref('backbone') }}
WHERE dec_np_xg IS NOT NULL
  AND (dec_np_xg < 0 OR dec_np_xg > 10)