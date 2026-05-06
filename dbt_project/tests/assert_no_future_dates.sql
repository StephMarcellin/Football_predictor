-- Retourne les matchs dont la date est dans le futur
-- Un match futur dans les données d'entraînement = leakage potentiel
SELECT match_id, team, date
FROM {{ source('gold', 'stg_backbone') }}
WHERE date > CURRENT_DATE