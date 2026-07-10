-- Retourne les matchs dont la date est dans le futur
-- Un match futur dans les données d'entraînement = leakage potentiel
{{ config(severity='warn') }}

SELECT match_id, team, date
FROM {{ ref('backbone') }}
WHERE date > CURRENT_DATE