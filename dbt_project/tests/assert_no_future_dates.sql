-- Retourne les matchs dont la date est dans le futur
-- Un match futur dans les données d'entraînement = leakage potentiel
{{ config(severity='warn') }}

SELECT str_match_id, str_team_id, dt_date
FROM {{ ref('backbone') }}
WHERE dt_date > CURRENT_DATE