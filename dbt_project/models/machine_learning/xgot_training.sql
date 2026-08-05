{{
    config(
        materialized='view',
        schema='machine_learning',
        alias='xgot_training'
    )
}}

-- Dataset d'entraînement du modèle xGOT (post-shot xG).
-- Cible : label = is_goal (le tir cadré finit-il au fond, vu son placement).
-- = socle de features (xgot_features) + label. Toute la logique de features et le
-- périmètre « tir cadré éligible » vivent dans xgot_features, partagé avec le
-- scoring → train/serve skew structurellement impossible.
-- Grain : un tir cadré éligible. Vue → toujours à jour.

SELECT
    * EXCLUDE (is_goal),
    is_goal::INTEGER AS label
FROM {{ ref('xgot_features') }}