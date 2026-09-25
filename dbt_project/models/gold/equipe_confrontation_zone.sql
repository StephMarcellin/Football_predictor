{{ config(materialized='table', schema='gold') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- equipe_confrontation_zone — grain (match_id, team_id).
-- Pivote zone_confrontation_match (grain couloir) au grain match-équipe :
-- perspective OFFENSIVE (équipe attaquante) + DÉFENSIVE (équipe défendante).
-- Les marts n'ont plus qu'à le sélectionner.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- zone_confrontation_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_zone_confrontation_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_attacking_team_id AS BIGINT)                        AS "attacking_team_id",
        CAST(str_defending_team_id AS BIGINT)                        AS "defending_team_id",
        str_attack_corridor                                          AS "attack_corridor",
        dec_off_strength                                             AS "off_strength",
        dec_opp_def_solidity                                         AS "opp_def_solidity",
        dec_matchup_danger_by_corridor                               AS "matchup_danger_by_corridor",
        dec_matchup_cross_threat                                     AS "matchup_cross_threat",
        dec_matchup_dribble_threat                                   AS "matchup_dribble_threat",
        dec_matchup_central_control                                  AS "matchup_central_control",
        dec_self_central_progression                                 AS "self_central_progression",
        dec_self_central_touch                                       AS "self_central_touch",
        dec_opp_central_def_density                                  AS "opp_central_def_density"
    FROM {{ ref('zone_confrontation_match') }}
),

mdl_body AS (
with

teams as (
    select distinct match_id, team_id from (
        select match_id, attacking_team_id as team_id from in_zone_confrontation_match
        union
        select match_id, defending_team_id     from in_zone_confrontation_match
    )
),

off as (
    select match_id, attacking_team_id as team_id,
        max(case when attack_corridor='gauche' then matchup_danger_by_corridor end) as off_danger_gauche,
        max(case when attack_corridor='axe'    then matchup_danger_by_corridor end) as off_danger_axe,
        max(case when attack_corridor='droit'  then matchup_danger_by_corridor end) as off_danger_droit,
        max(case when attack_corridor='gauche' then matchup_cross_threat end)       as off_cross_gauche,
        max(case when attack_corridor='droit'  then matchup_cross_threat end)       as off_cross_droit,
        max(case when attack_corridor='gauche' then matchup_dribble_threat end)     as off_dribble_gauche,
        max(case when attack_corridor='droit'  then matchup_dribble_threat end)     as off_dribble_droit,
        max(case when attack_corridor='axe'    then matchup_central_control end)    as off_central_axe
    from in_zone_confrontation_match
    group by 1, 2
),

def as (
    select match_id, defending_team_id as team_id,
        max(case when attack_corridor='gauche' then matchup_danger_by_corridor end) as def_danger_gauche,
        max(case when attack_corridor='axe'    then matchup_danger_by_corridor end) as def_danger_axe,
        max(case when attack_corridor='droit'  then matchup_danger_by_corridor end) as def_danger_droit,
        max(case when attack_corridor='gauche' then matchup_cross_threat end)       as def_cross_gauche,
        max(case when attack_corridor='droit'  then matchup_cross_threat end)       as def_cross_droit,
        max(case when attack_corridor='gauche' then matchup_dribble_threat end)     as def_dribble_gauche,
        max(case when attack_corridor='droit'  then matchup_dribble_threat end)     as def_dribble_droit,
        max(case when attack_corridor='axe'    then matchup_central_control end)    as def_central_axe
    from in_zone_confrontation_match
    group by 1, 2
)

select
    t.match_id, t.team_id,
    off.* exclude (match_id, team_id),
    def.* exclude (match_id, team_id)
from teams t
left join off using (match_id, team_id)
left join def using (match_id, team_id)
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "off_danger_gauche"                                          AS dec_off_danger_gauche,
        "off_danger_axe"                                             AS dec_off_danger_axe,
        "off_danger_droit"                                           AS dec_off_danger_droit,
        "off_cross_gauche"                                           AS dec_off_cross_gauche,
        "off_cross_droit"                                            AS dec_off_cross_droit,
        "off_dribble_gauche"                                         AS dec_off_dribble_gauche,
        "off_dribble_droit"                                          AS dec_off_dribble_droit,
        "off_central_axe"                                            AS dec_off_central_axe,
        "def_danger_gauche"                                          AS dec_def_danger_gauche,
        "def_danger_axe"                                             AS dec_def_danger_axe,
        "def_danger_droit"                                           AS dec_def_danger_droit,
        "def_cross_gauche"                                           AS dec_def_cross_gauche,
        "def_cross_droit"                                            AS dec_def_cross_droit,
        "def_dribble_gauche"                                         AS dec_def_dribble_gauche,
        "def_dribble_droit"                                          AS dec_def_dribble_droit,
        "def_central_axe"                                            AS dec_def_central_axe
    FROM mdl_body
)

SELECT * FROM mdl_out
