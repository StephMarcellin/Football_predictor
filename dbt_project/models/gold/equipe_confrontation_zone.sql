{{ config(materialized='table', schema='gold') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- equipe_confrontation_zone — grain (match_id, team_id).
-- Pivote zone_confrontation_match (grain couloir) au grain match-équipe :
-- perspective OFFENSIVE (équipe attaquante) + DÉFENSIVE (équipe défendante).
-- Les marts n'ont plus qu'à le sélectionner.
-- ══════════════════════════════════════════════════════════════════════════════

with

teams as (
    select distinct match_id, team_id from (
        select match_id, attacking_team_id as team_id from {{ ref('zone_confrontation_match') }}
        union
        select match_id, defending_team_id     from {{ ref('zone_confrontation_match') }}
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
    from {{ ref('zone_confrontation_match') }}
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
    from {{ ref('zone_confrontation_match') }}
    group by 1, 2
)

select
    t.match_id, t.team_id,
    off.* exclude (match_id, team_id),
    def.* exclude (match_id, team_id)
from teams t
left join off using (match_id, team_id)
left join def using (match_id, team_id)