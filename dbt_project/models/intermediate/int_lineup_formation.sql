{{ config(materialized='table', schema='intermediate', alias='int_lineup_formation') }}

-- int_lineup_formation — grain (match_id, team_id).
-- Features structurelles de la compo de DÉPART (période 1) : compteurs par
-- ligne via role_fin_resolved, catégorie 3/4/5-back, écart-type spatial du
-- bloc de champ (GK exclu — sa position fixe gv≈0 bruiterait la mesure),
-- hauteur moyenne des lignes défensive et offensive, axialité du XI.
-- Consommé par int_formation_matchup_match (Phase 4B) et par equipe_lineup_match.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_player_role_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_player_role_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        str_season                                                   AS "season",
        dec_gv_start                                                 AS "gv_start",
        dec_gh_start                                                 AS "gh_start",
        str_role_fin_lag                                             AS "role_fin_lag",
        str_role_fin_current                                         AS "role_fin_current",
        str_role_fin_resolved                                        AS "role_fin_resolved",
        str_role_source                                              AS "role_source"
    FROM {{ ref('int_player_role_match') }}
),

-- int_whoscored_lineup lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_lineup AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_formation_seq                                            AS "formation_seq",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        int_period                                                   AS "period",
        int_start_minute                                             AS "start_minute",
        int_end_minute                                               AS "end_minute",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        int_slot                                                     AS "slot",
        dec_grid_vertical                                            AS "grid_vertical",
        dec_grid_horizontal                                          AS "grid_horizontal",
        bool_is_captain                                              AS "is_captain"
    FROM {{ ref('int_whoscored_lineup') }}
),

mdl_body AS (
with

-- XI de départ enrichi de son rôle résolu (Table 2 de Phase 1).
xi_with_role as (
    select
        l.match_id,
        l.team_id,
        l.player_id,
        l.grid_vertical,
        l.grid_horizontal,
        l.formation_id,
        r.role_fin_resolved
    from in_int_whoscored_lineup l
    left join in_int_player_role_match r
        on r.match_id  = l.match_id
       and r.player_id = l.player_id
    where l.start_minute = 0
        and l.match_id is not null
        and l.team_id  is not null
    qualify row_number() over (
        partition by l.match_id, l.player_id
        order by l.formation_seq, l.slot
    ) = 1
),

-- Une seule formation_id par (match, team) : celle de la période 1.
formation_by_team as (
    select match_id, team_id,
           any_value(formation_id) as formation_id
    from xi_with_role
    group by 1, 2
),

-- Compteurs par ligne. n_gk + n_def + n_mid + n_att = 11 par construction.
role_counts as (
    select
        match_id, team_id,
        count(*) filter (where role_fin_resolved = 'GK')                        as n_gk,
        count(*) filter (where role_fin_resolved in ('CB', 'FB'))               as n_def,
        count(*) filter (where role_fin_resolved in ('DM', 'CM', 'AM', 'WM'))   as n_mid,
        count(*) filter (where role_fin_resolved in ('W', 'ST'))                as n_att,
        count(*) filter (where role_fin_resolved in ('W', 'WM'))                as n_wingers,
        count(*) filter (where role_fin_resolved in ('ST', 'AM'))               as n_central_att
    from xi_with_role
    group by 1, 2
),

-- Statistiques spatiales SUR LES 10 JOUEURS DE CHAMP (GK exclu).
-- Le GK à gv=0 tirerait bloc_depth vers le haut artificiellement.
field_stats as (
    select
        match_id, team_id,
        stddev_samp(grid_horizontal)                                            as bloc_width,
        stddev_samp(grid_vertical)                                              as bloc_depth,
        avg(grid_vertical) filter (where role_fin_resolved in ('CB', 'FB'))     as line_defensive_avg,
        avg(grid_vertical) filter (where role_fin_resolved in ('W', 'ST'))      as line_offensive_avg,
        avg(case when abs(grid_horizontal - 5) <= 1.5 then 1.0 else 0.0 end)    as axiality_score
    from xi_with_role
    where role_fin_resolved != 'GK'
    group by 1, 2
)

select
    fbt.match_id,
    fbt.team_id,
    fbt.formation_id,
    -- Catégorie structurelle basée sur n_def : 3/4/5-back. Autre = fallback rare
    -- (n_def = 2 ou 6+ observé sur des compos irrégulières).
    case
        when rc.n_def = 3 then '3-back'
        when rc.n_def = 4 then '4-back'
        when rc.n_def = 5 then '5-back'
        else                   'other'
    end as formation_family,
    rc.n_gk, rc.n_def, rc.n_mid, rc.n_att,
    rc.n_wingers, rc.n_central_att,
    fs.bloc_width,
    fs.bloc_depth,
    fs.line_defensive_avg,
    fs.line_offensive_avg,
    fs.axiality_score
from formation_by_team fbt
left join role_counts  rc using (match_id, team_id)
left join field_stats  fs using (match_id, team_id)
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(formation_id AS VARCHAR)                                AS str_formation_id,
        "formation_family"                                           AS str_formation_family,
        "n_gk"                                                       AS int_n_gk,
        "n_def"                                                      AS int_n_def,
        "n_mid"                                                      AS int_n_mid,
        "n_att"                                                      AS int_n_att,
        "n_wingers"                                                  AS int_n_wingers,
        "n_central_att"                                              AS int_n_central_att,
        "bloc_width"                                                 AS dec_bloc_width,
        "bloc_depth"                                                 AS dec_bloc_depth,
        "line_defensive_avg"                                         AS dec_line_defensive_avg,
        "line_offensive_avg"                                         AS dec_line_offensive_avg,
        "axiality_score"                                             AS dec_axiality_score
    FROM mdl_body
)

SELECT * FROM mdl_out
