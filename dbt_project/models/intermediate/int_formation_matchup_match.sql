{{ config(materialized='table', schema='intermediate', alias='int_formation_matchup_match') }}

-- int_formation_matchup_match — grain (match_id, team_id) DIRECTIONNEL A→B.
-- Deltas structurels entre la formation de l'équipe A et celle de l'adversaire B.
-- Signal complémentaire aux features individuelles de int_lineup_formation :
-- capte les effets non-linéaires de matchup formation × formation
-- (ex. vulnérabilité 4-back face à pistons 3-back / 5-back).
-- Consommé par mart_1n2 via jointure sur (match_id, team_id).
--
-- Ne dépend PAS de backbone (qui ne couvre que Big5) : l'adversaire d'une équipe
-- dans un match est l'autre équipe présente dans int_lineup_formation pour ce
-- match_id (self-join). Marche donc aussi pour les matchs de coupe et d'Europe.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_lineup_formation lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_lineup_formation AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        str_formation_family                                         AS "formation_family",
        int_n_gk                                                     AS "n_gk",
        int_n_def                                                    AS "n_def",
        int_n_mid                                                    AS "n_mid",
        int_n_att                                                    AS "n_att",
        int_n_wingers                                                AS "n_wingers",
        int_n_central_att                                            AS "n_central_att",
        dec_bloc_width                                               AS "bloc_width",
        dec_bloc_depth                                               AS "bloc_depth",
        dec_line_defensive_avg                                       AS "line_defensive_avg",
        dec_line_offensive_avg                                       AS "line_offensive_avg",
        dec_axiality_score                                           AS "axiality_score"
    FROM {{ ref('int_lineup_formation') }}
),

mdl_body AS (
with

-- Une ligne = un match complet où les DEUX compos sont exploitables.
-- Le having count=2 protège des matchs à couverture partielle (une seule compo scrapée).
valid_matches as (
    select match_id
    from in_int_lineup_formation
    where match_id is not null and team_id is not null
    group by 1
    having count(distinct team_id) = 2
),

filtered as (
    select f.*
    from in_int_lineup_formation f
    join valid_matches vm using (match_id)
),

-- Self-join : pour chaque (match, team_A), o.team_id = l'AUTRE équipe du match.
paired as (
    select
        s.match_id,
        s.team_id,
        o.team_id                as opponent_id,
        s.formation_family,
        s.n_def, s.n_mid, s.n_att,
        s.n_wingers, s.n_central_att,
        s.bloc_width, s.bloc_depth,
        s.line_defensive_avg, s.line_offensive_avg,
        s.axiality_score,
        o.formation_family       as opp_formation_family,
        o.n_def                  as opp_n_def,
        o.n_mid                  as opp_n_mid,
        o.n_att                  as opp_n_att,
        o.n_wingers              as opp_n_wingers,
        o.n_central_att          as opp_n_central_att,
        o.bloc_width             as opp_bloc_width,
        o.bloc_depth             as opp_bloc_depth,
        o.line_defensive_avg     as opp_line_defensive_avg,
        o.line_offensive_avg     as opp_line_offensive_avg,
        o.axiality_score         as opp_axiality_score
    from filtered s
    join filtered o
        on  o.match_id = s.match_id
        and o.team_id != s.team_id
)

select
    match_id,
    team_id,
    opponent_id,
    (n_att              - opp_n_def)              as attack_overload,
    (n_mid              - opp_n_mid)              as mid_control_delta,
    (line_defensive_avg - opp_line_defensive_avg) as back_depth_delta,
    (bloc_width         - opp_bloc_width)         as width_delta,
    (bloc_depth         - opp_bloc_depth)         as depth_delta,
    (axiality_score     - opp_axiality_score)     as axiality_delta,
    formation_family                                       as formation_family_self,
    opp_formation_family                                   as formation_family_opp,
    concat(formation_family, '_vs_', opp_formation_family) as matchup_family
from paired
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        "attack_overload"                                            AS int_attack_overload,
        "mid_control_delta"                                          AS int_mid_control_delta,
        "back_depth_delta"                                           AS dec_back_depth_delta,
        "width_delta"                                                AS dec_width_delta,
        "depth_delta"                                                AS dec_depth_delta,
        "axiality_delta"                                             AS dec_axiality_delta,
        "formation_family_self"                                      AS str_formation_family_self,
        "formation_family_opp"                                       AS str_formation_family_opp,
        "matchup_family"                                             AS str_matchup_family
    FROM mdl_body
)

SELECT * FROM mdl_out
