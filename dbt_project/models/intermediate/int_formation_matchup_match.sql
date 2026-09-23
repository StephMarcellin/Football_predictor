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

with

-- Une ligne = un match complet où les DEUX compos sont exploitables.
-- Le having count=2 protège des matchs à couverture partielle (une seule compo scrapée).
valid_matches as (
    select match_id
    from {{ ref('int_lineup_formation') }}
    where match_id is not null and team_id is not null
    group by 1
    having count(distinct team_id) = 2
),

filtered as (
    select f.*
    from {{ ref('int_lineup_formation') }} f
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