{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_events — grain (match_id) — cibles total_cards / total_corners (bruts ;
-- seuils fixés en config). Croise discipline + corners + coups francs des DEUX
-- équipes (profil d'équipe construit une fois, préfixé home_/away_).
-- ══════════════════════════════════════════════════════════════════════════════

with

matches as (
    select match_id,
        max(case when venue='Home' then team_id end) as home_team_id,
        max(case when venue='Away' then team_id end) as away_team_id,
        max(date) as date, max(season) as season
    from {{ ref('backbone') }} where match_id is not null
    group by match_id
),

label as (
    select match_id, sum(yellow_cards + coalesce(red_cards, 0)) as total_cards
    from {{ ref('backbone') }} where match_id is not null
    group by match_id
),

corners as (
    select match_id, sum(corners_total) as total_corners
    from {{ ref('int_whoscored_player_match') }}
    group by match_id
),

-- Profil événementiel d'une équipe : discipline + corners + coups francs.
team_events as (
    select em.match_id, em.team_id,
        em.yellow_cards_rolling_3, em.yellow_cards_rolling_5, em.yellow_cards_rolling_10,
        em.fouls_committed_rolling_3, em.fouls_committed_rolling_5, em.fouls_committed_rolling_10,
        rc.* exclude (match_id, team_id, date, season, league_source),
        rf.* exclude (match_id, team_id, date, season, league_source)
    from {{ ref('equipe_match') }} em
    left join {{ ref('rolling_corners') }}   rc using (match_id, team_id)
    left join {{ ref('rolling_freekicks') }} rf using (match_id, team_id)
),

home as (select columns('.*') as "home_\0" from team_events),
away as (select columns('.*') as "away_\0" from team_events)

select
    m.match_id, m.date, m.season,
    h.* exclude (home_match_id, home_team_id),
    a.* exclude (away_match_id, away_team_id),
    label.total_cards,
    corners.total_corners
from matches m
left join home    h on h.home_match_id = m.match_id and h.home_team_id = m.home_team_id
left join away    a on a.away_match_id = m.match_id and a.away_team_id = m.away_team_id
left join label   using (match_id)
left join corners using (match_id)