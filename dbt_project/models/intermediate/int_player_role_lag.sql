{{ config(materialized='table', schema='intermediate', alias='int_player_role_lag') }}

with
-- Nouvelle logique dans int_player_role_lag
apparitions_classified as (
    select
        l.player_id,
        idx.season as source_season,
        (l.end_minute - l.start_minute) as minutes_titu,
        case
            when l.grid_vertical <= 0.5 then 'GK'
            when l.grid_vertical <= 3.0 and abs(l.grid_horizontal - 5) <= 1.5 then 'CB'
            when l.grid_vertical <= 3.0 then 'FB'
            when l.grid_vertical <  5.0 and abs(l.grid_horizontal - 5) <= 1.5 then 'DM'
            when l.grid_vertical <= 6.0 and abs(l.grid_horizontal - 5) <= 1.5 then 'CM'
            when l.grid_vertical <= 7.5 and abs(l.grid_horizontal - 5) <= 1.5 then 'AM'
            when l.grid_vertical <= 6.0 then 'WM'
            when abs(l.grid_horizontal - 5) <= 1.5 then 'ST'
            else 'W'
        end as role_this_app
    from {{ ref('int_whoscored_lineup') }} l
    join {{ ref('int_whoscored_match_index') }} idx using (match_id)
    where (l.end_minute - l.start_minute) > 0
),

role_minutes_by_player_season as (
    -- Cumul minutes par (player, season, role)
    select player_id, source_season, role_this_app as role,
           sum(minutes_titu) as minutes_in_role
    from apparitions_classified
    group by 1, 2, 3
),

dominant_role as (
    -- Rôle dominant en minutes cumulées par (player, season)
    select player_id, source_season, role as role_fin_lag,
           minutes_in_role
    from role_minutes_by_player_season
    qualify row_number() over (
        partition by player_id, source_season
        order by minutes_in_role desc, role   -- tie-break stable par nom du rôle
    ) = 1
),

season_agg as (
    -- Coord moyennes (info seule, plus utilisées pour classer) + compteurs
    select
        ac.player_id, ac.source_season,
        sum(minutes_titu) as minutes_titu_lag,
        count(*) as apps_starter_lag,
        sum(grid_vertical * minutes_titu)   / sum(minutes_titu) as gv_avg,
        sum(grid_horizontal * minutes_titu) / sum(minutes_titu) as gh_avg
    from apparitions_classified ac
    join {{ ref('int_whoscored_lineup') }} l
        on l.player_id = ac.player_id
        -- ...
    group by 1, 2
),

-- Puis lag +1 saison et select final

lagged as (
    select
        player_id,
        -- printf est supporté nativement par DuckDB
        printf('%d-%d', 
            cast(substr(source_season, 1, 4) as integer) + 1,
            cast(substr(source_season, 6, 4) as integer) + 1
        ) as season,
        gv_avg,
        gh_avg,
        minutes_titu_lag,
        apps_starter_lag
    from season_agg
)

select
    player_id,
    season,
    gv_avg,
    gh_avg,
    abs(gh_avg - 5) as gh_offaxis,
    minutes_titu_lag,
    apps_starter_lag,
    case
        when gv_avg is null                               then null
        when gv_avg <= 0.5                                then 'GK'
        
        -- gv ∈ ]0.5, 3.0] : Défenseurs
        when gv_avg <= 3.0 and abs(gh_avg - 5) <= 2.0     then 'CB'
        when gv_avg <= 3.0                                then 'FB'
        
        -- gv ∈ ]3.0, 5.0[ : Milieux Défensifs (strictement inférieur à 5.0 pour inclure 4.5)
        when gv_avg <  5.0 and abs(gh_avg - 5) <= 1.5     then 'DM'
        
        -- gv ∈ [5.0, 6.0] : Milieux Centraux & Au-delà
        when gv_avg <= 6.0 and abs(gh_avg - 5) <= 1.5     then 'CM'
        when gv_avg <= 5.5                                then 'WM'
        
        -- gv ∈ ]6.0, 7.5] : Milieux Offensifs & Ailiers
        when gv_avg <= 7.5 and abs(gh_avg - 5) <= 1.5     then 'AM'
        
        -- gv > 7.5 : Attaquants
        when abs(gh_avg - 5) <= 1.5                       then 'ST'
        else                                                   'W'
    end as role_fin_lag
from lagged