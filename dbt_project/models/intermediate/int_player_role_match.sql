{{ config(materialized='table', schema='intermediate', alias='int_player_role_match') }}

-- int_player_role_match — grain (match_id, player_id).
-- Rôle tactique résolu pour ce match, cascade :
--   role_fin_lag     = int_player_role_lag (peut être NULL : recrue N-1)
--   role_fin_current = calculé sur la coord (gv, gh) du slot du joueur dans
--                      la formation INITIALE de ce match (période 1).
--   role_fin_resolved = COALESCE(role_fin_lag, role_fin_current) — JAMAIS NULL
--                       pour un titulaire du XI de départ.
-- Anti-leakage : la formation initiale est annoncée pré-match, la coord slot
-- n'est pas de l'info future.
-- Consommée par le KNN (Phase 3) et serve_features.py.

with

-- XI de départ : première période de formation (start_minute = 0).
-- qualify row_number() = 1 protège du cas rare où plusieurs enregistrements
-- existent pour un même joueur en période 1 (pas connu à ce jour, ceinture+bretelles).
starting_xi as (
    select
        l.match_id,
        l.team_id,
        l.player_id,
        l.grid_vertical   as gv_start,
        l.grid_horizontal as gh_start
    from {{ ref('int_whoscored_lineup') }} l
    where l.start_minute = 0
    qualify row_number() over (
        partition by l.match_id, l.player_id
        order by l.formation_seq, l.slot
    ) = 1
),

-- Ajout de la saison (nécessaire pour la jointure sur int_player_role_lag).
with_season as (
    select
        xi.*,
        idx.season
    from starting_xi xi
    join {{ ref('int_whoscored_match_index') }} idx using (match_id)
),

-- role_fin_current : mêmes règles qu'int_player_role_lag, sur la coord DE CE MATCH.
with_current as (
    select
        ws.*,
        case
            when gv_start is null                               then null
            when gv_start <= 0.5                                then 'GK'
            
            -- gv ∈ ]0.5, 3.0] : Défenseurs
            when gv_start <= 3.0 and abs(gh_start - 5) <= 2.0     then 'CB'
            when gv_start <= 3.0                                then 'FB'
            
            -- gv ∈ ]3.0, 5.0[ : Milieux Défensifs (strictement inférieur à 5.0 pour inclure 4.5)
            when gv_start <  5.0 and abs(gh_start - 5) <= 1.5     then 'DM'
            
            -- gv ∈ [5.0, 6.0] : Milieux Centraux & Au-delà
            when gv_start <= 6.0 and abs(gh_start - 5) <= 1.5     then 'CM'
            when gv_start <= 5.5                                then 'WM'
            
            -- gv ∈ ]6.0, 7.5] : Milieux Offensifs & Ailiers
            when gv_start <= 7.5 and abs(gh_start - 5) <= 1.5     then 'AM'
            
            -- gv > 7.5 : Attaquants
            when abs(gh_start - 5) <= 1.5                       then 'ST'
            else                                                   'W'
        end as role_fin_current
    from with_season ws
)

select
    wc.match_id,
    wc.team_id,
    wc.player_id,
    wc.season,
    wc.gv_start,
    wc.gh_start,
    lag.role_fin_lag,
    wc.role_fin_current,
    coalesce(lag.role_fin_lag, wc.role_fin_current) as role_fin_resolved,
    case
        when lag.role_fin_lag is not null then 'season_lag'
        else                                   'match_slot'
    end as role_source
from with_current wc
left join {{ ref('int_player_role_lag') }} lag
    on lag.player_id = wc.player_id
   and lag.season    = wc.season