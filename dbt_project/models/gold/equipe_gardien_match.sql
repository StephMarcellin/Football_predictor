{{ config(materialized='table', schema='gold') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- equipe_gardien_match — grain (match_id, team_id).
-- Profil de saison décalé du gardien titulaire (famille 9), rattaché à l'équipe.
-- ══════════════════════════════════════════════════════════════════════════════

with keeper_starter as (
    select match_id, team_id, player_id
    from {{ ref('int_whoscored_player_match') }}
    where position = 'GK' and is_first_eleven
    qualify row_number() over (partition by match_id, team_id order by player_id) = 1
)

select
    ks.match_id, ks.team_id,
    gs.keeper_psxg_plus_minus_lag,
    gs.keeper_psxg_per_shot_lag,
    gs.keeper_save_pct_lag,
    gs.keeper_shots_faced_lag
from keeper_starter ks
left join {{ ref('backbone') }} b using (match_id, team_id)
left join {{ ref('gardien_saison') }} gs
    on gs.keeper_id = ks.player_id and gs.season = b.season