{{ config(materialized='table', schema='gold') }}

-- gold_team_match_elo — grain (str_match_id, str_team_id). Elo et classement AVANT le match.
-- Lit machine_learning.elo_team_match, écrite par pipelines/features/team_elo.py (récurrence séquentielle : non exprimable en SQL).
-- Aucune fuite : valeurs figées avant la mise à jour par le résultat du match (voir team_elo.py).
select
    str_match_id,
    str_team_id,
    dec_elo,
    dec_opp_elo,
    dec_elo_diff,
    dec_elo_win_exp,
    dec_s_ppg,
    dec_s_gd_pg,
    dec_s_rank_pct,
    int_s_played,
    dec_opp_s_ppg,
    dec_opp_s_gd_pg,
    dec_opp_s_rank_pct,
    int_opp_s_played,
    dec_s_ppg_diff
from {{ source('machine_learning', 'elo_team_match') }}
