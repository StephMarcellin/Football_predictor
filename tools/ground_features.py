"""
ground_features.py — mesure du gain 1N2 des duels au sol et de l'arbitre (palier 18). Écrit trois CSV de features
externes pour `tools/rolling_chunk.py --extra`, jointes sur (match, équipe) :
  reports/chunks/ground_all.csv  : 9 mesures de la compo de départ + les mêmes 9 pour l'adversaire + 3 écarts
  reports/chunks/referee.csv     : profil de l'arbitre du match (fenêtre s-1 ou s-2), 7 colonnes
  reports/chunks/ground_ref.csv  : les deux ensemble
Sans fuite : profils joueur et arbitre lus sur la dernière fenêtre dont la saison de fin est s-1 ou s-2 (la fenêtre
inclut sa dernière saison). L'arbitre d'un match est connu avant le coup d'envoi (PMO) ; la compo environ 1 h avant.
Un joueur sans profil n'entre pas dans les moyennes. Groupes : A = ATT et MIL (dribbleurs), D = DEF, hors gardiens.
Usage : python tools/ground_features.py
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
import ml_common as mc  # noqa: E402

KEY = ["str_match_id", "str_team_id"]
XI = ["dec_xi_dribble_woe_att", "dec_xi_defended_woe_def", "dec_xi_dispossessed_share", "dec_xi_foul_committed_share",
      "dec_xi_foul_drawn_share_att", "dec_xi_yellow_per_foul", "dec_xi_fouls_committed_per_90_sum",
      "dec_xi_penalties_conceded_per_match_sum", "dec_xi_dribbles_per_90_sum"]
EDGES = ["dec_edge_dribble", "dec_edge_defended", "dec_edge_foul_draw"]
REF = ["dec_ref_yellow_net", "dec_ref_red_net", "dec_ref_fouls_net", "dec_ref_penalties_net", "dec_ref_home_goal_diff_net",
       "dec_ref_home_yellow_diff_net", "dec_ref_n_matches"]

SQL = """
WITH
mc AS (
    SELECT str_match_id, str_team_id, str_opponent_id, int_is_home, CAST(SUBSTR(str_season, 1, 4) AS INTEGER) AS y
    FROM gold.gold_team_match
),
xi AS (
    SELECT DISTINCT l.str_match_id, l.str_team_id, l.str_player_id, m.str_opponent_id, m.y
    FROM intermediate.intermediate_whoscored_lineup_period l
    JOIN mc m ON m.str_match_id = l.str_match_id AND m.str_team_id = l.str_team_id
    WHERE l.int_start_minute = 0 AND l.str_match_id IS NOT NULL
),
pos AS (
    SELECT str_player_id, arg_max(str_macro_position, int_n_matchs) AS pos
    FROM intermediate.intermediate_whoscored_player_team_season GROUP BY str_player_id
),
lp AS (
    SELECT x.str_match_id, x.str_team_id, x.str_player_id, MAX(p.str_season) AS ps
    FROM xi x JOIN gold.gold_player_ground_profile p
      ON p.str_player_id = x.str_player_id AND CAST(SUBSTR(p.str_season, 1, 4) AS INTEGER) BETWEEN x.y - 2 AND x.y - 1
    GROUP BY ALL
),
xp AS (
    SELECT x.str_match_id, x.str_team_id, x.str_opponent_id, a.pos, p.*
    FROM xi x
    LEFT JOIN pos a ON a.str_player_id = x.str_player_id
    LEFT JOIN lp ON lp.str_match_id = x.str_match_id AND lp.str_team_id = x.str_team_id AND lp.str_player_id = x.str_player_id
    LEFT JOIN gold.gold_player_ground_profile p ON p.str_player_id = x.str_player_id AND p.str_season = lp.ps
),
own AS (
    SELECT str_match_id, str_team_id, ANY_VALUE(str_opponent_id) AS str_opponent_id,
        AVG(dec_dribble_woe_adj) FILTER (WHERE pos IN ('ATT', 'MIL'))                               AS dribble_woe_att,
        AVG(dec_defended_woe_adj) FILTER (WHERE pos = 'DEF')                                         AS defended_woe_def,
        AVG(dec_dispossessed_share) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                        AS dispossessed_share,
        AVG(dec_foul_committed_share) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                      AS foul_committed_share,
        AVG(dec_foul_drawn_share) FILTER (WHERE pos IN ('ATT', 'MIL'))                               AS foul_drawn_share_att,
        AVG(dec_yellow_per_foul) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                           AS yellow_per_foul,
        SUM(dec_fouls_committed_per_90) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                    AS fouls_committed_per_90_sum,
        SUM(int_n_penalties_conceded * 1.0 / NULLIF(int_n_matches, 0)) FILTER (WHERE COALESCE(pos, '?') <> 'GK') AS penalties_conceded_per_match_sum,
        SUM(dec_dribbles_per_90) FILTER (WHERE COALESCE(pos, '?') <> 'GK')                           AS dribbles_per_90_sum
    FROM xp GROUP BY str_match_id, str_team_id
),
g AS (
    SELECT o.str_match_id, o.str_team_id,
        o.dribble_woe_att AS dec_xi_dribble_woe_att, o.defended_woe_def AS dec_xi_defended_woe_def,
        o.dispossessed_share AS dec_xi_dispossessed_share, o.foul_committed_share AS dec_xi_foul_committed_share,
        o.foul_drawn_share_att AS dec_xi_foul_drawn_share_att, o.yellow_per_foul AS dec_xi_yellow_per_foul,
        o.fouls_committed_per_90_sum AS dec_xi_fouls_committed_per_90_sum,
        o.penalties_conceded_per_match_sum AS dec_xi_penalties_conceded_per_match_sum,
        o.dribbles_per_90_sum AS dec_xi_dribbles_per_90_sum,
        q.dribble_woe_att AS dec_opp_dribble_woe_att, q.defended_woe_def AS dec_opp_defended_woe_def,
        q.dispossessed_share AS dec_opp_dispossessed_share, q.foul_committed_share AS dec_opp_foul_committed_share,
        q.foul_drawn_share_att AS dec_opp_foul_drawn_share_att, q.yellow_per_foul AS dec_opp_yellow_per_foul,
        q.fouls_committed_per_90_sum AS dec_opp_fouls_committed_per_90_sum,
        q.penalties_conceded_per_match_sum AS dec_opp_penalties_conceded_per_match_sum,
        q.dribbles_per_90_sum AS dec_opp_dribbles_per_90_sum,
        o.dribble_woe_att - q.defended_woe_def      AS dec_edge_dribble,
        o.defended_woe_def - q.dribble_woe_att      AS dec_edge_defended,
        o.foul_drawn_share_att - q.foul_committed_share AS dec_edge_foul_draw
    FROM own o LEFT JOIN own q ON q.str_match_id = o.str_match_id AND q.str_team_id = o.str_opponent_id
),
rm AS (
    SELECT b.str_match_id, m.referee_id, mc.str_team_id, mc.int_is_home, mc.y
    FROM intermediate.intermediate_whoscored_match_bridge b
    JOIN silver.stg_whoscored_match_meta m ON m.ws_match_id = b.str_ws_match_id
    JOIN mc ON mc.str_match_id = b.str_match_id
    WHERE m.referee_id IS NOT NULL
),
rl AS (
    SELECT r.str_match_id, r.str_team_id, MAX(p.str_season) AS ps
    FROM rm r JOIN gold.gold_referee_profile p
      ON p.str_referee_id = r.referee_id AND CAST(SUBSTR(p.str_season, 1, 4) AS INTEGER) BETWEEN r.y - 2 AND r.y - 1
    GROUP BY ALL
),
rf AS (
    SELECT r.str_match_id, r.str_team_id,
        p.dec_yellow_net AS dec_ref_yellow_net, p.dec_red_net AS dec_ref_red_net, p.dec_fouls_net AS dec_ref_fouls_net,
        p.dec_penalties_net AS dec_ref_penalties_net,
        CASE WHEN r.int_is_home = 1 THEN p.dec_home_goal_diff_net ELSE -p.dec_home_goal_diff_net END AS dec_ref_home_goal_diff_net,
        CASE WHEN r.int_is_home = 1 THEN p.dec_home_yellow_diff_net ELSE -p.dec_home_yellow_diff_net END AS dec_ref_home_yellow_diff_net,
        CAST(p.int_n_matches AS DOUBLE) AS dec_ref_n_matches
    FROM rm r JOIN rl ON rl.str_match_id = r.str_match_id AND rl.str_team_id = r.str_team_id
    JOIN gold.gold_referee_profile p ON p.str_referee_id = r.referee_id AND p.str_season = rl.ps
)
SELECT mc.str_match_id, mc.str_team_id, {cols}
FROM mc
LEFT JOIN g  ON g.str_match_id = mc.str_match_id AND g.str_team_id = mc.str_team_id
LEFT JOIN rf ON rf.str_match_id = mc.str_match_id AND rf.str_team_id = mc.str_team_id
"""


def main():
    cfg, _ = mc.load_configs()
    con = duckdb.connect(str(ROOT / cfg["paths"]["duckdb"]), read_only=True)
    gcols = XI + ["dec_opp_" + c[7:] for c in XI] + EDGES
    df = con.sql(SQL.format(cols=", ".join(gcols + REF).replace("dec_", "dec_"))).df()
    con.close()
    out = ROOT / "reports" / "chunks"
    out.mkdir(parents=True, exist_ok=True)
    df[KEY + gcols].to_csv(out / "ground_all.csv", index=False)
    df[KEY + REF].to_csv(out / "referee.csv", index=False)
    df[KEY + gcols + REF].to_csv(out / "ground_ref.csv", index=False)
    print(f"{len(df):,} lignes ; sol : {len(gcols)} colonnes (NaN {df[gcols].isna().mean().mean():.3f}) ; "
          f"arbitre : {len(REF)} colonnes (NaN {df[REF].isna().mean().mean():.3f})")


if __name__ == "__main__":
    main()
