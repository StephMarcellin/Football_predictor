"""
aerial_features.py — mesure du gain 1N2 des colonnes aériennes (palier 14). Écrit deux CSV de features externes
pour `tools/rolling_chunk.py --extra`, jointes sur (match, équipe), depuis gold.gold_team_match_aerial_lineup :
  reports/chunks/aerial_edges.csv : les 5 écarts face à l'adversaire (dec_edge_*)
  reports/chunks/aerial_all.csv   : 9 colonnes de la compo + les mêmes 9 pour l'adversaire (dec_opp_xi_*) + les 5 écarts
Usage : python tools/aerial_features.py
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
import ml_common as mc  # noqa: E402

KEY = ["str_match_id", "str_team_id"]
EDGES = ["dec_edge_height_mean", "dec_edge_att_height", "dec_edge_att_skill", "dec_edge_def_height", "dec_edge_def_skill"]
XI = ["dec_xi_height_mean", "dec_xi_height_top3", "dec_xi_height_def", "dec_xi_height_att", "dec_xi_woe_net_def_def",
      "dec_xi_woe_net_off_att", "dec_xi_duels_per_match_sum", "dec_xi_head_shots_setpiece_sum", "dec_xi_head_shots_open_sum"]


def main():
    cfg, _ = mc.load_configs()
    con = duckdb.connect(str(ROOT / cfg["paths"]["duckdb"]), read_only=True)
    g = con.sql(f"""select a.str_match_id, a.str_team_id, m.str_opponent_id, {', '.join('a.' + c for c in XI + EDGES)}
                    from gold.gold_team_match_aerial_lineup a
                    join gold.gold_team_match m on m.str_match_id = a.str_match_id and m.str_team_id = a.str_team_id""").df()
    con.close()
    o = g[["str_match_id", "str_team_id"] + XI].rename(columns={"str_team_id": "str_opponent_id", **{c: "dec_opp_" + c[4:] for c in XI}})
    g = g.merge(o, on=["str_match_id", "str_opponent_id"], how="left").drop(columns="str_opponent_id")
    out = ROOT / "reports" / "chunks"
    out.mkdir(parents=True, exist_ok=True)
    g[KEY + EDGES].to_csv(out / "aerial_edges.csv", index=False)
    cols = XI + ["dec_opp_" + c[4:] for c in XI] + EDGES
    g[KEY + cols].to_csv(out / "aerial_all.csv", index=False)
    print(f"{len(g):,} lignes ; edges : {len(EDGES)} colonnes ; all : {len(cols)} colonnes ; part de NaN moyenne {g[cols].isna().mean().mean():.3f}")


if __name__ == "__main__":
    main()
