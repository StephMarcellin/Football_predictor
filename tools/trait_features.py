"""
trait_features.py — mesure du gain 1N2 des types de joueurs par traits (palier 19). Écrit deux CSV de features externes
pour `tools/rolling_chunk.py --extra`, jointes sur (match, équipe) :
  reports/chunks/trait_types.csv : part du onze de départ (gardien exclu) dans chacun des 8 types GLOBAUX, pour l'équipe,
                                   pour l'adversaire, et l'écart (24 colonnes)
  reports/chunks/trait_z.csv     : moyenne du onze sur chacune des 8 familles de capacités (z-scores continus, moyenne des
                                   z de la famille), équipe, adversaire, écart (24 colonnes) : l'équivalent continu
Sans fuite : profil et type lus sur la dernière fenêtre dont la saison de fin est s-1 (à défaut s-2). Un joueur sans
profil (moins de 450 minutes sur la fenêtre, nouveau) n'entre pas dans les parts : elles sont calculées sur les joueurs typés.
Entrées : table machine_learning.player_trait_types (ou --types parquet) et profils (--behaviour parquet, défaut table gold).
Usage : python tools/trait_features.py [--types t.parquet] [--behaviour b.parquet]
"""
import argparse
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "pipelines" / "features"))
import ml_common as mc  # noqa: E402

KEY = ["str_match_id", "str_team_id"]
XI_SQL = """
SELECT DISTINCT l.str_match_id, l.str_team_id, l.str_player_id, m.str_opponent_id,
       CAST(SUBSTR(m.str_season, 1, 4) AS INTEGER) AS y
FROM intermediate.intermediate_whoscored_lineup_period l
JOIN gold.gold_team_match m ON m.str_match_id = l.str_match_id AND m.str_team_id = l.str_team_id
WHERE l.int_start_minute = 0 AND l.str_match_id IS NOT NULL
"""


def family_profiles(types, behaviour, families, n_global):
    """Une ligne (joueur, saison de fin) : type global et moyenne des z de chaque famille."""
    b = behaviour[["str_player_id", "str_season"] + [c for c in behaviour.columns if c.startswith("dec_z_")]]
    p = types[["str_player_id", "str_season", "int_type_global"]].merge(b, on=["str_player_id", "str_season"], how="left")
    for fam, feats in families.items():
        p[f"zf_{fam}"] = p[[f"dec_z_{f}" for f in feats]].mean(axis=1)
    p["py"] = p["str_season"].str[:4].astype(int)
    return p[["str_player_id", "py", "int_type_global"] + [f"zf_{f}" for f in families]]


def lineup_features(xi, prof, families, n_global):
    """Moyennes du onze (types globaux en parts, familles en z moyen), équipe / adversaire / écart."""
    x = xi.copy()
    x["py1"], x["py2"] = x["y"] - 1, x["y"] - 2
    cols = [c for c in prof.columns if c not in ("str_player_id", "py")]
    a = x.merge(prof.rename(columns={"py": "py1"}), on=["str_player_id", "py1"], how="left")
    b = x[["str_match_id", "str_team_id", "str_player_id", "py2"]].merge(
        prof.rename(columns={"py": "py2"}), on=["str_player_id", "py2"], how="left")
    miss = a["int_type_global"].isna().values
    for c in cols:
        a.loc[miss, c] = b[c].values[miss]
    for k in range(n_global):
        a[f"g{k}"] = (a["int_type_global"] == k).astype(float).where(a["int_type_global"].notna())
    g = [f"g{k}" for k in range(n_global)]
    z = [f"zf_{f}" for f in families]
    own = a.groupby(KEY)[g + z].mean().reset_index()
    own["str_opponent_id"] = a.groupby(KEY).str_opponent_id.first().values
    q = own.drop(columns="str_opponent_id").rename(columns={c: "opp_" + c for c in g + z})
    q = q.rename(columns={"str_team_id": "str_opponent_id"})
    m = own.merge(q, on=["str_match_id", "str_opponent_id"], how="left")
    out_t, out_z = m[KEY].copy(), m[KEY].copy()
    for c in g:
        out_t[f"dec_xi_type_{c}"], out_t[f"dec_opp_type_{c}"] = m[c], m["opp_" + c]
        out_t[f"dec_edge_type_{c}"] = m[c] - m["opp_" + c]
    for c in z:
        out_z[f"dec_xi_{c}"], out_z[f"dec_opp_{c}"] = m[c], m["opp_" + c]
        out_z[f"dec_edge_{c}"] = m[c] - m["opp_" + c]
    return out_t, out_z


def main(types=None, behaviour=None):
    import trait_types as tt
    cfg, _ = mc.load_configs()
    con = duckdb.connect(str(ROOT / cfg["paths"]["duckdb"]), read_only=True)
    t = pd.read_parquet(types) if types else con.sql("SELECT * FROM machine_learning.player_trait_types").df()
    bh = pd.read_parquet(behaviour) if behaviour else con.sql("SELECT * FROM gold.gold_player_behaviour_profile").df()
    xi = con.sql(XI_SQL).df()
    con.close()
    out_t, out_z = lineup_features(xi, family_profiles(t, bh, tt.FAMILIES, tt.N_GLOBAL), tt.FAMILIES, tt.N_GLOBAL)
    d = ROOT / "reports" / "chunks"
    d.mkdir(parents=True, exist_ok=True)
    out_t.to_csv(d / "trait_types.csv", index=False)
    out_z.to_csv(d / "trait_z.csv", index=False)
    print(f"{len(out_t):,} lignes ; types : {out_t.shape[1] - 2} colonnes (NaN {out_t.iloc[:, 2:].isna().mean().mean():.3f}) ; "
          f"z : {out_z.shape[1] - 2} colonnes (NaN {out_z.iloc[:, 2:].isna().mean().mean():.3f})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Features 1N2 des types par traits")
    ap.add_argument("--types", default=None)
    ap.add_argument("--behaviour", default=None)
    a = ap.parse_args()
    main(a.types, a.behaviour)
