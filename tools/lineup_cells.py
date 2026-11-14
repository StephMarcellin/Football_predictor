"""Compo de départ en CELLULES (ligne x flanc) -> table longue (match, équipe, ligne, flanc).

Pour chaque équipe et chaque match : le onze de départ, chaque joueur décrit par son profil de comportement
(gold_player_behaviour_profile, fenêtre terminant en s-1 ou s-2 : jamais la saison du match), rangé dans une
cellule :
  · ligne : GK, DEF (CB, FB), MID, ATT (WIDE, ST), d'après la famille de poste du profil ;
  · flanc : L, C ou R dans le repère de l'équipe (L = côté des petits y), d'après la latéralité du profil
    (part de touches à droite - part à gauche, seuil 0,10, hypothèse) ; un gardien n'a pas de flanc (C).
Chaque cellule porte le nombre de joueurs et la moyenne de 14 descripteurs (z-scores contre les pairs).
Un joueur sans profil n'entre dans aucune moyenne (compté dans int_n_unknown, au niveau de l'équipe, ligne 'UNK').

Mise en regard : le flanc L de l'attaquant fait face au flanc R du défenseur (repères miroirs).

Usage : python tools/lineup_cells.py                      (écrit reports/chunks/lineup_cells.parquet)
        python tools/lineup_cells.py --behaviour "read_parquet('b.parquet')" --out x.parquet
"""
import argparse
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

DESCRIPTORS = ["shots", "xg_contribution", "key_passes", "xt_created", "g_dribbles_per_90", "crosses",
               "tackles", "interceptions", "aerial_win_rate", "clearances", "g_defended_woe_adj",
               "pass_accuracy", "pass_forward_share", "progressive_passes"]
SIDE_THRESHOLD = 0.10

SQL = """
WITH
mc AS (
    SELECT str_match_id, str_team_id, CAST(SUBSTR(str_season, 1, 4) AS INTEGER) AS y
    FROM gold.gold_team_match
),
xi AS (
    SELECT DISTINCT l.str_match_id, l.str_team_id, l.str_player_id, m.y
    FROM intermediate.intermediate_whoscored_lineup_period l
    JOIN mc m ON m.str_match_id = l.str_match_id AND m.str_team_id = l.str_team_id
    WHERE l.int_start_minute = 0 AND l.str_match_id IS NOT NULL
),
prof AS (
    SELECT *, CAST(SUBSTR(str_season, 1, 4) AS INTEGER) AS py,
        (dec_sec_def_right + dec_sec_mid_right + dec_sec_att_right
       - dec_sec_def_left - dec_sec_mid_left - dec_sec_att_left) AS lat
    FROM {behaviour}
),
lastp AS (
    SELECT x.str_match_id, x.str_team_id, x.str_player_id, MAX(p.py) AS py
    FROM xi x JOIN prof p ON p.str_player_id = x.str_player_id AND p.py BETWEEN x.y - 2 AND x.y - 1
    GROUP BY ALL
),
cell AS (
    SELECT x.str_match_id, x.str_team_id, x.str_player_id,
        CASE p.str_macro_role WHEN 'GK' THEN 'GK' WHEN 'CB' THEN 'DEF' WHEN 'FB' THEN 'DEF'
             WHEN 'MID' THEN 'MID' WHEN 'WIDE' THEN 'ATT' WHEN 'ST' THEN 'ATT' END AS str_line,
        CASE WHEN p.str_macro_role = 'GK' THEN 'C'
             WHEN p.lat > {side} THEN 'R' WHEN p.lat < -{side} THEN 'L' ELSE 'C' END AS str_flank,
        {desc_cols}
    FROM xi x
    LEFT JOIN lastp lp ON lp.str_match_id = x.str_match_id AND lp.str_team_id = x.str_team_id AND lp.str_player_id = x.str_player_id
    LEFT JOIN prof p ON p.str_player_id = x.str_player_id AND p.py = lp.py
)
SELECT str_match_id, str_team_id, COALESCE(str_line, 'UNK') AS str_line, COALESCE(str_flank, 'C') AS str_flank,
       COUNT(*) AS int_n_players, {mean_cols}
FROM cell
GROUP BY str_match_id, str_team_id, COALESCE(str_line, 'UNK'), COALESCE(str_flank, 'C')
"""


def build_sql(behaviour):
    desc = ", ".join(f"p.dec_z_{d} AS z_{d}" for d in DESCRIPTORS)
    means = ", ".join(f"AVG(z_{d}) AS dec_cell_{d}" for d in DESCRIPTORS)
    return SQL.format(behaviour=behaviour, side=SIDE_THRESHOLD, desc_cols=desc, mean_cols=means)


def main(behaviour="gold.gold_player_behaviour_profile", out=None, db=None):
    import yaml
    cfg = yaml.safe_load(open(ROOT / "config.yaml", encoding="utf-8"))
    con = duckdb.connect(str(db or ROOT / cfg["paths"]["duckdb"]), read_only=True)
    out = Path(out or ROOT / "reports" / "chunks" / "lineup_cells.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    df = con.sql(build_sql(behaviour)).df()
    con.close()
    df.to_parquet(out)
    unk = df[df.str_line == "UNK"].int_n_players.sum()
    print(f"{len(df):,} cellules, {df[['str_match_id', 'str_team_id']].drop_duplicates().shape[0]:,} équipes-matchs ; "
          f"joueurs sans profil : {unk:,} sur {df.int_n_players.sum():,} -> {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Compo de départ en cellules ligne x flanc")
    ap.add_argument("--behaviour", default="gold.gold_player_behaviour_profile")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    main(args.behaviour, args.out)
