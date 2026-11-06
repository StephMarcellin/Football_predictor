"""
team_elo.py — Elo et classement AVANT chaque match, par (match, équipe). Étape Python entre deux phases dbt :
lit intermediate.intermediate_team_match_backbone, écrit machine_learning.elo_team_match (CREATE OR REPLACE),
lue par le modèle dbt gold_team_match_elo puis jointe aux marts 1N2.

Pourquoi Python : l'Elo est une récurrence séquentielle (chaque match met à jour deux notes), non exprimable en SQL pur.
Aucune fuite : valeurs figées avant la mise à jour par le résultat ; mises à jour en fin de journée ; un match non joué ne met rien à jour.
Paramètres figés avant mesure (non optimisés) : K=20, avantage du terrain 70 (0 si terrain neutre), multiplicateur d'écart de buts
(World Football Elo), départ 1500 (Big5) / 1400 (autres), régression de 25 % vers l'ancre de la division à chaque nouvelle saison.
Classement : points/match, différence de buts/match, rang en percentile, matchs joués, dans la même (compétition, saison), Big5 et D2 seulement.
Évalué (paliers 6 et 7) : gain de log-loss +0,0101 IC95 [+0,0074 ; +0,0130] sur la variante E, 7 saisons sur 7.

Lancement :  python pipelines/features/team_elo.py            (écrit la table)
             python pipelines/features/team_elo.py --dry-run  (calcule et contrôle, n'écrit rien)
"""
import argparse
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yaml
from loguru import logger

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)
DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]
OUT_SCHEMA, OUT_TABLE = "machine_learning", "elo_team_match"

K, HFA, REGRESS = 20.0, 70.0, 0.25
ANCHOR = {"Big5": 1500.0}
DEFAULT_ANCHOR = 1400.0
LEAGUE_CATS = ("Big5", "D2")
KEY = ["str_match_id", "str_team_id"]


def gmult(gd):
    return 1.0 if gd <= 1 else 1.5 if gd == 2 else (11.0 + gd) / 8.0


def build(rows):
    """rows : une ligne par (match, équipe). Retourne la liste des matchs (ordre chronologique) avec home/away/buts."""
    out = []
    for mid, g in rows.groupby("str_match_id", sort=False):
        r = g.iloc[0]
        if (g["str_venue"] == "Home").any():
            r = g[g["str_venue"] == "Home"].iloc[0]
            h, a, gh, ga, neutral = r.str_team_id, r.str_opponent_id, r.int_gf, r.int_ga, False
        elif (g["str_venue"] == "Away").any():
            r = g[g["str_venue"] == "Away"].iloc[0]
            h, a, gh, ga, neutral = r.str_opponent_id, r.str_team_id, r.int_ga, r.int_gf, False
        else:
            h, a, gh, ga, neutral = r.str_team_id, r.str_opponent_id, r.int_gf, r.int_ga, True
        out.append(dict(mid=mid, date=r.dt_date, season=r.str_season, league=r.str_league_source, cat=r.str_comp_category,
                        h=h, a=a, gh=gh, ga=ga, neutral=neutral))
    return pd.DataFrame(out).sort_values(["date", "mid"]).reset_index(drop=True)


def compute(rows):
    """Retourne (M, feats, F) : matchs triés, détail par match, et DataFrame des features par (match, équipe) présent dans rows."""
    rows = rows.copy()
    rows["int_gf"] = rows["int_gf"].astype("float64")
    rows["int_ga"] = rows["int_ga"].astype("float64")
    M = build(rows)
    elo, tier, last_season = {}, {}, {}
    table = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0]))        # (ligue, saison) -> équipe -> [pts, diff, buts, joués]
    feats = []

    def rating(team, cat, season):
        if team not in elo:
            tier[team] = ANCHOR.get(cat, DEFAULT_ANCHOR)
            elo[team] = tier[team]
        if cat in LEAGUE_CATS:
            tier[team] = ANCHOR.get(cat, DEFAULT_ANCHOR)
        if last_season.get(team) != season:                                # nouvelle saison pour l'équipe
            if team in last_season:
                elo[team] += REGRESS * (tier[team] - elo[team])
            last_season[team] = season
        return elo[team]

    for date, day in M.groupby("date", sort=True):
        pending = []
        for m in day.itertuples():
            eh, ea = rating(m.h, m.cat, m.season), rating(m.a, m.cat, m.season)
            dr = eh - ea + (0.0 if m.neutral else HFA)
            exp_h = 1.0 / (1.0 + 10 ** (-dr / 400.0))
            st = None
            if m.cat in LEAGUE_CATS:
                t = table[(m.league, m.season)]
                order = sorted(t, key=lambda x: (-t[x][0], -t[x][1], -t[x][2]))
                n = max(len(order) - 1, 1)

                def s(team):
                    p, d, g, j = t[team] if team in t else (0, 0, 0, 0)
                    rk = (order.index(team) / n) if team in order else np.nan
                    return (p / j if j else np.nan, d / j if j else np.nan, rk, j)

                st = (s(m.h), s(m.a))
            feats.append((m, eh, ea, exp_h, st))
            if not np.isnan(m.gh):
                pending.append((m, exp_h))
        for m, exp_h in pending:                                           # mises à jour APRÈS toute la journée
            gh, ga = m.gh, m.ga
            sc = 1.0 if gh > ga else 0.5 if gh == ga else 0.0
            d = K * gmult(abs(gh - ga)) * (sc - exp_h)
            elo[m.h] += d
            elo[m.a] -= d
            if m.cat in LEAGUE_CATS:
                t = table[(m.league, m.season)]
                for team, f, c in ((m.h, gh, ga), (m.a, ga, gh)):
                    r = t[team]
                    r[0] += 3 if f > c else 1 if f == c else 0
                    r[1] += f - c
                    r[2] += f
                    r[3] += 1

    nan = (np.nan, np.nan, np.nan, np.nan)
    out = []
    for m, eh, ea, exp_h, st in feats:
        sh, sa = st if st else (nan, nan)
        for team, e1, e2, e_exp, s1, s2 in ((m.h, eh, ea, exp_h, sh, sa), (m.a, ea, eh, 1 - exp_h, sa, sh)):
            out.append(dict(str_match_id=m.mid, str_team_id=team, dec_elo=e1, dec_opp_elo=e2, dec_elo_diff=e1 - e2,
                            dec_elo_win_exp=e_exp, dec_s_ppg=s1[0], dec_s_gd_pg=s1[1], dec_s_rank_pct=s1[2], int_s_played=s1[3],
                            dec_opp_s_ppg=s2[0], dec_opp_s_gd_pg=s2[1], dec_opp_s_rank_pct=s2[2], int_opp_s_played=s2[3]))
    F = pd.DataFrame(out)
    F["dec_s_ppg_diff"] = F["dec_s_ppg"] - F["dec_opp_s_ppg"]
    F = F.merge(rows[KEY].drop_duplicates(), on=KEY, how="inner")           # uniquement les (match, équipe) du backbone
    return M, feats, F


def main(write=True):
    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.sql("""select str_match_id, str_team_id, str_opponent_id, dt_date, str_venue, str_season, str_league_source,
                      str_comp_category, int_gf, int_ga from intermediate.intermediate_team_match_backbone""").df()
    con.close()
    if rows.duplicated(KEY).any():
        raise RuntimeError("intermediate_team_match_backbone : clé (str_match_id, str_team_id) non unique.")
    M, feats, F = compute(rows)
    if F.duplicated(KEY).any():
        raise RuntimeError("elo_team_match : clé (str_match_id, str_team_id) non unique.")
    logger.info(f"{len(M):,} matchs, {len(F):,} lignes (match, équipe) ; Elo moyen {F.dec_elo.mean():.0f}")
    if not write:
        logger.info("--dry-run : aucune écriture.")
        return F
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA}")
    con.execute(f"CREATE OR REPLACE TABLE {OUT_SCHEMA}.{OUT_TABLE} AS SELECT * FROM F")
    n = con.execute(f"SELECT count(*) FROM {OUT_SCHEMA}.{OUT_TABLE}").fetchone()[0]
    con.close()
    logger.info(f"{OUT_SCHEMA}.{OUT_TABLE} écrite : {n:,} lignes.")
    return F


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Elo et classement avant match -> machine_learning.elo_team_match")
    ap.add_argument("--dry-run", action="store_true", help="calcule et contrôle sans écrire dans la base")
    a = ap.parse_args()
    main(write=not a.dry_run)
