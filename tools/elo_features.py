"""
elo_features.py — palier 6. Calcule, pour chaque (match, équipe), l'Elo et le classement AVANT le match (aucune fuite :
les valeurs sont figées avant la mise à jour par le résultat du match, et un match non joué ne met rien à jour).
Source : intermediate.intermediate_team_match_backbone (lecture seule). Sortie : reports/chunks/elo_features.csv
Paramètres figés avant mesure : K=20, avantage du terrain 70 (0 si terrain neutre), multiplicateur d'écart de buts
(World Football Elo), départ 1500 (Big5) / 1400 (autres), régression de 25 % vers l'ancre de la division à chaque nouvelle saison.
Classement : points, différence de buts, rang en percentile, matchs joués, dans la même (compétition, saison), Big5 et D2 seulement.
"""
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
import ml_common as mc  # noqa: E402

K, HFA, REGRESS = 20.0, 70.0, 0.25
ANCHOR = {"Big5": 1500.0}
DEFAULT_ANCHOR = 1400.0
LEAGUE_CATS = ("Big5", "D2")


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


def main():
    cfg, _ = mc.load_configs()
    con = duckdb.connect(str(ROOT / cfg["paths"]["duckdb"]), read_only=True)
    rows = con.sql("""select str_match_id, str_team_id, str_opponent_id, dt_date, str_venue, str_season, str_league_source,
                      str_comp_category, int_gf, int_ga from intermediate.intermediate_team_match_backbone""").df()
    con.close()
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
        if last_season.get(team) != season:                                  # nouvelle saison pour l'équipe
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
        for m, exp_h in pending:                                             # mises à jour APRÈS toute la journée
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
        for team, opp, e1, e2, e_exp, s1, s2 in ((m.h, m.a, eh, ea, exp_h, sh, sa), (m.a, m.h, ea, eh, 1 - exp_h, sa, sh)):
            out.append(dict(str_match_id=m.mid, str_team_id=team, dec_elo=e1, dec_opp_elo=e2, dec_elo_diff=e1 - e2,
                            dec_elo_win_exp=e_exp, dec_s_ppg=s1[0], dec_s_gd_pg=s1[1], dec_s_rank_pct=s1[2], int_s_played=s1[3],
                            dec_opp_s_ppg=s2[0], dec_opp_s_gd_pg=s2[1], dec_opp_s_rank_pct=s2[2], int_opp_s_played=s2[3]))
    F = pd.DataFrame(out)
    F["dec_s_ppg_diff"] = F["dec_s_ppg"] - F["dec_opp_s_ppg"]
    dest = ROOT / "reports" / "chunks" / "elo_features.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    F.to_csv(dest, index=False)
    # contrôles
    P = M.assign(eh=[f[1] for f in feats], ea=[f[2] for f in feats], pe=[f[3] for f in feats])
    P = P[(P.cat == "Big5") & P.gh.notna()]
    P["y"] = np.where(P.gh > P.ga, 0, np.where(P.gh == P.ga, 1, 2))
    best = np.where(P.pe >= 0.5, 0, 2)
    print(f"{len(M):,} matchs, {len(F):,} lignes -> {dest}")
    print("Big5 : Elo moyen par saison:", P.groupby("season")[["eh"]].mean().round(0).T.to_string(header=False)[:200])
    print(f"Big5 : le favori Elo (avec avantage du terrain) gagne {(best == P.y).mean():.3f} des matchs (n={len(P):,}) ; 3 issues, nuls inclus")
    print("part de NaN classement (D2/Big5 attendu faible, coupes élevé):", round(float(F.dec_s_ppg.isna().mean()), 3))


if __name__ == "__main__":
    main()
