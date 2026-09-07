"""
test_serve_skew.py — Garde-fou anti train/serve skew du chemin de service.

Rejoue serve_features sur un échantillon de matchs et vérifie que les 28 features
de compo recalculées collent au mart (produit par dbt). Écart toléré : 1e-9.
Si un modèle dbt change sans que serve_features suive, ce test casse → alerte.
"""
import math
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
import serve_features as sf  # noqa: E402

DB = ROOT / "db" / "football.duckdb"
N_MATCHS = 40
TOL = 1e-9


def _close(a, b):
    an = a is None or (isinstance(a, float) and math.isnan(a))
    bn = b is None or (isinstance(b, float) and math.isnan(b))
    if an and bn:
        return True
    if an or bn:
        return False
    return abs(a - b) < TOL


@pytest.fixture(scope="module")
def con():
    if not DB.exists():
        pytest.skip("base DuckDB absente")
    c = duckdb.connect(str(DB), read_only=True)
    yield c
    c.close()


def _sample(con):
    return [r[0] for r in con.execute(f"""
        select distinct l.match_id from intermediate.int_whoscored_lineup l
        where l.start_minute=0 and l.match_id in (select match_id from marts.mart_1n2)
        using sample {N_MATCHS} rows""").fetchall()]


def test_serve_features_no_skew(con):
    mismatches = []
    for mid in _sample(con):
        teams = [r[0] for r in con.execute(
            "select distinct team_id from marts.mart_1n2 where match_id=? order by team_id",
            [mid]).fetchall()]
        if len(teams) != 2:
            continue
        A, B = teams
        xi = {t: [r[0] for r in con.execute(
            "select distinct player_id from intermediate.int_whoscored_lineup "
            "where match_id=? and team_id=? and start_minute=0", [mid, t]).fetchall()]
            for t in teams}
        gk = {t: con.execute(
            "select player_id from intermediate.int_whoscored_player_match "
            "where match_id=? and team_id=? and position='GK' and is_first_eleven "
            "qualify row_number() over(partition by match_id,team_id order by player_id)=1",
            [mid, t]).fetchone() for t in teams}
        if any(g is None for g in gk.values()):
            continue
        compo = {t: {"xi": xi[t], "gk": gk[t][0]} for t in teams}
        xi_pos = con.execute(
            "select team_id,player_id,grid_horizontal from intermediate.int_whoscored_lineup "
            "where match_id=? and start_minute=0", [mid]).fetchall()

        zonal = sf.zonal_features(con, mid, xi_pos)
        for t, opp in [(A, B), (B, A)]:
            season = con.execute(
                "select any_value(season) from marts.mart_1n2 where match_id=? and team_id=?",
                [mid, t]).fetchone()[0]
            feats = sf.match_compo_features(con, mid, t, opp, compo, season)
            feats.update(zonal.get(t, {}))
            stored = con.execute(
                f"select {', '.join(feats.keys())} from marts.mart_1n2 "
                f"where match_id=? and team_id=?", [mid, t]).fetchdf().iloc[0].to_dict()
            for k in feats:
                if not _close(feats[k], stored[k]):
                    mismatches.append((mid[:10], t, k, feats[k], stored[k]))

    assert not mismatches, f"{len(mismatches)} écarts, ex: {mismatches[:5]}"
