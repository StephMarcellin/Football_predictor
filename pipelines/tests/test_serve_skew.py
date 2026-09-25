"""
test_serve_skew.py — Garde-fou anti train/serve skew du chemin de service.
Rejoue serve_features sur un échantillon de matchs et vérifie que les features
de compo recalculées collent au mart (produit par dbt). Écart toléré : 1e-9.
Si un modèle dbt change sans que serve_features suive, ce test casse → alerte.
Nommage : noms refondus (préfixe de type en tête), ids en VARCHAR.
"""
import math
import sys
from pathlib import Path

import duckdb
import pytest
import yaml

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
sys.path.insert(0, str(ROOT / "pipelines"))
import serve_features as sf  # noqa: E402

with open(ROOT / "config.yaml", encoding="utf-8") as _f:
    DB = ROOT / yaml.safe_load(_f)["paths"]["duckdb"]
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
        select distinct l.str_match_id from intermediate.int_whoscored_lineup l
        where l.int_start_minute=0
          and l.str_match_id in (select str_match_id from marts.mart_1n2)
        using sample {N_MATCHS} rows""").fetchall()]


def test_serve_features_no_skew(con):
    mismatches = []
    for mid in _sample(con):
        teams = [r[0] for r in con.execute(
            "select distinct str_team_id from marts.mart_1n2 where str_match_id=? order by str_team_id",
            [mid]).fetchall()]
        if len(teams) != 2:
            continue
        A, B = teams
        xi = {t: [r[0] for r in con.execute(
            "select distinct str_player_id from intermediate.int_whoscored_lineup "
            "where str_match_id=? and str_team_id=? and int_start_minute=0", [mid, t]).fetchall()]
            for t in teams}
        gk = {t: con.execute(
            "select str_player_id from intermediate.int_whoscored_player_match "
            "where str_match_id=? and str_team_id=? and str_position='GK' and bool_is_first_eleven "
            "qualify row_number() over(partition by str_match_id,str_team_id order by str_player_id)=1",
            [mid, t]).fetchone() for t in teams}
        if any(g is None for g in gk.values()):
            continue
        compo = {t: {"xi": xi[t], "gk": gk[t][0]} for t in teams}
        # 4-tuples (team, player, grid_vertical, grid_horizontal) attendus par serve_features
        xi_pos = con.execute(
            "select str_team_id, str_player_id, dec_grid_vertical, dec_grid_horizontal "
            "from intermediate.int_whoscored_lineup where str_match_id=? and int_start_minute=0",
            [mid]).fetchall()

        zonal = sf.zonal_features(con, mid, xi_pos)
        for t, opp in [(A, B), (B, A)]:
            season = con.execute(
                "select any_value(str_season) from marts.mart_1n2 where str_match_id=? and str_team_id=?",
                [mid, t]).fetchone()[0]
            feats = sf.match_compo_features(con, mid, t, opp, compo, season)
            feats.update(zonal.get(t, {}))
            stored = con.execute(
                f"select {', '.join(feats.keys())} from marts.mart_1n2 "
                f"where str_match_id=? and str_team_id=?", [mid, t]).fetchdf().iloc[0].to_dict()
            for k in feats:
                if not _close(feats[k], stored[k]):
                    mismatches.append((mid[:10], t, k, feats[k], stored[k]))

    assert not mismatches, f"{len(mismatches)} écarts, ex: {mismatches[:5]}"
