"""Tests des fonctions pures de tools/sector_validation.py : mise en regard des flancs (miroir) et priors glissants."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("lightgbm")
pytest.importorskip("duckdb")

_spec = importlib.util.spec_from_file_location(
    "sector_validation", Path(__file__).resolve().parents[2] / "tools/sector_validation.py")
sv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sv)

A_THREAT = {"L": 1.0, "C": 2.0, "R": 3.0}      # menace créée par A sur ses flancs, constante
B_THREAT = {"L": 10.0, "C": 20.0, "R": 30.0}   # menace créée par B sur ses flancs, constante


def _frame(n_matches=12):
    rows = []
    for i in range(n_matches):
        date = pd.Timestamp("2020-08-01") + pd.Timedelta(days=7 * i)
        for team, opp, threat, home in (("A", "B", A_THREAT, 1), ("B", "A", B_THREAT, 0)):
            for flank, y in threat.items():
                rows.append(dict(str_match_id=f"m{i}", str_team_id=team, str_opponent_id=opp, flank=flank, y=y,
                                 dt_date=date, str_season="2020-2021", str_league_source="Ligue 1", int_is_home=home))
    return pd.DataFrame(rows)


def test_la_defense_en_face_est_le_flanc_miroir_de_l_adversaire():
    d = sv.add_priors(_frame())
    last = d[d.str_match_id == "m11"].set_index(["str_team_id", "flank"])
    # A attaque sur son flanc L : il fait face au flanc R de B ; B y concède la menace créée par A sur son L (1,0)
    assert last.loc[("A", "L"), "att_prior"] == pytest.approx(1.0)
    assert last.loc[("A", "L"), "opp_def_prior"] == pytest.approx(1.0)
    # B attaque sur son flanc R : fait face au flanc L de A, qui concède la menace créée par B sur son R (30)
    assert last.loc[("B", "R"), "opp_def_prior"] == pytest.approx(30.0)
    # le flanc central se regarde lui-même
    assert last.loc[("A", "C"), "opp_def_prior"] == pytest.approx(2.0)


def test_pas_de_prior_sans_historique_et_pas_de_fuite_du_match_courant():
    d = sv.add_priors(_frame())
    first = d[d.str_match_id == "m0"]
    assert first.att_prior.isna().all()
    # un match unique différent des autres ne change pas son propre prior
    f = _frame()
    f.loc[(f.str_match_id == "m11") & (f.str_team_id == "A") & (f.flank == "L"), "y"] = 99.0
    d2 = sv.add_priors(f).set_index(["str_match_id", "str_team_id", "flank"])
    assert d2.loc[("m11", "A", "L"), "att_prior"] == pytest.approx(1.0)


def test_nanmean_rows_ignore_les_nan_sans_avertir():
    a = np.array([[1.0, np.nan], [3.0, np.nan]])
    out = sv._nanmean_rows(a)
    assert out[0] == 2.0 and np.isnan(out[1])
