"""Tests de tools/duo_matchup_test.py : mise en regard des coéquipiers et des adversaires (flanc miroir)."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("lightgbm")
pytest.importorskip("duckdb")

_spec = importlib.util.spec_from_file_location(
    "duo_matchup_test", Path(__file__).resolve().parents[2] / "tools/duo_matchup_test.py")
dm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dm)


def _row(team, opp, player, macro, side, base):
    r = dict(str_match_id="m1", str_team_id=team, str_opponent_id=opp, str_player_id=player, int_is_home=1,
             str_league_source="Ligue 1", y=2020, macro=macro, side=side, mins=90.0, int_n_shots=1,
             dec_xg_contribution=0.1, dec_xt_created=0.1)
    r.update({f"z_{n}": base for n in dm.ALLZ})
    return r


@pytest.fixture(scope="module")
def out():
    rows = [
        _row("T1", "T2", "A_left", "WIDE", "L", 1.0), _row("T1", "T2", "A_right", "WIDE", "R", 2.0),
        _row("T1", "T2", "A_st", "ST", "C", 3.0),
        _row("T1", "T2", "T1_fb_l", "FB", "L", 10.0), _row("T1", "T2", "T1_fb_r", "FB", "R", 20.0),
        _row("T1", "T2", "T1_mid", "MID", "C", 7.0),
        _row("T2", "T1", "T2_fb_l", "FB", "L", 100.0), _row("T2", "T1", "T2_fb_r", "FB", "R", 200.0),
        _row("T2", "T1", "T2_cb1", "CB", "L", 40.0), _row("T2", "T1", "T2_cb2", "CB", "R", 60.0),
    ]
    return dm.build(pd.DataFrame(rows)).set_index("str_player_id")


def test_un_attaquant_par_ligne(out):
    assert sorted(out.index[out.str_team_id == "T1"]) == ["A_left", "A_right", "A_st"]
    assert "T1_fb_l" not in out.index and "T2_fb_l" not in out.index


def test_arriere_du_meme_flanc_et_arriere_en_face(out):
    # l'ailier gauche de T1 joue avec l'arrière gauche de T1 (10) et fait face à l'arrière DROIT de T2 (200)
    assert out.loc["A_left", "fb_xt_created"] == 10.0 and out.loc["A_left", "ofb_g_defended_woe_adj"] == 200.0
    # l'ailier droit de T1 : arrière droit (20), face à l'arrière GAUCHE de T2 (100)
    assert out.loc["A_right", "fb_xt_created"] == 20.0 and out.loc["A_right", "ofb_g_defended_woe_adj"] == 100.0


def test_attaquant_axial_sans_arriere_associe_mais_avec_les_centraux(out):
    assert np.isnan(out.loc["A_st", "fb_xt_created"]) and np.isnan(out.loc["A_st", "ofb_tackles"])
    assert out.loc["A_st", "ocb_clearances"] == 50.0      # moyenne des deux défenseurs centraux adverses


def test_autres_attaquants_hors_soi_et_milieux(out):
    # A_left : autres attaquants = (2 + 3) / 2 ; milieux = 7
    assert out.loc["A_left", "att_shots"] == pytest.approx(2.5)
    assert out.loc["A_left", "mid_xt_created"] == 7.0
