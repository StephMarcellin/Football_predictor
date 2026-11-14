"""Tests de tools/trait_features.py (parts de types du onze, écart équipe - adversaire, absence de fuite) sur données synthétiques."""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pipelines"))
_spec = importlib.util.spec_from_file_location("trait_features", ROOT / "tools/trait_features.py")
tf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tf)

FAMS = {"f1": ["a"], "f2": ["b"]}


def _prof():
    # joueurs 1-2 de l'équipe A (types 0 et 1), 3-4 de l'équipe B (types 1 et 1) ; profil de la fenêtre terminant en 2019
    rows = [("1", 2019, 0, 1.0, 0.0), ("2", 2019, 1, 3.0, 0.0), ("3", 2019, 1, -1.0, 2.0), ("4", 2019, 1, -3.0, 2.0),
            ("1", 2020, 1, 99.0, 99.0)]    # fenêtre 2020 : ne doit PAS servir pour un match de 2020
    return pd.DataFrame(rows, columns=["str_player_id", "py", "int_type_global", "zf_f1", "zf_f2"])


def _xi():
    rows = [("m", "A", "1", "B", 2020), ("m", "A", "2", "B", 2020), ("m", "B", "3", "A", 2020), ("m", "B", "4", "A", 2020)]
    return pd.DataFrame(rows, columns=["str_match_id", "str_team_id", "str_player_id", "str_opponent_id", "y"])


@pytest.fixture(scope="module")
def res():
    return tf.lineup_features(_xi(), _prof(), FAMS, 2)


def test_parts_de_types_equipe_adversaire_ecart(res):
    t = res[0].set_index("str_team_id")
    assert t.loc["A", "dec_xi_type_g0"] == 0.5 and t.loc["A", "dec_xi_type_g1"] == 0.5
    assert t.loc["A", "dec_opp_type_g1"] == 1.0 and t.loc["B", "dec_opp_type_g0"] == 0.5
    assert t.loc["A", "dec_edge_type_g0"] == pytest.approx(0.5) and t.loc["B", "dec_edge_type_g0"] == pytest.approx(-0.5)


def test_pas_de_fuite_la_fenetre_de_la_saison_du_match_est_ignoree(res):
    z = res[1].set_index("str_team_id")
    assert z.loc["A", "dec_xi_zf_f1"] == pytest.approx(2.0)      # moyenne de 1,0 et 3,0, pas 99
    assert z.loc["B", "dec_xi_zf_f2"] == pytest.approx(2.0)
