"""Anti-fuite et cohérence de pipelines/features/team_elo.py (données synthétiques, sans base)."""
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pipelines" / "features"))
import team_elo  # noqa: E402


def _rows(matches):
    """matches : (id, date, home, away, gh, ga) -> deux lignes (match, équipe) comme le backbone."""
    out = []
    for mid, d, h, a, gh, ga in matches:
        for team, opp, venue, gf, gc in ((h, a, "Home", gh, ga), (a, h, "Away", ga, gh)):
            out.append(dict(str_match_id=mid, str_team_id=team, str_opponent_id=opp, dt_date=d, str_venue=venue,
                            str_season="2020-2021", str_league_source="Premier League", str_comp_category="Big5",
                            int_gf=gf, int_ga=gc))
    return pd.DataFrame(out)


def _feat(F, mid, team):
    return F[(F.str_match_id == mid) & (F.str_team_id == team)].iloc[0]


def test_feature_independante_du_resultat_du_match():
    base = [("m1", date(2020, 9, 1), "A", "B", 1, 0), ("m3", date(2020, 9, 20), "B", "C", 0, 0)]
    win = _rows(base + [("m2", date(2020, 9, 10), "A", "C", 3, 0)])
    loss = _rows(base + [("m2", date(2020, 9, 10), "A", "C", 0, 3)])
    Fw, Fl = team_elo.compute(win)[2], team_elo.compute(loss)[2]
    cols = [c for c in Fw.columns if c not in ("str_match_id", "str_team_id")]
    for team in ("A", "C"):                                   # le résultat de m2 ne doit pas changer les features de m2
        a, b = _feat(Fw, "m2", team)[cols], _feat(Fl, "m2", team)[cols]
        assert np.allclose(a.astype(float), b.astype(float), equal_nan=True)
    assert _feat(Fw, "m3", "C").dec_elo != _feat(Fl, "m3", "C").dec_elo   # mais m3 (après) en dépend


def test_match_non_joue_ne_met_rien_a_jour():
    rows = _rows([("m1", date(2020, 9, 1), "A", "B", 2, 0), ("m2", date(2020, 9, 8), "A", "C", np.nan, np.nan),
                  ("m3", date(2020, 9, 15), "A", "C", np.nan, np.nan)])
    F = team_elo.compute(rows)[2]
    assert _feat(F, "m2", "A").dec_elo == _feat(F, "m3", "A").dec_elo


def test_meme_journee_etat_fige():
    rows = _rows([("m1", date(2020, 9, 1), "A", "B", 5, 0), ("m2", date(2020, 9, 1), "A", "C", 0, 5)])
    F = team_elo.compute(rows)[2]
    assert _feat(F, "m1", "A").dec_elo == _feat(F, "m2", "A").dec_elo   # A n'a pas été mis à jour entre ses deux matchs du jour


def test_somme_elo_conservee_et_cle_unique():
    rows = _rows([("m1", date(2020, 9, 1), "A", "B", 1, 0), ("m2", date(2020, 9, 8), "B", "C", 2, 2)])
    F = team_elo.compute(rows)[2]
    assert not F.duplicated(["str_match_id", "str_team_id"]).any()
    assert abs(_feat(F, "m1", "A").dec_elo_win_exp + _feat(F, "m1", "B").dec_elo_win_exp - 1.0) < 1e-12
