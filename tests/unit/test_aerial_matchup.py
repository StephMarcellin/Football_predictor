"""Tests du step de confrontation aérienne (pipelines/features/aerial_matchup.py), données synthétiques."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
pytest.importorskip("loguru")

SPEC = importlib.util.spec_from_file_location(
    "aerial_matchup", Path(__file__).resolve().parents[2] / "pipelines/features/aerial_matchup.py")
am = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(am)

SEASONS = [f"{y}-{y + 1}" for y in range(2016, 2024)]    # 2016-17 a 2023-24
B_HDIFF, B_ATT, B_OPP = 0.5, 0.4, -0.4                   # effets vrais (taille en 10 cm, compétence en 10 points)


def _sigmoid(z):
    return 1 / (1 + np.exp(-z))


@pytest.fixture(scope="module")
def monde():
    rng = np.random.default_rng(3)
    n_pl = 120
    height = rng.normal(184, 6, n_pl).round()
    skill = rng.normal(0, 0.05, n_pl)                       # compétence nette (taux de victoire)
    profile = pd.DataFrame([dict(str_player_id=f"p{i}", str_season=s, dec_woe_net_off=skill[i], dec_woe_net_def=skill[i])
                            for i in range(n_pl) for s in SEASONS])
    rows = []
    for s in SEASONS:
        for k in range(3000):
            a, d = rng.choice(n_pl, 2, replace=False)
            z = -0.1 + B_HDIFF * (height[a] - height[d]) / 10 + B_ATT * skill[a] * 10 + B_OPP * skill[d] * 10
            rows.append(dict(str_match_id=f"{s}-{k}", str_team_id="1", int_event_id=1, int_opp_event_id=2, str_season=s,
                             str_player_id=f"p{a}", str_opp_player_id=f"p{d}",
                             str_ball_type=rng.choice(["corner", "centre", "longue_balle"]),
                             dec_x=rng.uniform(60, 100), int_expanded_minute=int(rng.integers(1, 95)),
                             str_zone="surface", str_cross_lane="centre",
                             won=int(rng.random() < _sigmoid(z)), own_h=height[a], opp_h=height[d]))
    return pd.DataFrame(rows), profile


def test_retrouve_les_effets_de_la_taille_et_des_competences(monde):
    duels, profile = monde
    out, coefs, metrics = am.fit_predict(am.build_features(duels, profile))
    c = coefs.set_index("str_feature")["dec_coef"]
    assert abs(c["hdiff"] - B_HDIFF) < 0.1
    assert c["woe_att"] > 0.2 and c["woe_opp"] < -0.2
    ll_ctx, ll_full = metrics["contexte"][0], metrics["complet"][0]
    assert ll_full < metrics["taille"][0] < ll_ctx


def test_sortie_cle_unique_probabilites_et_drapeaux(monde):
    duels, profile = monde
    out, _, _ = am.fit_predict(am.build_features(duels, profile))
    assert len(out) == len(duels)
    assert not out.duplicated(am.KEY).any()
    assert out.dec_p_attacker_win.between(0, 1).all()
    fitted = out.set_index("str_season").bool_is_fitted_season
    assert not fitted["2016-2017"].any() and fitted["2020-2021"].all()
    assert abs(out.dec_p_attacker_win.mean() - out.bool_attacker_won.mean()) < 0.01   # calibration moyenne


def test_taille_manquante_donne_ecart_vide_et_indicateur(monde):
    duels, profile = monde
    d = duels.copy()
    d.loc[d.index[:50], "opp_h"] = np.nan
    f = am.build_features(d, profile)
    assert (f.h_missing.iloc[:50] == 1).all() and (f.hdiff.iloc[:50] == 0).all()
    out, _, _ = am.fit_predict(f)
    assert out.dec_height_diff_cm.iloc[:50].isna().all() and out.dec_height_diff_cm.iloc[50:].notna().all()


def test_aucune_fuite_la_competence_vient_de_la_saison_precedente():
    duels = pd.DataFrame([dict(str_match_id="m", str_team_id="1", int_event_id=1, int_opp_event_id=2, str_season="2020-2021",
                               str_player_id="a", str_opp_player_id="b", str_ball_type="corner", dec_x=90.0,
                               int_expanded_minute=30, str_zone="surface", str_cross_lane="centre",
                               won=1, own_h=190.0, opp_h=180.0)])
    profile = pd.DataFrame([
        dict(str_player_id="a", str_season="2020-2021", dec_woe_net_off=0.2, dec_woe_net_def=0.0),   # même saison : à ignorer
        dict(str_player_id="a", str_season="2019-2020", dec_woe_net_off=0.05, dec_woe_net_def=0.0),
        dict(str_player_id="b", str_season="2021-2022", dec_woe_net_off=0.0, dec_woe_net_def=0.3),   # saison suivante : à ignorer
    ])
    f = am.build_features(duels, profile)
    assert f.woe_att.iloc[0] == pytest.approx(0.5) and f.has_prior_att.iloc[0] == 1
    assert f.woe_opp.iloc[0] == 0 and f.has_prior_opp.iloc[0] == 0
    assert f.hdiff.iloc[0] == pytest.approx(1.0)


def test_refuse_un_echantillon_trop_petit(monde):
    duels, profile = monde
    with pytest.raises(RuntimeError):
        am.fit_predict(am.build_features(duels.head(100), profile))
