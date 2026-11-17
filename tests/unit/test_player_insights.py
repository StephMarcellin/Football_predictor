"""Tests de app/player_insights.py : fiabilité, regroupement, percentiles, points forts, radar, netteté des types, synthèse."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import app.player_data as pdta  # noqa: E402
import app.player_insights as ins  # noqa: E402

NAMES = ins.feature_names()
SEASONS = ["2019-2020", "2020-2021", "2021-2022", "2022-2023"]


def make_profiles(n=300, noisy=("shots",), twins=(("tackles", "interceptions"),), seed=0):
    """n joueurs de champ sur 4 saisons consécutives. Chaque variable est persistante (z = niveau du joueur + petit bruit), sauf
    les variables `noisy` (bruit pur) ; pour les `twins`, la seconde variable recopie la première."""
    rng = np.random.default_rng(seed)
    level = {k: rng.normal(size=n) for k in NAMES}
    rows = []
    for s in SEASONS:
        d = pd.DataFrame({"str_player_id": [str(i) for i in range(n)], "str_season": s, "str_macro_role": "ST",
                          "str_league_source": "L1", "bool_is_peer": True, "int_minutes": 2000, "int_n_matches": 30})
        for k in NAMES:
            z = rng.normal(size=n) if k in noisy else level[k] + rng.normal(0, 0.2, n)
            d[f"dec_z_{k}"] = z
            d[f"dec_{k}"] = 1.0 + 0.5 * z
        for a, b in twins:
            d[f"dec_z_{b}"] = d[f"dec_z_{a}"] + rng.normal(0, 0.01, n)
        rows.append(d)
    return pd.concat(rows, ignore_index=True)


@pytest.fixture(scope="module")
def prof():
    return make_profiles()


def test_fiabilite_persistante_contre_bruit(prof):
    rel = ins.feature_reliability(prof)
    assert rel["aerial_duels"] > 0.8 and abs(rel["shots"]) < 0.3
    assert len(rel) == len(NAMES)


def test_regroupement_des_variables_jumelles(prof):
    g = ins.feature_groups(prof)
    assert g["tackles"] == g["interceptions"] and g["tackles"] != g["aerial_duels"]


def test_percentile_et_valeurs(prof):
    d = prof[prof.str_season == SEASONS[-1]].copy()
    row = d.iloc[0].copy()
    row["dec_z_aerial_duels"] = d["dec_z_aerial_duels"].max() + 1
    row["dec_aerial_duels"] = 9.0
    tb = ins.variable_table(prof, row, ins.feature_reliability(prof), ins.feature_groups(prof)).set_index("name")
    assert tb.loc["aerial_duels", "percentile"] > 99 and tb.loc["aerial_duels", "valeur"] == 9.0
    assert abs(tb.loc["aerial_duels", "mediane"] - 1.0) < 0.2
    assert len(tb) == len(NAMES)
    petit = prof[prof.str_season == SEASONS[-1]].head(5)                     # moins de 20 pairs : pas de percentile
    assert ins.variable_table(petit, petit.iloc[0], ins.feature_reliability(prof), ins.feature_groups(prof)).percentile.isna().all()


def test_points_forts_fiables_et_dedoublonnes(prof):
    rel, grp = ins.feature_reliability(prof), ins.feature_groups(prof)
    d = prof[prof.str_season == SEASONS[-1]].copy()
    row = d.iloc[0].copy()
    for k in ("shots", "tackles", "interceptions"):                           # bruit pur / jumelles : tous très hauts
        row[f"dec_z_{k}"] = 5.0
    tb = ins.variable_table(prof, row, rel, grp)
    forts, _ = ins.headline(tb)
    assert "shots" not in set(forts.name)                                      # peu fiable : écartée
    assert len({"tackles", "interceptions"} & set(forts.name)) <= 1            # un seul représentant du groupe
    assert (forts.percentile >= 75).all()


def test_radar_huit_aptitudes(prof):
    row = prof[prof.str_season == SEASONS[-1]].iloc[0]
    ax = ins.axis_table(prof, row, ins.feature_reliability(prof))
    assert len(ax) == 8 and ax.percentile.between(0, 100).all()
    assert all(n in NAMES for _, (_, names) in ins.AXES.items() for n in names)


def test_nettete_des_types():
    rng = np.random.default_rng(1)
    p = make_profiles(n=400, seed=3)
    for fam in list(pdta.tt.FAMILIES) + ["global"]:
        p[f"int_type_{fam}"] = rng.integers(0, 3, len(p))                      # étiquettes au hasard : types flous
    cols = [f"dec_z_{n}" for n in pdta.tt.FAMILIES["aerien"]]
    lab = rng.integers(0, 3, len(p))
    for c in cols:                                                             # famille aérienne : trois blobs très séparés
        p[c] = (lab - 1) * 3.0 + rng.normal(0, 0.3, len(p))   # -3, 0, 3 : dans la limite d écrêtage (±4)
    p["int_type_aerien"] = lab
    q = ins.type_quality(p)
    assert (q[q.family == "aerien"].nettete == "nette").all()
    assert (q[q.family == "finition"].nettete == "floue").all()
    assert ins.sharpness(q, "aerien", 1) == "nette" and ins.sharpness(q, "aerien", 9) == "n.c."


def test_phrase_de_synthese():
    forts = pd.DataFrame([{"Variable": "Tirs cadrés", "percentile": 97.0}])
    faibles = pd.DataFrame([{"Variable": "Duels aériens", "percentile": 3.0}])
    types = pd.DataFrame([{"Famille": "Finition", "Type": "Finisseur de surface", "Ressemblance": "forte"}])
    q = pd.DataFrame([{"family": "finition", "type": 2, "part": 0.26, "nettete": "moyenne"}])
    s = ins.summary_sentence(pd.Series({"int_type_finition": 2}), "Avant-centre", forts, faibles, types, q, None)
    assert s.startswith("Avant-centre — finisseur de surface.") and "tirs cadrés (percentile 97)" in s and "duels aériens (percentile 3)" in s
    vide = ins.summary_sentence(pd.Series({"int_type_finition": 2}), "Ailier", forts.iloc[0:0], faibles.iloc[0:0], types.iloc[0:0], q, None)
    assert "Aucune variable fiable" in vide
