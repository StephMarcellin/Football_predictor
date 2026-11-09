"""Tests de l'étape de clusters aériens (pipelines/features/aerial_clusters.py) sur profils synthétiques."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
pytest.importorskip("loguru")

SPEC = importlib.util.spec_from_file_location(
    "aerial_clusters", Path(__file__).resolve().parents[2] / "pipelines/features/aerial_clusters.py")
ac = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ac)

# centres (woe_off, woe_def, part offensive, duels/match, tirs de la tête, dégagements, taille) -> étiquette attendue
CENTRES = {
    "défenseur qui dégage":         (0.032, 0.018, 0.30, 2.09, 0.198, 0.262, 187),
    "avant-centre moyen":           (-0.010, -0.032, 0.743, 2.49, 0.162, 0.062, 187),
    "attaquant qui perd ses duels": (-0.086, -0.107, 0.658, 1.60, 0.148, 0.066, 177.5),
    "avant-centre de présence":     (0.048, 0.003, 0.785, 5.455, 0.133, 0.059, 188),
    "défenseur dominant":           (0.092, 0.080, 0.349, 2.90, 0.286, 0.217, 189),
    "faible présence aérienne":     (0.034, -0.042, 0.39, 1.48, 0.125, 0.168, 180),
}
SD = (0.01, 0.01, 0.03, 0.2, 0.02, 0.02, 2)


@pytest.fixture(scope="module")
def profils():
    rng = np.random.default_rng(1)
    rows = []
    for g, (lab, c) in enumerate(CENTRES.items()):
        for i in range(60):
            v = rng.normal(c, SD)
            rows.append(dict(str_player_id=f"{g}-{i}", str_season="2024-2025", int_n_duels=150,
                             truth=lab, **dict(zip(ac.FEATURES, v))))
    df = pd.DataFrame(rows)
    # profils à exclure : trop peu de duels, saison trop ancienne
    df = pd.concat([df, df.head(5).assign(str_player_id=lambda x: "peu" + x.str_player_id, int_n_duels=40),
                    df.head(5).assign(str_player_id=lambda x: "vieux" + x.str_player_id, str_season="2015-2016")])
    return df.reset_index(drop=True)


def test_retrouve_les_six_groupes_et_leurs_etiquettes(profils):
    out, cent = ac.fit_assign(profils)
    m = out.merge(profils[ac.KEY + ["truth"]], on=ac.KEY)
    assert set(out.str_cluster_label) == set(CENTRES)
    assert (m.str_cluster_label == m.truth).mean() > 0.95


def test_exclut_petits_volumes_et_anciennes_fenetres(profils):
    out, _ = ac.fit_assign(profils)
    assert len(out) == 360
    assert not out.str_player_id.str.startswith(("peu", "vieux")).any()
    assert not out.duplicated(ac.KEY).any()


def test_numerotation_stable_et_deterministe(profils):
    a, ca = ac.fit_assign(profils)
    b, _ = ac.fit_assign(profils.sample(frac=1, random_state=3).reset_index(drop=True))
    assert list(ca.int_cluster) == list(range(6))
    # le groupe 0 est celui qui gagne le plus ses duels (woe_off + woe_def)
    top = ca.assign(s=ca.dec_woe_off + ca.dec_woe_def).sort_values("s", ascending=False).iloc[0]
    assert top.int_cluster == 0 and top.str_cluster_label == "défenseur dominant"
    assert (a.sort_values(ac.KEY).int_cluster.values == b.sort_values(ac.KEY).int_cluster.values).all()
