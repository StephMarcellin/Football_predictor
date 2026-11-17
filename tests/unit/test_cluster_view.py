"""Tests de app/cluster_view.py (logique de l'onglet Clusters) sur données synthétiques."""
import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
pytest.importorskip("plotly")

import app.cluster_view as cv  # noqa: E402


def _points(n_per=60, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for t, (cx, cy) in enumerate([(-4, 0), (4, 0), (0, 5)]):
        for i in range(n_per):
            rows.append({"str_player_id": f"p{t}_{i}", "type": t, "x": cx + rng.normal(0, 1), "y": cy + rng.normal(0, 1),
                         "dist": rng.random(), "marge": rng.random(), "coeur": True})
    return pd.DataFrame(rows)


def test_clic_actif_seulement_jusqu_a_dix_par_cluster():
    assert cv.clickable(10) and cv.clickable(1)
    assert not cv.clickable(11) and not cv.clickable(50)


def test_pick_players_n_par_cluster_et_emboitement():
    d = _points()
    a, b = cv.pick_players(d, 5), cv.pick_players(d, 8)
    assert a.groupby("type").size().eq(5).all() and b.groupby("type").size().eq(8).all()
    assert set(a.str_player_id) <= set(b.str_player_id)          # augmenter n n'enlève personne
    assert cv.pick_players(d, 5).str_player_id.tolist() == a.str_player_id.tolist()  # reproductible
    assert cv.pick_players(d, 500).groupby("type").size().eq(60).all()  # plafonné à l'effectif


def test_pick_players_typiques_et_filtres():
    d = _points()
    t = cv.pick_players(d, 3, "typical")
    for k, g in t.groupby("type"):
        assert g.dist.max() <= d[d.type == k].dist.nlargest(len(d[d.type == k]) - 3).min() + 1e-12
    only = cv.pick_players(d, 5, allowed_ids=[f"p1_{i}" for i in range(20)], types=[1])
    assert set(only.type) == {1} and len(only) == 5


def test_ellipse_contient_la_part_demandee():
    d = _points(500)
    for k, g in d.groupby("type"):
        x, y = cv.ellipse(g.x, g.y)
        assert len(x) == len(y) and np.isfinite(x).all()
    assert cv.share_in_ellipse(d) == pytest.approx(0.8, abs=0.02)
    assert cv.ellipse([1, 2], [1, 2]) is None


def test_melange_faible_si_clusters_distincts_fort_sinon():
    d = _points(300)
    assert cv.overlap_share(d) < 0.02
    m = d.assign(x=np.random.default_rng(1).normal(size=len(d)), y=np.random.default_rng(2).normal(size=len(d)))
    assert cv.overlap_share(m) > 0.4


def test_figure_et_retour_du_clic():
    d = _points()
    shown = cv.pick_players(d, 4)
    names = pd.DataFrame({"player_id": d.str_player_id, "player": d.str_player_id, "team": "FC", "championnat": "L"})
    fig, ids = cv.build_figure(d, shown, {0: "A", 1: "B", 2: "C"}, names, picks=["p0_0"])
    assert len(ids) == 3 and all(len(v) == 4 for v in ids.values())
    # 3 ellipses, 3 nuages, 1 surbrillance
    assert len(fig.data) == 7
    curve = list(ids)[1]
    assert cv.clicked_players([{"curve_number": curve, "point_index": 2}], ids) == [ids[curve][2]]
    assert cv.clicked_players([{"curve_number": 99, "point_index": 0}], ids) == []   # courbe inconnue (ellipse, surbrillance)
    assert cv.clicked_players([], ids) == []


def test_comparaison_garde_les_deux_derniers_sans_doublon():
    assert cv.update_picks([], ["a"]) == ["a"]
    assert cv.update_picks(["a"], ["a", "b"]) == ["a", "b"]
    assert cv.update_picks(["a", "b"], ["c"]) == ["b", "c"]
