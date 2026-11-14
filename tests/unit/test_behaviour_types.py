"""Tests de pipelines/features/behaviour_types.py sur données synthétiques."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

_spec = importlib.util.spec_from_file_location(
    "behaviour_types", Path(__file__).resolve().parents[2] / "pipelines/features/behaviour_types.py")
bt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bt)


def _frame(seed=0, per_group=60):
    """Deux groupes bien séparés par famille de poste, sur trois fenêtres d'apprentissage."""
    rng = np.random.default_rng(seed)
    rows = []
    for role in ("FB", "GK"):
        for season in ("2016-2017", "2017-2018", "2018-2019", "2019-2020"):
            for g in (0, 1):
                for i in range(per_group):
                    r = {"str_player_id": f"{role}{g}_{i}", "str_season": season, "str_macro_role": role,
                         "bool_is_peer": True}
                    for f in ("pass_accuracy", "shots", "tackles", "key_passes"):
                        r[f"dec_z_{f}"] = (3.0 if g else -3.0) + rng.normal(0, 0.3)
                    # côté : un tiers des joueurs à droite, deux tiers à gauche
                    right = i % 3 == 0
                    for d in bt.DEPTHS:
                        r[f"dec_sec_{d}_right"], r[f"dec_sec_{d}_left"] = (0.30, 0.02) if right else (0.02, 0.30)
                        r[f"dec_z_sec_{d}_right"], r[f"dec_z_sec_{d}_left"] = (2.0, -1.0) if right else (-1.0, 2.0)
                        r[f"dec_z_sec_{d}_centre"] = 0.0
                    rows.append(r)
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def out():
    return bt.build(_frame())


def test_les_deux_groupes_sont_separes(out):
    fb = out[out.str_macro_role == "FB"]
    # 4 types demandés pour 2 groupes réels : chaque type doit rester dans un seul groupe
    purity = fb.groupby("int_type").str_player_id.agg(lambda s: s.str[:3].value_counts(normalize=True).iloc[0])
    assert (purity > 0.99).all()


def test_gauche_et_droite_sont_ramenes_au_meme_flanc_principal():
    df = bt.add_mirrored_sectors(_frame().query("str_macro_role == 'FB'"))
    assert df["dec_z_sec_att_main"].eq(2.0).all() and df["dec_z_sec_att_off"].eq(-1.0).all()
    assert set(df.loc[df.str_player_id.str.endswith(("_0", "_3")), "str_side"]) == {"R"}


def test_le_gardien_n_a_pas_de_cote(out):
    gk = out[out.str_macro_role == "GK"]
    assert gk.str_side.isna().all() and gk.dec_lateral_index.isna().all()


def test_colonnes_et_description(out):
    assert {"str_type", "str_type_description", "dec_dist_to_center", "dec_margin_to_second", "dec_pc1"} <= set(out.columns)
    assert out.str_type_description.str.startswith("haut :").all()
    assert (out.dec_margin_to_second >= 0).all()


def test_seuls_les_pairs_sont_types():
    df = _frame()
    df.loc[df.str_player_id == "FB0_0", "bool_is_peer"] = False
    assert "FB0_0" not in set(bt.build(df).str_player_id)
