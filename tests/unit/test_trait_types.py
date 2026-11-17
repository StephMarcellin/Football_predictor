"""Tests de pipelines/features/trait_types.py sur données synthétiques."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

_spec = importlib.util.spec_from_file_location(
    "trait_types", Path(__file__).resolve().parents[2] / "pipelines/features/trait_types.py")
tt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tt)


def _frame(per_group=80, seed=0):
    """Trois niveaux de jeu de tête (faible, moyen, fort), répartis sur DEUX postes : le type ne doit pas suivre le poste."""
    rng = np.random.default_rng(seed)
    rows = []
    for season in ("2016-2017", "2017-2018", "2018-2019", "2019-2020"):
        for level, mu in enumerate((-2.0, 0.0, 2.0)):
            for i in range(per_group):
                r = {"str_player_id": f"p{level}_{i}", "str_season": season, "bool_is_peer": True,
                     "str_macro_role": "CB" if i % 2 == 0 else "ST"}
                for fam, feats in tt.FAMILIES.items():
                    for f in feats:
                        r[f"dec_z_{f}"] = (mu if fam == "aerien" else 0.0) + rng.normal(0, 0.3)
                rows.append(r)
    gk = {"str_player_id": "gk", "str_season": "2016-2017", "bool_is_peer": True, "str_macro_role": "GK"}
    for feats in tt.FAMILIES.values():
        for f in feats:
            gk[f"dec_z_{f}"] = 0.0
    rows.append(gk)
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def res():
    return tt.build(_frame())


def test_les_trois_niveaux_sont_separes_et_ordonnes(res):
    types, _ = res
    t = types.assign(niveau=types.str_player_id.str[1].astype(int))
    pur = t.groupby("niveau").int_type_aerien.agg(lambda s: s.value_counts(normalize=True).iloc[0])
    assert (pur > 0.99).all()
    assert t.groupby("niveau").int_type_aerien.mean().is_monotonic_increasing    # type 0 = le plus faible


def test_le_type_est_independant_du_poste(res):
    types, _ = res
    ct = pd.crosstab(types.int_type_aerien, types.str_macro_role, normalize="columns")
    assert (ct.loc[:, "CB"] - ct.loc[:, "ST"]).abs().max() < 0.05


def test_les_gardiens_sont_exclus_et_les_colonnes_presentes(res):
    types, labels = res
    assert "gk" not in set(types.str_player_id)
    assert {f"int_type_{f}" for f in tt.FAMILIES} <= set(types.columns)
    assert len(labels) == len(tt.FAMILIES) * tt.N_TYPES + tt.N_GLOBAL
    assert "int_type_global" in types.columns and types.int_type_global.nunique() <= tt.N_GLOBAL
    assert labels.str_description.str.startswith("haut :").all()


def test_aucune_variable_de_position_dans_les_familles():
    feats = {f for v in tt.FAMILIES.values() for f in v}
    assert not any(f.startswith("sec_") or f.startswith("pass_from_") for f in feats)


def test_type_global_separe_les_groupes_et_ignore_le_poste(res):
    types, _ = res
    t = types.assign(niveau=types.str_player_id.str[1].astype(int))
    # trois niveaux de jeu de tête très marqués : chaque type global ne contient (presque) qu'un seul niveau
    pur = t.groupby("int_type_global")["niveau"].agg(lambda x: x.value_counts(normalize=True).iloc[0])
    assert (pur > 0.95).all()
    ct = pd.crosstab(t.int_type_global, t.str_macro_role, normalize="columns")
    assert (ct.loc[:, "CB"] - ct.loc[:, "ST"]).abs().max() < 0.06


def test_v2_ecarte_les_variables_instables_et_fournit_la_marge(res):
    types, _ = res
    assert not set(tt.UNSTABLE) & {f for fam in tt.FAMILIES for f in tt.family_features(fam)}
    assert set(tt.UNSTABLE) <= {f for v in tt.FAMILIES.values() for f in v}
    for fam in list(tt.FAMILIES) + ["global"]:
        m = types[f"dec_margin_{fam}"]
        assert m.between(0, 1).all()
    # un joueur au coeur d'un blob (aerien) a une marge faible
    assert (types.dec_margin_aerien < tt.CORE_RATIO).mean() > 0.9


def test_v1_reste_reproductible():
    types, labels = tt.build(_frame(), version="v1")
    assert "dec_margin_global" not in types.columns
    assert len(labels) == len(tt.FAMILIES) * tt.N_TYPES + tt.N_GLOBAL_V1


def test_noms_de_types_v1_et_v2_et_revalidation():
    assert tt.type_name("global", 2, "haut : apt_tir +0.8") == tt.TYPE_NAMES[("global", 2)][0]
    assert tt.type_name("global", 6, "haut : shots_on_target +1.0") == tt.TYPE_NAMES_GLOBAL_V1[6][0]
    assert tt.type_name("aerien", 2, "haut : shots +1").endswith("(à revalider)")
