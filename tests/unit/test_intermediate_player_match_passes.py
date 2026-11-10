"""Tests de intermediate_player_match_passes (passes de jeu ouvert, remises en jeu, xT) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/intermediate/intermediate_player_match_passes.sql"

EV_SCHEMA = dict(
    str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", int_row_num="INTEGER",
    str_type_name="VARCHAR", str_outcome_id="VARCHAR", dec_x="DOUBLE", dec_y="DOUBLE",
    dec_end_x="DOUBLE", dec_end_y="DOUBLE", str_season="VARCHAR")
Q_SCHEMA = dict(str_match_id="VARCHAR", int_row_num="INTEGER", str_type_name="VARCHAR",
                str_qual_type_name="VARCHAR", str_season="VARCHAR")
XT_SCHEMA = dict(str_match_id="VARCHAR", int_row_num="INTEGER", str_season="VARCHAR", dec_xt_added="DOUBLE")


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    df = pd.DataFrame(rows)[list(schema)]
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def _pass(player, row, outcome, x, y, ex, ey, typ="Pass"):
    return dict(str_match_id="m1", str_team_id="1", str_player_id=str(player), int_row_num=row, str_type_name=typ,
                str_outcome_id=str(outcome), dec_x=x, dec_y=y, dec_end_x=ex, dec_end_y=ey, str_season="2020-2021")


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    ev = [
        _pass(10, 1, 1, 20, 50, 40, 50),    # réussie, vers l'avant (21 m)
        _pass(10, 2, 0, 30, 50, 80, 50),    # ratée, longue (52,5 m), vers l'avant
        _pass(10, 3, 1, 60, 50, 50, 50),    # réussie, vers l'arrière (10,5 m)
        _pass(10, 4, 1, 50, 10, 50, 40),    # réussie, latérale (20,4 m), jamais d'avance en x
        _pass(10, 5, 1, 10, 10, 10, 30),    # remise en jeu (touche) : exclue des taux
        _pass(10, 6, 1, 60, 50, 90, 50),    # réussie, longue (31,5 m), entrée dans le dernier tiers
        _pass(11, 7, 1, 50, 50, 55, 50),    # autre joueur, vers l'avant (5,25 m)
        _pass(12, 8, 1, 50, 50, 55, 50, typ="TakeOn"),   # pas une passe : ignoré
    ]
    q = [dict(str_match_id="m1", int_row_num=5, str_type_name="Pass", str_qual_type_name="ThrowIn",
              str_season="2020-2021")]
    xt = [dict(str_match_id="m1", int_row_num=1, str_season="2020-2021", dec_xt_added=0.01),
          dict(str_match_id="m1", int_row_num=3, str_season="2020-2021", dec_xt_added=-0.004),
          dict(str_match_id="m1", int_row_num=8, str_season="2020-2021", dec_xt_added=0.5)]  # dribble : ignoré
    _table(con, "ev", EV_SCHEMA, ev)
    _table(con, "q", Q_SCHEMA, q)
    _table(con, "xt", XT_SCHEMA, xt)
    refs = {"intermediate_whoscored_events": "ev", "intermediate_whoscored_event_qualifiers": "q",
            "intermediate_xt_contributions": "xt"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n], var=lambda k, d=None: d, is_incremental=lambda: False)
    return con.execute(sql).fetchdf().set_index("str_player_id")


def test_une_ligne_par_joueur_et_les_non_passes_sont_ignores(res):
    assert sorted(res.index) == ["10", "11"]


def test_les_remises_en_jeu_sont_exclues_des_taux(res):
    r = res.loc["10"]
    assert (r.int_passes_open, r.int_passes_set_piece) == (5, 1)
    assert (r.int_passes_open_completed, r.int_passes_open_lost) == (4, 1)


def test_direction_et_longueur(res):
    r = res.loc["10"]
    assert (r.int_passes_forward, r.int_passes_forward_completed, r.int_passes_backward) == (3, 2, 1)
    assert (r.int_passes_long, r.int_passes_long_completed) == (2, 1)
    assert r.int_passes_short == 1


def test_tiers_de_depart_et_entree_dans_le_dernier_tiers(res):
    r = res.loc["10"]
    assert (r.int_passes_from_own_third, r.int_passes_from_middle_third, r.int_passes_from_attacking_third) == (2, 3, 0)
    assert r.int_passes_into_final_third == 1


def test_xt_des_passes_seulement(res):
    assert res.loc["10"].dec_xt_created == pytest.approx(0.006)
    assert res.loc["10"].dec_xt_created_positive == pytest.approx(0.01)
    assert res.loc["11"].dec_xt_created == 0
