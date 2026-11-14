"""Tests de intermediate_player_match_commitment (blocs, dernier homme, actions en surface) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/intermediate/intermediate_player_match_commitment.sql"

EV = dict(str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", int_row_num="INTEGER",
          str_type_name="VARCHAR", dec_x="DOUBLE", dec_y="DOUBLE", str_season="VARCHAR")
Q = dict(str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", int_row_num="INTEGER",
         str_type_name="VARCHAR", str_qual_type_name="VARCHAR", dec_x="DOUBLE", dec_y="DOUBLE", str_season="VARCHAR")


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    df = pd.DataFrame(rows)[list(schema)]
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def _ev(player, row, typ, x, y):
    return dict(str_match_id="m1", str_team_id="1", str_player_id=player, int_row_num=row, str_type_name=typ,
                dec_x=x, dec_y=y, str_season="2020-2021")


def _q(player, row, typ, qual, x, y):
    return dict(str_match_id="m1", str_team_id="1", str_player_id=player, int_row_num=row, str_type_name=typ,
                str_qual_type_name=qual, dec_x=x, dec_y=y, str_season="2020-2021")


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    ev = [
        _ev("10", 1, "Tackle", 10, 50),            # dans la surface propre
        _ev("10", 2, "Clearance", 12, 50),         # dans la surface propre
        _ev("10", 3, "Interception", 40, 50),      # hors surface
        _ev("10", 4, "BlockedPass", 15, 90),       # x dans la surface mais trop excentré : hors surface
        _ev("11", 5, "Tackle", 17, 20),            # limite incluse
        _ev("12", 6, "Pass", 5, 50),               # pas une action défensive
        _ev("10", 3, "Interception", 40, 50),      # événement en double : une seule action
    ]
    q = [
        _q("10", 7, "Save", "OutfielderBlock", 8, 45), _q("10", 7, "Save", "OutfielderBlock", 8, 45),  # qualifier en double
        _q("10", 8, "Clearance", "BlockedCross", 20, 80),
        _q("11", 9, "Tackle", "LastMan", 30, 50),
        _q("13", 10, "Save", "OutfielderBlock", 60, 50),
    ]
    _table(con, "ev", EV, ev)
    _table(con, "q", Q, q)
    refs = {"intermediate_whoscored_events": "ev", "intermediate_whoscored_event_qualifiers": "q"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n], var=lambda k, d=None: d, is_incremental=lambda: False)
    return con.execute(sql).fetchdf().set_index("str_player_id")


def test_joueurs_sans_action_defensive_ou_sacrifice_absents(res):
    assert sorted(res.index) == ["10", "11", "13"]


def test_actions_defensives_et_surface(res):
    r = res.loc["10"]
    # Tackle, Clearance, Interception, BlockedPass, tir bloqué : 5 actions, dont 3 dans la surface (Tackle, Clearance, tir bloqué)
    assert (r.int_def_actions, r.int_def_actions_own_box) == (5, 3)
    assert (res.loc["11", "int_def_actions"], res.loc["11", "int_def_actions_own_box"]) == (1, 1)
    assert res.loc["13", "int_def_actions_own_box"] == 0


def test_blocs_et_dernier_homme(res):
    r = res.loc["10"]
    assert (r.int_shots_blocked, r.int_passes_blocked, r.int_crosses_blocked, r.int_last_man_actions) == (1, 1, 1, 0)
    assert res.loc["11", "int_last_man_actions"] == 1
