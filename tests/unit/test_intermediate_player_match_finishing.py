"""Tests de intermediate_player_match_finishing (tirs, lieu, tête, penaltys, coups de pied arrêtés) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/intermediate/intermediate_player_match_finishing.sql"
EV = dict(str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", int_row_num="INTEGER",
          str_type_name="VARCHAR", dec_x="DOUBLE", dec_y="DOUBLE", str_season="VARCHAR")
Q = dict(str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", int_row_num="INTEGER",
         str_type_name="VARCHAR", str_qual_type_name="VARCHAR", str_season="VARCHAR")


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    con.register("tmp_df", pd.DataFrame(rows)[list(schema)])
    con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def _ev(player, row, typ, x, y):
    return dict(str_match_id="m1", str_team_id="1", str_player_id=player, int_row_num=row, str_type_name=typ,
                dec_x=x, dec_y=y, str_season="2020-2021")


def _q(player, row, typ, qual):
    return dict(str_match_id="m1", str_team_id="1", str_player_id=player, int_row_num=row, str_type_name=typ,
                str_qual_type_name=qual, str_season="2020-2021")


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    ev = [
        _ev("10", 1, "Goal", 90, 50),                 # but dans la surface, cadré
        _ev("10", 2, "SavedShot", 70, 50),            # arrêté, hors surface, cadré
        _ev("10", 3, "SavedShot", 90, 30),            # bloqué : tir, pas cadré
        _ev("10", 4, "MissedShots", 95, 55),          # raté dans la surface, de la tête
        _ev("10", 5, "Goal", 88, 50),                 # penalty : exclu des tirs de jeu
        _ev("10", 6, "Goal", 50, 50),                 # csc : exclu
        _ev("11", 7, "Pass", 100, 0), _ev("11", 8, "Pass", 30, 20), _ev("11", 9, "Pass", 50, 50),
        _ev("11", 10, "SavedShot", 80, 50),           # coup franc direct
    ]
    q = [_q("10", 3, "SavedShot", "Blocked"), _q("10", 3, "SavedShot", "Blocked"),   # qualifier en double
         _q("10", 4, "MissedShots", "Head"), _q("10", 1, "Goal", "BigChance"),
         _q("10", 5, "Goal", "Penalty"), _q("10", 6, "Goal", "OwnGoal"),
         _q("11", 7, "Pass", "CornerTaken"), _q("11", 8, "Pass", "FreekickTaken"),
         _q("11", 10, "SavedShot", "DirectFreekick")]
    _table(con, "ev", EV, ev)
    _table(con, "q", Q, q)
    refs = {"intermediate_whoscored_events": "ev", "intermediate_whoscored_event_qualifiers": "q"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n], var=lambda k, d=None: d, is_incremental=lambda: False)
    return con.execute(sql).fetchdf().set_index("str_player_id")


def test_tirs_de_jeu_sans_penalty_ni_csc(res):
    r = res.loc["10"]
    assert r.int_shots_open == 4 and r.int_goals_open == 1
    assert r.int_penalties_taken == 1


def test_cadrage_blocs_tete_surface(res):
    r = res.loc["10"]
    assert r.int_shots_on_target == 2           # but + arrêté non bloqué ; le bloqué ne compte pas
    assert r.int_shots_blocked_by_def == 1      # qualifier en double : un seul bloc
    assert r.int_shots_head == 1 and r.int_shots_big_chance == 1
    assert r.int_shots_in_box == 3 and r.int_shots_outside_box == 1


def test_distance_moyenne(res):
    pts = [(90, 50), (70, 50), (90, 30), (95, 55)]
    attendu = sum((((100 - x) * 1.05) ** 2 + ((50 - y) * 0.68) ** 2) ** 0.5 for x, y in pts) / len(pts)
    assert res.loc["10"].dec_shot_dist_mean == pytest.approx(attendu)


def test_coups_de_pied_arretes(res):
    r = res.loc["11"]
    assert r.int_corners_taken == 1 and r.int_freekicks_played == 1 and r.int_freekicks_direct_shots == 1
    assert r.int_shots_open == 1               # le coup franc direct est aussi un tir
