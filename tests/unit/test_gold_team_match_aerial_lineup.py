"""Tests de gold_team_match_aerial_lineup : agrégats aériens de la composition de départ, sans fuite.

Le SQL est rendu avec Jinja (ref simulé) puis exécuté dans DuckDB en mémoire. Match m1 de la saison
2020-2021 entre t1 et t2.
"""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/gold/gold_team_match_aerial_lineup.sql"
S = "2020-2021"


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    if rows:
        df = pd.DataFrame(rows)[list(schema)]
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")


def _prof(pid, season, off=0.0, dfn=0.0, duels=2.0, sp=0.1, op=0.05):
    return dict(str_player_id=pid, str_season=season, dec_woe_net=(off + dfn) / 2, dec_woe_net_off=off, dec_woe_net_def=dfn,
                dec_duels_per_match=duels, dec_head_shots_setpiece_per_match=sp, dec_head_shots_open_per_match=op)


@pytest.fixture(scope="module")
def res():
    # (id, équipe, poste, taille) ; t1 : gardien, 4 défenseurs, 1 milieu, 2 attaquants (+ un remplaçant entré à la 60e)
    players = [("g1", "t1", "GK", 195), ("d1", "t1", "DEF", 190), ("d2", "t1", "DEF", 190), ("d3", "t1", "DEF", 190),
               ("d4", "t1", "DEF", 190), ("m1", "t1", "MIL", 178), ("a1", "t1", "ATT", 180), ("a2", "t1", "ATT", 180),
               ("sub", "t1", "ATT", 200),
               ("gg", "t2", "GK", 190), ("e1", "t2", "DEF", 185), ("e2", "t2", "DEF", 185), ("f1", "t2", "ATT", 192),
               ("f2", "t2", "ATT", 192)]
    lineup = [dict(str_match_id="m1", str_team_id=t, str_player_id=p, int_start_minute=60 if p == "sub" else 0) for p, t, _, _ in players]
    con = duckdb.connect()
    _table(con, "lineup", dict(str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", int_start_minute="INTEGER"), lineup)
    _table(con, "gtm", dict(str_match_id="VARCHAR", str_team_id="VARCHAR", str_opponent_id="VARCHAR", str_season="VARCHAR"),
           [dict(str_match_id="m1", str_team_id="t1", str_opponent_id="t2", str_season=S),
            dict(str_match_id="m1", str_team_id="t2", str_opponent_id="t1", str_season=S)])
    _table(con, "pts", dict(str_player_id="VARCHAR", int_height="DOUBLE", str_macro_position="VARCHAR", int_n_matchs="INTEGER"),
           [dict(str_player_id=p, int_height=float(h), str_macro_position=pos, int_n_matchs=30) for p, _, pos, h in players])
    prof_schema = dict(str_player_id="VARCHAR", str_season="VARCHAR", dec_woe_net="DOUBLE", dec_woe_net_off="DOUBLE",
                       dec_woe_net_def="DOUBLE", dec_duels_per_match="DOUBLE", dec_head_shots_setpiece_per_match="DOUBLE",
                       dec_head_shots_open_per_match="DOUBLE")
    profs = ([_prof(p, "2019-2020", dfn=0.10) for p in ("d1", "d2", "d3", "d4")]            # s-1 : retenu
             + [_prof("a1", "2019-2020", off=0.05), _prof("a2", "2018-2019", off=0.05)]     # a2 : seulement s-2 : retenu
             + [_prof("a1", "2018-2019", off=-0.30)]                                          # a1 aussi s-2 : ignoré (s-1 existe)
             + [_prof("m1", S, off=0.5, dfn=0.5)]                                             # même saison : ignoré (fuite)
             + [_prof("g1", "2016-2017")]                                                     # trop ancien : ignoré
             + [_prof(p, "2019-2020", off=0.0, dfn=0.0) for p in ("e1", "e2", "f1", "f2")])
    _table(con, "prof", prof_schema, profs)
    refs = {"intermediate_whoscored_lineup_period": "lineup", "gold_team_match": "gtm",
            "intermediate_whoscored_player_team_season": "pts", "gold_player_aerial_profile": "prof"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(config=lambda **k: "", ref=lambda n: refs[n])
    return con.execute(sql).df().set_index("str_team_id")


def test_une_ligne_par_equipe_et_remplacant_exclu(res):
    assert list(res.index.sort_values()) == ["t1", "t2"]
    assert res.loc["t1", "int_n_starters"] == 8          # le remplaçant (minute 60) n'est pas compté
    assert res.loc["t2", "int_n_starters"] == 5


def test_aucune_fuite_profils_s_moins_1_ou_s_moins_2_seulement(res):
    r = res.loc["t1"]
    assert r.int_n_profiled == 6                          # d1-d4, a1, a2 ; ni m1 (même saison) ni g1 (trop ancien)
    assert r.dec_xi_woe_net_off_att == pytest.approx(0.05)   # a1 lit 2019-2020 (0,05), pas 2018-2019 (-0,30) ; a2 lit 2018-2019
    assert r.dec_xi_woe_net_def_def == pytest.approx(0.10)
    assert r.dec_xi_duels_per_match_sum == pytest.approx(6 * 2.0)   # m1 et g1 sans profil : ne comptent pas


def test_tailles_gardien_exclu_et_top3(res):
    r = res.loc["t1"]
    assert r.dec_xi_height_mean == pytest.approx((190 * 4 + 178 + 180 * 2) / 7)   # sans le gardien (195)
    assert r.dec_xi_height_top3 == pytest.approx(190)
    assert r.dec_xi_height_def == pytest.approx(190) and r.dec_xi_height_att == pytest.approx(180)


def test_menaces_de_la_tete_sommees(res):
    r = res.loc["t1"]
    assert r.dec_xi_head_shots_setpiece_sum == pytest.approx(6 * 0.1)
    assert r.dec_xi_head_shots_open_sum == pytest.approx(6 * 0.05)


def test_ecarts_face_a_l_adversaire_et_symetrie(res):
    a, b = res.loc["t1"], res.loc["t2"]
    assert a.dec_edge_att_height == pytest.approx(180 - 185) and a.dec_edge_def_height == pytest.approx(190 - 192)
    assert b.dec_edge_att_height == pytest.approx(-a.dec_edge_def_height) and b.dec_edge_def_height == pytest.approx(-a.dec_edge_att_height)
    assert a.dec_edge_att_skill == pytest.approx(0.05 - 0.0) and a.dec_edge_def_skill == pytest.approx(0.10 - 0.0)
    assert a.dec_edge_height_mean == pytest.approx(-b.dec_edge_height_mean)
