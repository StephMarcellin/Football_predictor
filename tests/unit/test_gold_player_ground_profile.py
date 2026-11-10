"""Tests de gold_player_ground_profile (attendu par zone, rétrécissement, fenêtre, fautes) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/gold/gold_player_ground_profile.sql"

SCHEMA = dict(
    str_match_id="VARCHAR", str_season="VARCHAR", str_player_id="VARCHAR", str_opp_player_id="VARCHAR",
    str_duel_family="VARCHAR", str_role="VARCHAR", str_zone_5x5="VARCHAR", str_zone_depth="VARCHAR",
    str_zone_corridor="VARCHAR", bool_is_won="BOOLEAN", bool_ball_recovered="BOOLEAN",
    int_own_shots_10s="INTEGER", int_opp_shots_10s="INTEGER", bool_yellow_card_120s="BOOLEAN",
    bool_red_card_120s="BOOLEAN", bool_leads_to_penalty="BOOLEAN", str_next_event_player_id="VARCHAR",
    int_next_event_row_num="INTEGER")


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    if rows:
        df = pd.DataFrame(rows)[list(schema)]
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")


def _row(season, player, opp, family, role, won, zone="z4_c3", yellow=None, red=None, pen=None, depth="tiers_off"):
    return dict(str_match_id="m1", str_season=season, str_player_id=player, str_opp_player_id=opp,
                str_duel_family=family, str_role=role, str_zone_5x5=zone, str_zone_depth=depth,
                str_zone_corridor="axe", bool_is_won=won, bool_ball_recovered=None, int_own_shots_10s=0,
                int_opp_shots_10s=0, bool_yellow_card_120s=yellow, bool_red_card_120s=red,
                bool_leads_to_penalty=pen, str_next_event_player_id=None, int_next_event_row_num=None)


def _dribbles(season, att, dfn, n, wins):
    rows = []
    for i in range(n):
        won = i < wins
        rows.append(_row(season, att, dfn, "dribble", "attaquant", won))
        rows.append(_row(season, dfn, att, "dribble", "defenseur", not won))
    return rows


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    s = "2019-2020"
    rows = _dribbles(s, "A", "D", 10, 6) + _dribbles(s, "B", "E", 10, 2)      # A réussit 6/10, B 2/10
    rows += _dribbles("2018-2019", "A", "D", 5, 5)                            # saison précédente : 5 réussis sur 5
    # fautes : F commet 4 fautes dont 1 jaune, G en commet 6 dont 3 jaunes
    for p, n, y in (("F", 4, 1), ("G", 6, 3)):
        for i in range(n):
            rows.append(_row(s, p, "V", "faute", "auteur", False, yellow=i < y, red=False, pen=False))
            rows.append(_row(s, "V", p, "faute", "victime", True, yellow=i < y, red=False, pen=False))
    _table(con, "g", SCHEMA, rows)
    _table(con, "xt", dict(str_match_id="VARCHAR", int_row_num="INTEGER", dec_xt_added="DOUBLE"), [])
    ids = {p for r in rows for p in (r["str_player_id"],)}
    _table(con, "pts", dict(str_player_id="VARCHAR", str_season="VARCHAR", str_team_id="VARCHAR", str_player_name="VARCHAR",
                            str_macro_position="VARCHAR", int_n_matchs="INTEGER"),
           [dict(str_player_id=p, str_season=se, str_team_id="t", str_player_name=p, str_macro_position="ATT", int_n_matchs=30)
            for p in ids for se in ("2018-2019", "2019-2020")])
    _table(con, "mins", dict(str_player_id="VARCHAR", str_season="VARCHAR", int_minutes_played="INTEGER",
                             int_n_tackles="INTEGER", int_n_tackles_won="INTEGER"),
           [dict(str_player_id=p, str_season=se, int_minutes_played=1800, int_n_tackles=0, int_n_tackles_won=0)
            for p in ids for se in ("2018-2019", "2019-2020")])
    refs = {"intermediate_ground_duel_players": "g", "intermediate_xt_contributions": "xt",
            "intermediate_whoscored_player_team_season": "pts", "intermediate_player_match_event_stats": "mins"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n])
    return con.execute(sql).df().set_index(["str_player_id", "str_season"])


def test_cle_unique(res):
    assert not res.index.duplicated().any()


def test_taux_brut_et_volumes_de_la_fenetre(res):
    a = res.loc[("A", "2019-2020")]
    assert (a.int_n_seasons_window, a.int_n_dribbles, a.int_n_dribbles_won) == (2, 15, 11)
    assert a.dec_dribble_win_rate == pytest.approx(11 / 15)
    first = res.loc[("A", "2018-2019")]
    assert first.int_n_seasons_window == 1 and first.int_n_dribbles == 5


def test_au_dessus_de_l_attendu_de_la_zone_retreci(res):
    # 2019-2020, zone z4_c3 : attaquants A 6/10 et B 2/10, attendu 0,4. A : +2 / (10 + 30) sur cette saison.
    # Fenêtre de B (une seule saison) : -2 / (10 + 30).
    assert res.loc[("B", "2019-2020")].dec_dribble_woe == pytest.approx(-2 / 40)
    assert res.loc[("E", "2019-2020")].dec_defended_woe == pytest.approx(2 / 40)   # E stoppe 8/10 pour un attendu de 0,6


def test_ajustement_par_l_adversaire_leave_one_out(res):
    # B (une saison) face à E, qui sur-performe en défense : le même résultat est jugé moins sévèrement.
    # r1 des 10 duels de B : 2 gagnés (+0,6) et 8 perdus (-0,4) ; effet de E hors duel = (s1_E + r1) / (n_E - 1 + 30)
    # avec s1_E = +2. Somme ajustée = -2 + sum((2 + r1_i) / 39).
    expected = (-2 + (10 * 2 + (2 * 0.6 + 8 * -0.4)) / 39) / 40
    assert res.loc[("B", "2019-2020")].dec_dribble_woe_adj == pytest.approx(expected)
    assert res.loc[("B", "2019-2020")].dec_dribble_woe_adj > res.loc[("B", "2019-2020")].dec_dribble_woe


def test_jaunes_par_faute_retrecis_vers_la_moyenne(res):
    # moyenne générale : 4 jaunes sur 10 fautes = 0,4 ; F : 1 jaune sur 4 fautes, K = 20
    f = res.loc[("F", "2019-2020")]
    assert (f.int_n_fouls_committed, f.int_n_yellow_after_foul) == (4, 1)
    assert f.dec_yellow_per_foul == pytest.approx((1 + 20 * 0.4) / (4 + 20))
    assert res.loc[("V", "2019-2020")].int_n_fouls_drawn == 10


def test_dribbleur_sans_faute_a_une_part_de_fautes_nulle_vers_la_moyenne(res):
    a = res.loc[("A", "2019-2020")]
    assert a.int_n_fouls_committed == 0
    assert 0 <= a.dec_foul_committed_share <= 1
