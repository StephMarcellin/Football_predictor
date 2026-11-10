"""Tests de intermediate_ground_duel_players (dribbles, dispossessions, fautes) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/intermediate/intermediate_ground_duel_players.sql"

PAIR_SCHEMA = dict(
    str_match_id="VARCHAR", str_season="VARCHAR", str_league_source="VARCHAR", int_expanded_minute="INTEGER",
    int_second="INTEGER", str_duel_type_id="VARCHAR", str_duel_type="VARCHAR", dec_x="DOUBLE", dec_y="DOUBLE",
    int_event_id_a="INTEGER", int_team_id_a="BIGINT", int_player_id_a="INTEGER", int_outcome_a="INTEGER",
    int_event_id_b="INTEGER", int_team_id_b="BIGINT", int_player_id_b="INTEGER", int_outcome_b="INTEGER")
EV_SCHEMA = dict(
    str_match_id="VARCHAR", str_team_id="VARCHAR", str_player_id="VARCHAR", str_event_id="VARCHAR",
    int_row_num="INTEGER", str_type_name="VARCHAR", str_outcome_id="VARCHAR", bool_is_shot="BOOLEAN",
    str_card_type="VARCHAR", int_expanded_minute="INTEGER", int_second="INTEGER")


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    if rows:
        df = pd.DataFrame(rows)[list(schema)]
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")


def _pair(dtype_id, dtype, minute, x, y, a, b):
    """a, b = (équipe, joueur, événement, issue)."""
    return dict(str_match_id="m1", str_season="2020-2021", str_league_source="PL", int_expanded_minute=minute,
                int_second=0, str_duel_type_id=dtype_id, str_duel_type=dtype, dec_x=x, dec_y=y,
                int_team_id_a=a[0], int_player_id_a=a[1], int_event_id_a=a[2], int_outcome_a=a[3],
                int_team_id_b=b[0], int_player_id_b=b[1], int_event_id_b=b[2], int_outcome_b=b[3])


def _ev(team, player, event, row, typ, outcome, minute, second=0, shot=False, card=None):
    return dict(str_match_id="m1", str_team_id=str(team), str_player_id=str(player), str_event_id=str(event),
                int_row_num=row, str_type_name=typ, str_outcome_id=str(outcome), bool_is_shot=shot,
                str_card_type=card, int_expanded_minute=minute, int_second=second)


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    pairs = [
        # dribble réussi : le défenseur battu (Challenge) est côté A, l'attaquant (TakeOn) côté B
        _pair("45", "Challenge", 10, 30, 50, (2, 20, 1, 0), (1, 10, 2, 1)),
        # dribble raté, tacle gagnant
        _pair("3", "TakeOn", 20, 40, 20, (1, 10, 3, 0), (2, 20, 4, 1)),
        # dépossession, tacle perdant
        _pair("50", "Dispossessed", 30, 80, 90, (1, 10, 5, 1), (2, 21, 6, 0)),
        # faute : la victime côté A, l'auteur côté B
        _pair("4", "Foul", 40, 50, 50, (1, 10, 7, 1), (2, 22, 8, 0)),
        # duel aérien : ignoré
        _pair("44", "Aerial", 50, 50, 50, (1, 11, 9, 1), (2, 23, 10, 0)),
    ]
    ev = [
        _ev(2, 20, 1, 1, "Challenge", 0, 10), _ev(1, 10, 2, 2, "TakeOn", 1, 10),
        _ev(1, 10, 2, 2, "TakeOn", 1, 10),                       # événement en double : une seule ligne par duel
        _ev(1, 10, 11, 3, "Pass", 1, 10, 1), _ev(1, 10, 12, 4, "SavedShot", 1, 10, 5, shot=True),
        _ev(1, 10, 3, 10, "TakeOn", 0, 20), _ev(2, 20, 4, 11, "Tackle", 1, 20),
        _ev(1, 10, 5, 20, "Dispossessed", 1, 30), _ev(2, 21, 6, 21, "Tackle", 0, 30),
        _ev(1, 10, 7, 30, "Foul", 1, 40), _ev(2, 22, 8, 31, "Foul", 0, 40),
        _ev(2, 22, 99, 32, "Card", 1, 40, 30, card="Yellow"),
        _ev(1, 11, 9, 40, "Aerial", 1, 50), _ev(2, 23, 10, 41, "Aerial", 0, 50),
    ]
    _table(con, "pairs", PAIR_SCHEMA, pairs)
    _table(con, "ev", EV_SCHEMA, ev)
    _table(con, "fd", dict(str_match_id="VARCHAR", str_event_id="VARCHAR", str_drawing_team_id="VARCHAR",
                           bool_leads_to_penalty="BOOLEAN"),
           [dict(str_match_id="m1", str_event_id="7", str_drawing_team_id="1", bool_leads_to_penalty=True)])
    refs = {"intermediate_duel_pairs": "pairs", "intermediate_whoscored_events_enriched": "ev",
            "intermediate_fouls_drawn": "fd"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n], var=lambda k, d=None: d, is_incremental=lambda: False)
    return con.execute(sql).df().set_index(["str_player_id", "int_event_id"])


def test_une_ligne_par_joueur_et_par_duel_sans_aerien_ni_doublon(res):
    assert len(res) == 8
    assert not res.index.duplicated().any()
    assert set(res.str_duel_family) == {"dribble", "dispossession", "faute"}


def test_dribble_reussi_roles_issue_et_zone_dans_le_repere_de_l_attaquant(res):
    att, dfn = res.loc[("10", 2)], res.loc[("20", 1)]
    assert (att.str_role, dfn.str_role) == ("attaquant", "defenseur")
    assert bool(att.bool_is_won) is True and bool(dfn.bool_is_won) is False
    # le duel est à x = 30 pour le défenseur : 70 dans le repère de l'attaquant
    assert (att.str_zone_depth, att.str_zone_5x5, att.str_zone_corridor) == ("tiers_off", "z4_c3", "axe")
    assert (dfn.str_zone_5x5, dfn.dec_x_duel) == (att.str_zone_5x5, att.dec_x_duel)
    assert att.dec_x == pytest.approx(70) and dfn.dec_x == pytest.approx(30)


def test_tirs_dans_les_dix_secondes_vus_de_chaque_cote(res):
    att, dfn = res.loc[("10", 2)], res.loc[("20", 1)]
    assert (att.int_own_shots_10s, att.int_opp_shots_10s) == (1, 0)
    assert (dfn.int_own_shots_10s, dfn.int_opp_shots_10s) == (0, 1)


def test_dribble_rate_tacle_gagnant_recupere_le_ballon(res):
    att, dfn = res.loc[("10", 3)], res.loc[("20", 4)]
    assert bool(att.bool_is_won) is False and bool(dfn.bool_is_won) is True
    assert bool(dfn.bool_ball_recovered) is True
    assert dfn.dec_x == pytest.approx(60)           # repère du défenseur : miroir de x = 40


def test_dispossession_l_attaquant_perd_toujours_le_tacle_decide_du_gagnant(res):
    att, dfn = res.loc[("10", 5)], res.loc[("21", 6)]
    assert bool(att.bool_is_won) is False
    assert bool(dfn.bool_is_won) is False           # tacle perdant : le ballon n'est pas récupéré
    assert att.str_zone_corridor == "droit" and att.str_zone_depth == "tiers_off"


def test_faute_victime_auteur_carton_et_penalty(res):
    vic, aut = res.loc[("10", 7)], res.loc[("22", 8)]
    assert (vic.str_role, aut.str_role) == ("victime", "auteur")
    assert bool(vic.bool_is_won) is True and bool(aut.bool_is_won) is False
    assert bool(aut.bool_yellow_card_120s) is True and bool(aut.bool_red_card_120s) is False
    assert bool(vic.bool_leads_to_penalty) is True and bool(aut.bool_leads_to_penalty) is True


def test_consequences_de_faute_nulles_hors_faute(res):
    r = res.loc[("10", 2)]
    assert pd.isna(r.bool_yellow_card_120s) and pd.isna(r.bool_leads_to_penalty)
