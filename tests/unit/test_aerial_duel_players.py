"""Tests du modèle dbt intermediate_aerial_duel_players sur données synthétiques.

Le SQL est rendu avec Jinja (ref/this simulés) puis exécuté dans DuckDB en mémoire.
Scénario : un centre de l'extérieur gagné par la défense, puis un corner gagné par
l'attaque et suivi d'un but. Les ids d'événement sont volontairement identiques entre
équipes (l'id est propre à l'équipe) pour vérifier que la jointure en tient compte.
"""
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = (Path(__file__).resolve().parents[2]
            / "dbt_project/models/intermediate/intermediate_aerial_duel_players.sql")

EVENT_COLS = ("str_match_id, str_team_id, str_player_id, str_event_id, int_row_num, str_type_id, "
              "str_type_name, str_outcome_id, bool_is_shot, dec_x, dec_y, dec_end_x, dec_end_y, "
              "int_expanded_minute, int_second")


def _event(row, team, player, eid, type_id, type_name, minute, sec, x=50, y=50, ex=None, ey=None, shot=False, outcome=None):
    out = "NULL" if outcome is None else f"'{outcome}'"
    return (f"('m1','{team}','{player}','{eid}',{row},'{type_id}','{type_name}',{out},{str(shot).lower()},"
            f"{x},{y},{'NULL' if ex is None else ex},{'NULL' if ey is None else ey},{minute},{sec})")


@pytest.fixture(scope="module")
def result():
    events = [
        # Duel 1 : centre de l'extérieur de l'équipe 1, gagné par le défenseur de l'équipe 2
        _event(1, 1, 10, 1, 1, "Pass", 10, 0, x=10, y=10, ex=80, ey=50),
        _event(2, 1, 11, 2, 44, "Aerial", 10, 2, x=80, y=50),
        _event(3, 2, 20, 1, 44, "Aerial", 10, 2, x=20, y=50),
        _event(4, 2, 20, 2, 1, "Pass", 10, 4, x=50, y=50, ex=65, ey=50, outcome=0),                       # id 2 aussi côté équipe 2
        _event(5, 2, 21, 3, 13, "MissedShots", 10, 8, shot=True),    # dans les 10 s
        _event(6, 2, 21, 4, 16, "Goal", 10, 20, shot=True),          # hors fenêtre
        # Duel 2 : corner de l'équipe 1, gagné par son attaquant, puis but
        _event(7, 1, 10, 5, 1, "Pass", 30, 0, x=100, y=0, ex=94, ey=45),
        _event(8, 1, 12, 6, 44, "Aerial", 30, 2, x=94, y=45),
        _event(9, 2, 22, 7, 44, "Aerial", 30, 2, x=6, y=55),
        _event(10, 1, 12, 8, 16, "Goal", 30, 5, x=90, y=50, shot=True, outcome=1),
    ]
    # (ligne, type d'événement, qualifier) : 2 centre, 6 corner, 285/286 rôle, 3 passe de la tête, 15 tir de la tête
    quals = [
        (1, "1", "2"), (2, "44", "286"), (3, "44", "285"), (4, "1", "3"),
        (7, "1", "6"), (8, "44", "286"), (9, "44", "285"), (10, "16", "15"),
    ]
    q_sql = ",".join(f"('m1',{r},'{t}','{q}',NULL)" for r, t, q in quals)

    con = duckdb.connect()
    con.execute("create schema intermediate")
    con.execute(f"""create table intermediate.intermediate_whoscored_events_enriched as
        select * from (values {','.join(events)}) t({EVENT_COLS})""")
    con.execute(f"""create table intermediate.intermediate_whoscored_event_qualifiers as
        select * from (values {q_sql}) t(str_match_id, int_row_num, str_type_id, str_qual_type_id, str_qual_value)""")
    con.execute("""create table intermediate.intermediate_duel_pairs as select * from (values
        ('m1','2023-2024','Premier League',10,2,'44','Aerial',80.0,50.0, 2,1,11,0, 1,2,20,1),
        ('m1','2023-2024','Premier League',30,2,'44','Aerial',94.0,45.0, 6,1,12,1, 7,2,22,0)
        ) t(str_match_id,str_season,str_league_source,int_expanded_minute,int_second,str_duel_type_id,
            str_duel_type,dec_x,dec_y,int_event_id_a,int_team_id_a,int_player_id_a,int_outcome_a,
            int_event_id_b,int_team_id_b,int_player_id_b,int_outcome_b)""")

    class This:
        database = "memory"

    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: f"intermediate.{n}", var=lambda k, d=None: d,
        is_incremental=lambda: False, this=This(), season_match_ids=lambda r: "SELECT NULL")
    df = con.execute(sql).fetchdf()
    return df.set_index(["str_player_id"], drop=False)


def test_deux_lignes_par_duel_et_cle_unique(result):
    assert len(result) == 4
    key = ["str_match_id", "str_team_id", "int_event_id", "int_opp_event_id"]
    assert not result.duplicated(subset=key).any()


def test_centre_exterieur_perdu_par_l_attaquant(result):
    r = result.loc["11"]
    assert r.str_ball_type == "centre" and r.str_cross_lane == "exterieur"
    assert r.str_role == "offensif" and not r.bool_is_won
    assert r.str_opp_player_id == "20" and bool(r.bool_source_is_own_team)
    assert r.str_source_event_type == "Pass" and r.str_source_player_id == "10"
    assert r.dec_source_end_dist == pytest.approx(0.0)


def test_defenseur_gagnant_et_conclusion(result):
    r = result.loc["20"]
    assert r.bool_is_won and r.str_role == "defensif" and r.str_opp_player_id == "11"
    assert r.str_next_event_type == "Pass"           # id 2 de l'équipe 2, pas l'aérien de l'équipe 1
    assert r.int_own_shots_10s == 1 and not r.bool_own_goal_10s   # le but à 20 s est hors fenêtre
    assert result.loc["11"].int_opp_shots_10s == 1


def test_corner_suivi_d_un_but(result):
    r = result.loc["12"]
    assert r.str_ball_type == "corner" and r.bool_ball_corner and r.bool_is_won
    assert r.bool_own_goal_10s and not r.bool_opp_goal_10s
    assert result.loc["22"].bool_opp_goal_10s
    assert r.str_cross_lane is None or r.str_cross_lane != r.str_cross_lane  # NULL hors centre


def test_geste_suivant_du_gagnant(result):
    w, l = result.loc["20"], result.loc["11"]
    assert w.str_next_event_player_id == "20" and w.bool_next_event_is_own_player
    assert not l.bool_next_event_is_own_player and l.str_next_event_player_id == "20"
    assert w.int_duel_last_row_num == 3 and w.int_next_event_row_num == 4
    assert not w.bool_next_event_success                  # passe de la tête ratée (issue 0)
    assert w.bool_next_event_is_header
    assert w.dec_next_event_progress == pytest.approx(15.0)
    c = result.loc["12"]
    assert c.str_next_event_type == "Goal" and c.bool_next_event_success and c.bool_next_event_is_header
