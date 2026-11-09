"""Tests des colonnes ajoutées à gold_player_aerial_profile (compétence nette de la taille, têtes CPA / jeu ouvert).

Le SQL est rendu avec Jinja (ref simulé) puis exécuté dans DuckDB en mémoire sur des tables synthétiques.
Monde : la victoire d'un duel ne dépend QUE de l'écart de taille. Les attaquants grands « gagnent plus que
l'attendu » en brut, mais pas net de la taille.
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = (Path(__file__).resolve().parents[2] / "dbt_project/models/gold/gold_player_aerial_profile.sql")
SEASON = "2016-2017"


def _table(con, name, schema, df):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    if len(df):
        con.register("tmp_df", df[list(schema)])
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")


@pytest.fixture(scope="module")
def profil():
    rng = np.random.default_rng(5)
    n_att, n_def = 30, 30
    h_att = np.linspace(172, 198, n_att).round()
    h_def = np.linspace(172, 198, n_def).round()
    rows = []
    k = 0
    for _ in range(24000):
        a, d = int(rng.integers(n_att)), int(rng.integers(n_def))
        p = 1 / (1 + np.exp(-0.09 * (h_att[a] - h_def[d])))
        won = rng.random() < p
        bt = str(rng.choice(["corner", "centre", "longue_balle"]))
        for pl, opp, role, w in ((f"a{a}", f"d{d}", "offensif", won), (f"d{d}", f"a{a}", "defensif", not won)):
            rows.append(dict(str_player_id=pl, str_team_id="t", str_season=SEASON, str_match_id=f"m{k}", str_role=role,
                             str_ball_type=bt, bool_is_won=bool(w), int_own_shots_10s=0, bool_next_event_is_own_player=False,
                             str_next_event_type=None, bool_next_event_success=None, dec_next_event_progress=None,
                             int_next_event_row_num=None, str_opp_player_id=opp))
        k += 1
    duels = pd.DataFrame(rows)
    players = pd.DataFrame(
        [dict(str_player_id=f"a{i}", str_season=SEASON, int_n_matchs=30, str_player_name=f"A{i}", str_macro_position="ATT",
              int_height=float(h_att[i])) for i in range(n_att)]
        + [dict(str_player_id=f"d{i}", str_season=SEASON, int_n_matchs=30, str_player_name=f"D{i}", str_macro_position="DEF",
                int_height=float(h_def[i])) for i in range(n_def)])
    # tirs de la tête du joueur a0 : 3 sur coup de pied arrêté, 2 en jeu ouvert ; une tête non tirée (pas dans shots)
    shots = pd.DataFrame([dict(str_match_id="s", str_player_id="a0", str_season=SEASON, int_row_num=i,
                               bool_is_goal=False, bool_is_own_goal=False) for i in range(1, 6)])
    quals = pd.DataFrame([dict(str_match_id="s", int_row_num=i, str_qual_type_id="15", str_season=SEASON) for i in range(1, 6)]
                         + [dict(str_match_id="s", int_row_num=1, str_qual_type_id="24", str_season=SEASON),
                            dict(str_match_id="s", int_row_num=2, str_qual_type_id="25", str_season=SEASON),
                            dict(str_match_id="s", int_row_num=3, str_qual_type_id="25", str_season=SEASON),
                            dict(str_match_id="s", int_row_num=4, str_qual_type_id="22", str_season=SEASON)])
    con = duckdb.connect()
    _table(con, "aerial_duels", dict(str_player_id="VARCHAR", str_team_id="VARCHAR", str_season="VARCHAR", str_match_id="VARCHAR",
                                     str_role="VARCHAR", str_ball_type="VARCHAR", bool_is_won="BOOLEAN", int_own_shots_10s="INTEGER",
                                     bool_next_event_is_own_player="BOOLEAN", str_next_event_type="VARCHAR",
                                     bool_next_event_success="BOOLEAN", dec_next_event_progress="DOUBLE",
                                     int_next_event_row_num="BIGINT", str_opp_player_id="VARCHAR"), duels)
    _table(con, "xt", dict(str_match_id="VARCHAR", int_row_num="BIGINT", dec_xt_added="DOUBLE"), pd.DataFrame())
    _table(con, "players", dict(str_player_id="VARCHAR", str_season="VARCHAR", int_n_matchs="INTEGER", str_player_name="VARCHAR",
                                str_macro_position="VARCHAR", int_height="DOUBLE"), players)
    _table(con, "shots", dict(str_match_id="VARCHAR", str_player_id="VARCHAR", str_season="VARCHAR", int_row_num="BIGINT",
                              bool_is_goal="BOOLEAN", bool_is_own_goal="BOOLEAN"), shots)
    _table(con, "quals", dict(str_match_id="VARCHAR", int_row_num="BIGINT", str_qual_type_id="VARCHAR", str_season="VARCHAR"), quals)
    refs = {"intermediate_aerial_duel_players": "aerial_duels", "intermediate_xt_contributions": "xt",
            "intermediate_whoscored_player_team_season": "players", "intermediate_shots": "shots",
            "intermediate_whoscored_event_qualifiers": "quals"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(config=lambda **k: "", ref=lambda n: refs[n])
    return con.execute(sql).df()


def test_cle_unique_et_colonnes_ajoutees(profil):
    assert not profil.duplicated(["str_player_id", "str_season"]).any()
    for c in ["dec_woe_net", "dec_woe_net_off", "dec_woe_net_def", "dec_height_diff_mean",
              "dec_head_shots_setpiece_per_match", "dec_head_shots_open_per_match"]:
        assert c in profil.columns


def test_la_competence_nette_retire_l_effet_de_la_taille(profil):
    att = profil[profil.str_player_id.str.startswith("a")]
    assert att.dec_woe_off.corr(att.int_height) > 0.8            # brut : les grands « gagnent plus que l'attendu »
    assert abs(att.dec_woe_net_off.corr(att.int_height)) < 0.5   # net : la taille ne compte plus
    assert att.dec_woe_net_off.std() < att.dec_woe_off.std() / 2
    dfd = profil[profil.str_player_id.str.startswith("d")]
    assert dfd.dec_woe_def.corr(dfd.int_height) > 0.8
    assert dfd.dec_woe_net_def.std() < dfd.dec_woe_def.std() / 2


def test_ecart_de_taille_moyen_est_celui_des_adversaires(profil):
    r = profil.set_index("str_player_id")
    # adversaires répartis de 172 à 198 cm (moyenne 185) : le plus grand attaquant (198) a un écart moyen d'environ +13
    assert r.loc["a29", "dec_height_diff_mean"] == pytest.approx(13, abs=2)
    assert r.loc["a0", "dec_height_diff_mean"] == pytest.approx(-13, abs=2)
    assert abs(r.loc["a15", "dec_height_diff_mean"]) < 3


def test_tetes_separees_coup_de_pied_arrete_et_jeu_ouvert(profil):
    r = profil.set_index("str_player_id").loc["a0"]
    assert r.int_n_head_shots == 5
    assert (r.int_n_head_shots_setpiece, r.int_n_head_shots_open) == (3, 2)
    assert r.dec_head_shots_setpiece_per_match == pytest.approx(3 / 30)
    assert r.dec_head_shots_open_per_match == pytest.approx(2 / 30)
