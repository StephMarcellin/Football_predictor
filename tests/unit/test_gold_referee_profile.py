"""Tests de gold_referee_profile (net de la ligue-saison, rétrécissement, fenêtre) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/gold/gold_referee_profile.sql"


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    if rows:
        df = pd.DataFrame(rows)[list(schema)]
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    # m1 (arbitre 1) et m2 (arbitre 2) dans la même ligue-saison ; m3 (arbitre 1) la saison suivante
    matches = [("m1", "w1", "2019-2020", 1, "1 : 0"), ("m2", "w2", "2019-2020", 2, "0 : 0"),
               ("m3", "w3", "2020-2021", 1, "2 : 2")]
    _table(con, "bridge", dict(str_match_id="VARCHAR", str_ws_match_id="VARCHAR", str_league_source="VARCHAR",
                               str_season="VARCHAR", str_ws_home_team_id="VARCHAR"),
           [dict(str_match_id=m, str_ws_match_id=w, str_league_source="PL", str_season=s, str_ws_home_team_id="100")
            for m, w, s, _, _ in matches])
    _table(con, "meta", dict(ws_match_id="VARCHAR", referee_id="VARCHAR", referee_name="VARCHAR", ft_score="VARCHAR"),
           [dict(ws_match_id=w, referee_id=str(r), referee_name=f"Arbitre {r}", ft_score=sc) for _, w, _, r, sc in matches])

    def cards(match, season, team, yellow, red=0):
        base = dict(str_match_id=match, str_season=season, str_team_id=str(team))
        return ([dict(base, str_card_type="Yellow")] * yellow) + ([dict(base, str_card_type="Red")] * red)

    ev = (cards("m1", "2019-2020", 100, 4) + cards("m1", "2019-2020", 200, 2)        # 6 jaunes, domicile 4 / extérieur 2
          + cards("m2", "2019-2020", 100, 1) + cards("m2", "2019-2020", 200, 1)      # 2 jaunes
          + cards("m3", "2020-2021", 100, 3, red=1))
    _table(con, "ev", dict(str_match_id="VARCHAR", str_season="VARCHAR", str_team_id="VARCHAR", str_card_type="VARCHAR"), ev)

    def fouls(match, n, pen=0):
        return [dict(str_match_id=match, str_season="2019-2020" if match != "m3" else "2020-2021",
                     str_duel_family="faute", str_role="auteur", bool_leads_to_penalty=i < pen) for i in range(n)]
    fl = fouls("m1", 20, pen=1) + fouls("m2", 10) + fouls("m3", 14)
    _table(con, "g", dict(str_match_id="VARCHAR", str_season="VARCHAR", str_duel_family="VARCHAR", str_role="VARCHAR",
                          bool_leads_to_penalty="BOOLEAN"), fl)
    refs = {"intermediate_whoscored_match_bridge": "bridge", "intermediate_whoscored_events_enriched": "ev",
            "intermediate_ground_duel_players": "g"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n], source=lambda a, b: "meta")
    return con.execute(sql).df().set_index(["str_referee_id", "str_season"])


def test_cle_unique_et_fenetre(res):
    assert not res.index.duplicated().any()
    w = res.loc[("1", "2020-2021")]
    assert (w.int_n_seasons_window, w.int_n_matches) == (2, 2)
    assert res.loc[("1", "2019-2020")].int_n_matches == 1


def test_moyennes_brutes_par_match(res):
    r = res.loc[("1", "2019-2020")]
    assert (r.dec_yellow_per_match, r.dec_fouls_per_match, r.dec_penalties_per_match) == (6, 20, 1)
    w = res.loc[("1", "2020-2021")]
    assert w.dec_yellow_per_match == pytest.approx(4.5)          # (6 + 3) / 2
    assert w.dec_red_per_match == pytest.approx(0.5)


def test_net_de_la_ligue_saison_et_retrecissement(res):
    # 2019-2020 : moyenne de la ligue = (6 + 2) / 2 = 4 jaunes ; l'arbitre 1 sort + 2, rétréci par K = 10 matchs
    assert res.loc[("1", "2019-2020")].dec_yellow_net == pytest.approx(2 / (1 + 10))
    assert res.loc[("2", "2019-2020")].dec_yellow_net == pytest.approx(-2 / (1 + 10))
    # fautes : moyenne (20 + 10) / 2 = 15
    assert res.loc[("1", "2019-2020")].dec_fouls_net == pytest.approx(5 / 11)


def test_avantage_du_domicile_en_buts_et_en_jaunes(res):
    # m1 : écart de buts +1 et de jaunes +2 ; m2 : 0 et 0 ; moyennes de la ligue 0,5 et 1
    r = res.loc[("1", "2019-2020")]
    assert r.dec_home_goal_diff_net == pytest.approx(0.5 / 11)
    assert r.dec_home_yellow_diff_net == pytest.approx(1 / 11)
