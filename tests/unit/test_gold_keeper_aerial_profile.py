"""Tests de gold_keeper_aerial_profile (fenêtre de 3 saisons, part de captés rétrécie) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/gold/gold_keeper_aerial_profile.sql"


def _table(con, name, schema, rows):
    con.execute(f"CREATE TABLE {name} ({', '.join(f'{c} {t}' for c, t in schema.items())})")
    if rows:
        df = pd.DataFrame(rows)[list(schema)]
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")


def _events(pid, season, claims, punches, sweeps):
    rows = [dict(str_player_id=pid, str_season=season, str_type_name="Claim")] * claims
    rows += [dict(str_player_id=pid, str_season=season, str_type_name="Punch")] * punches
    rows += [dict(str_player_id=pid, str_season=season, str_type_name="KeeperSweeper")] * sweeps
    return rows


@pytest.fixture(scope="module")
def res():
    con = duckdb.connect()
    pts = [dict(str_player_id="k1", str_season=s, str_macro_position="GK", int_n_matchs=30, str_player_name="K1", int_height=190)
           for s in ("2018-2019", "2019-2020", "2020-2021")]
    pts += [dict(str_player_id="k2", str_season="2020-2021", str_macro_position="GK", int_n_matchs=10, str_player_name="K2", int_height=140)]
    pts += [dict(str_player_id="d1", str_season="2020-2021", str_macro_position="DEF", int_n_matchs=30, str_player_name="D1", int_height=185)]
    _table(con, "pts", dict(str_player_id="VARCHAR", str_season="VARCHAR", str_macro_position="VARCHAR", int_n_matchs="INTEGER",
                            str_player_name="VARCHAR", int_height="DOUBLE"), pts)
    ev = (_events("k1", "2018-2019", 20, 10, 6) + _events("k1", "2019-2020", 30, 10, 9) + _events("k1", "2020-2021", 10, 20, 12)
          + _events("k2", "2020-2021", 4, 0, 0)
          + [dict(str_player_id="d1", str_season="2020-2021", str_type_name="Claim")] * 5)     # un défenseur : ignoré
    _table(con, "ev", dict(str_player_id="VARCHAR", str_season="VARCHAR", str_type_name="VARCHAR"), ev)
    refs = {"intermediate_whoscored_player_team_season": "pts", "intermediate_whoscored_events": "ev"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(config=lambda **k: "", ref=lambda n: refs[n])
    return con.execute(sql).df().set_index(["str_player_id", "str_season"])


def test_cle_unique_et_seuls_les_gardiens(res):
    assert not res.index.duplicated().any()
    assert "d1" not in res.index.get_level_values(0)


def test_fenetre_de_trois_saisons_cumule_les_actions(res):
    r = res.loc[("k1", "2020-2021")]
    assert (r.int_n_seasons_window, r.int_n_matches) == (3, 90)
    assert (r.int_n_claims, r.int_n_punches, r.int_n_sweeps) == (60, 40, 27)
    assert r.dec_aerial_actions_per_match == pytest.approx(100 / 90)
    assert r.dec_sweeper_per_match == pytest.approx(27 / 90)
    first = res.loc[("k1", "2018-2019")]
    assert first.int_n_seasons_window == 1 and first.int_n_claims == 20


def test_part_de_captes_retrecie_vers_la_moyenne(res):
    k1 = res.loc[("k1", "2020-2021")].dec_claim_share
    k2 = res.loc[("k2", "2020-2021")]
    assert 0.5 < k1 < 0.65                          # 60 captés sur 100 actions : proche de 0,6 mais tirée vers la moyenne
    assert k2.dec_claim_share < 1.0 and k2.dec_claim_share > k1   # 4 sur 4 : fortement rétrécie, jamais 1
    assert pd.isna(k2.int_height)                   # 140 cm : valeur aberrante (<= 150), traitée comme manquante
    assert res.loc[("k1", "2020-2021")].int_height == 190
