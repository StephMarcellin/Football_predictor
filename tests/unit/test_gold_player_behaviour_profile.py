"""Tests de gold_player_behaviour_profile (fenêtre, pairs, z-scores, rétrécissement) sur données synthétiques."""
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")

SQL_PATH = Path(__file__).resolve().parents[2] / "dbt_project/models/gold/gold_player_behaviour_profile.sql"

ES_INT = ["int_n_shots", "int_n_key_passes", "int_n_chances_created", "int_n_progressive_passes", "int_n_crosses",
          "int_n_throughballs", "int_n_longballs", "int_n_tackles", "int_n_tackles_won", "int_n_interceptions",
          "int_n_ball_recoveries", "int_n_clearances", "int_n_challenges", "int_n_aerial_duels", "int_n_aerial_won",
          "int_n_touches", "int_n_yellow_cards"]
ZONES = [f"dec_pct_z{z}_c{c}" for z in range(1, 6) for c in range(1, 6)]
PASS_COLS = ["int_passes_open", "int_passes_open_completed", "int_passes_short", "int_passes_long",
             "int_passes_long_completed", "int_passes_forward", "int_passes_forward_completed", "int_passes_backward",
             "int_passes_from_own_third", "int_passes_from_attacking_third", "int_passes_into_final_third"]
G_COLS = ["dec_dribbles_per_90", "dec_dribble_woe_adj", "dec_dribble_win_rate", "dec_defended_woe_adj",
          "dec_dispossessed_share", "dec_fouls_committed_per_90", "dec_fouls_drawn_per_90", "dec_yellow_per_foul"]
CM_COLS = ["int_def_actions", "int_def_actions_own_box", "int_shots_blocked", "int_passes_blocked",
           "int_crosses_blocked", "int_last_man_actions"]
A_COLS = ["dec_woe_net_off", "dec_woe_net_def", "dec_duels_per_match", "dec_head_shots_per_match"]


def _load(con, name, df):
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE {name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def _build(rows):
    """rows : (joueur, saison, match, tirs, passes de jeu ouvert). 90 minutes, rôle ST, même ligue."""
    con = duckdb.connect()
    es, ps, ro, cm = [], [], [], []
    for player, season, match, shots, passes in rows:
        key = dict(str_match_id=match, str_team_id="1", str_player_id=player)
        e = dict(key, str_season=season, str_league_source="L1", int_minutes_played=90, dec_xg_contribution=0.0)
        e.update({c: 0 for c in ES_INT})
        e.update({c: 0.0 for c in ZONES})
        e["int_n_shots"], e["int_n_touches"], e["dec_pct_z5_c3"] = shots, 50, 0.5
        es.append(e)
        p = dict(key)
        p.update({c: 0 for c in PASS_COLS})
        p.update(int_passes_open=passes, int_passes_open_completed=passes, dec_pass_length_mean=15.0, dec_xt_created=0.0)
        ps.append(p)
        ro.append(dict(key, str_season=season, str_role_fin_resolved="ST"))
        cm.append(dict(key, **{c: 0 for c in CM_COLS}))
    _load(con, "es", pd.DataFrame(es))
    _load(con, "ps", pd.DataFrame(ps))
    _load(con, "ro", pd.DataFrame(ro))
    _load(con, "cm", pd.DataFrame(cm))
    players = sorted({r[0] for r in rows})
    seasons = sorted({r[1] for r in rows})
    gp = pd.DataFrame([dict(str_player_id=p, str_season=s, str_player_name=f"nom_{p}", **{c: None for c in G_COLS})
                       for p in players for s in seasons])
    ap = pd.DataFrame([dict(str_player_id=p, str_season=s, str_player_name=f"nom_{p}", **{c: None for c in A_COLS})
                       for p in players for s in seasons])
    for df, cols in ((gp, G_COLS), (ap, A_COLS)):
        for c in cols:
            df[c] = df[c].astype("float64")
    _load(con, "gp", gp)
    _load(con, "ap", ap)
    refs = {"intermediate_player_match_event_stats": "es", "intermediate_player_match_passes": "ps",
            "intermediate_player_match_role": "ro", "intermediate_player_match_commitment": "cm", "gold_player_ground_profile": "gp", "gold_player_aerial_profile": "ap"}
    sql = jinja2.Environment().from_string(SQL_PATH.read_text(encoding="utf-8")).render(
        config=lambda **k: "", ref=lambda n: refs[n])
    return con.execute(sql).fetchdf()


def _season_rows(season, shots_by_player, n_matches=6, passes=40):
    return [(p, season, f"{season}-{p}-{i}", shots, passes) for p, shots in shots_by_player.items()
            for i in range(n_matches)]


@pytest.fixture(scope="module")
def res():
    rows = _season_rows("2020-2021", {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4})
    return _build(rows).set_index("str_player_id")


def test_les_z_scores_sont_centres_et_ordonnes(res):
    z = res["dec_z_shots"]
    assert z.sum() == pytest.approx(0, abs=1e-9)
    assert list(z.sort_values().index) == ["a", "b", "c", "d", "e"]
    assert z.std(ddof=0) == pytest.approx(1.0)


def test_le_retrecissement_ramene_vers_la_moyenne(res):
    # 540 minutes = 6 « 90 minutes » ; K = 10 : e passe de 4 tirs par 90 à (24 + 10 × 2) / 16 = 2,75
    assert res.loc["e", "dec_shots"] == pytest.approx(2.75)
    assert res.loc["a", "dec_shots"] == pytest.approx(1.25)


def test_la_part_du_secteur_et_le_poste(res):
    assert res.loc["a", "str_macro_role"] == "ST"
    assert res.loc["a", "dec_sec_att_centre"] == pytest.approx(1.0)  # toutes les touches localisées sont en z5_c3
    assert res.loc["a", "dec_sec_def_left"] == pytest.approx(0.0)
    assert res["dec_pass_accuracy"].between(0, 1).all()


def test_une_ligne_seulement_si_le_joueur_joue_dans_la_saison():
    rows = _season_rows("2020-2021", {"a": 1, "b": 2, "c": 3}) + _season_rows("2021-2022", {"a": 1, "b": 2})
    out = _build(rows)
    keys = set(zip(out.str_player_id, out.str_season))
    assert ("c", "2021-2022") not in keys
    assert ("a", "2021-2022") in keys and ("c", "2020-2021") in keys
    # la fenêtre de 2021-2022 contient 2020-2021 : 12 matchs pour a
    assert out[(out.str_player_id == "a") & (out.str_season == "2021-2022")].int_n_matches.iloc[0] == 12
