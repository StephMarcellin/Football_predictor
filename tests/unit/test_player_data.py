"""Tests de app/player_data.py (catalogue, infos, stats de saison, joueurs similaires, types) sur une base DuckDB synthétique."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import app.player_data as pdta  # noqa: E402

ES_INT = ["int_n_shots", "int_n_assists", "int_n_key_passes", "int_n_chances_created", "int_n_progressive_passes",
          "int_n_touches", "int_n_tackles", "int_n_tackles_won", "int_n_interceptions", "int_n_clearances",
          "int_n_ball_recoveries", "int_n_aerial_duels", "int_n_aerial_won", "int_n_yellow_cards", "int_n_red_cards"]
SEASON = "2023-2024"


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    for sch in ("intermediate", "silver", "gold", "machine_learning"):
        c.execute(f"CREATE SCHEMA {sch}")
    ev = []
    for m in range(2):                                    # joueur 1 : 2 matchs, 90 min
        e = dict(str_match_id=f"m{m}", str_team_id="T1", str_player_id="1", str_season=SEASON, str_league_source="L1",
                 int_minutes_played=90, dec_xg_contribution=0.5)
        e.update({k: 0 for k in ES_INT}); e.update(int_n_shots=3, int_n_assists=1, int_n_tackles=2, int_n_tackles_won=1)
        ev.append(e)
    e0 = dict(ev[0], str_match_id="m9", str_player_id="2", str_team_id="T2", int_minutes_played=0)   # 0 minute : ignoré
    ev.append(e0)
    c.register("df", pd.DataFrame(ev)); c.execute("CREATE TABLE intermediate.intermediate_player_match_event_stats AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(str_match_id=f"m{m}", str_team_id="T1", str_player_id="1", int_passes_open=40,
                                        int_passes_open_completed=30, dec_xt_created=0.4) for m in range(2)]))
    c.execute("CREATE TABLE intermediate.intermediate_player_match_passes AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(str_match_id="m0", str_team_id="T1", str_player_id="1", int_shots_on_target=2)]))
    c.execute("CREATE TABLE intermediate.intermediate_player_match_finishing AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(str_player_id="1", str_season=SEASON, str_team_id="T1", str_team_name="Club Un",
                                        str_player_name="Joueur Un", str_macro_position="ATT", int_height=180, int_weight=75,
                                        int_n_matchs=2, int_n_starts=2)]))
    c.execute("CREATE TABLE intermediate.intermediate_whoscored_player_team_season AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(str_match_id=f"m{i}", str_team_id="T1", str_player_id="1", str_season=SEASON,
                                        str_role_fin_resolved="ST") for i in range(2)]))
    c.execute("CREATE TABLE intermediate.intermediate_player_match_role AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(str_match_id="m0", str_team_id="T1", str_player_id="1", str_season=SEASON,
                                        bool_is_goal=g, bool_is_own_goal=o) for g, o in
                                   [(True, False), (True, False), (True, True), (False, False)]]))
    c.execute("CREATE TABLE intermediate.intermediate_shots AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(str_match_id="m0", str_ws_match_id="900", str_season=SEASON),
                                   dict(str_match_id="m1", str_ws_match_id="901", str_season=SEASON)]))
    c.execute("CREATE TABLE intermediate.intermediate_whoscored_match_bridge AS SELECT * FROM df")
    c.register("df", pd.DataFrame([dict(ws_match_id="900", player_id=1, age=24), dict(ws_match_id="901", player_id=1, age=0)]))
    c.execute("CREATE TABLE silver.stg_whoscored_player_match AS SELECT * FROM df")
    # profils : 6 attaquants (2 ligues), 1 latéral, 1 gardien ; z construits pour que 1 soit proche de 2 et 3, loin de 4
    names = [n for ns in pdta.tt.FAMILIES.values() for n in ns]
    rng = np.random.default_rng(0)
    base = {"1": 0.0, "2": 0.2, "3": 0.5, "4": 3.0, "5": 0.3, "6": -0.4}
    league = {"1": "L1", "2": "L1", "3": "L2", "4": "L2", "5": "L3", "6": "L3"}
    rows = []
    for pid, v in base.items():
        r = dict(str_player_id=pid, str_season=SEASON, str_macro_role="ST", str_league_source=league[pid], int_minutes=2000,
                 int_n_matches=30, bool_is_peer=True)
        r.update({f"dec_z_{n}": v + rng.normal(0, 0.01) for n in names})
        r.update({f"dec_sec_{d}_{s}": 1 / 9 for d, _ in pdta.SECTORS for s, _ in pdta.SIDES})
        for f in list(pdta.tt.FAMILIES) + ["global"]:
            r[f"int_type_{f}"], r[f"dec_dist_{f}"] = (1 if pid != "4" else 0), 0.5
        rows.append(r)
    for pid, role in (("7", "FB"), ("8", "GK")):
        r = dict(rows[0], str_player_id=pid, str_macro_role=role, bool_is_peer=(role != "GK"))
        rows.append(r)
    c.register("df", pd.DataFrame(rows))
    sel = ", ".join(f"p.{c_}" for c_ in pd.DataFrame(rows).columns if not c_.startswith("int_type_") and not c_.startswith("dec_dist_"))
    c.execute(f"CREATE TABLE gold.gold_player_behaviour_profile AS SELECT {sel} FROM df p")
    tcols = ["str_player_id", "str_season", "str_macro_role"] + [x for f in list(pdta.tt.FAMILIES) + ["global"]
                                                                  for x in (f"int_type_{f}", f"dec_dist_{f}")]
    c.execute(f"CREATE TABLE machine_learning.player_trait_types AS SELECT {', '.join(tcols)} FROM df")
    lab = [dict(str_family=f, int_type=k, str_description="haut : aerial_win_rate +1.0 | bas : tackles -0.5")
           for f in list(pdta.tt.FAMILIES) + ["global"] for k in range(3)]
    c.register("df", pd.DataFrame(lab))
    c.execute("CREATE TABLE machine_learning.player_trait_type_labels AS SELECT * FROM df")
    return c


@pytest.fixture(scope="module")
def cat(con):
    # catalogue : joueurs 1 à 6 en 3 championnats (le catalogue réel vient de event_stats ; ici on le construit à la main)
    rows = [dict(season=SEASON, league=l, team_id=f"T{i}", team=f"Club {i}", player_id=str(i), player=f"Joueur {i}",
                 minutes=2000.0, matches=30) for i, l in
            [(1, "L1"), (2, "L1"), (3, "L2"), (4, "L2"), (5, "L3"), (6, "L3"), (7, "L1"), (8, "L1")]]
    return pd.DataFrame(rows)


def test_catalogue_ignore_les_matchs_sans_minute(con):
    c = pdta.load_catalog(con)
    assert len(c) == 1 and c.iloc[0].player == "Joueur Un" and c.iloc[0].minutes == 180 and c.iloc[0].matches == 2


def test_infos_age_taille_poste(con):
    i = pdta.player_info(con, "1", SEASON, "T1")
    assert (i["height"], i["weight"], i["starts"], i["role_fin"], i["age"]) == (180, 75, 2, "ST", 24)   # âge 0 ignoré


def test_stats_saison_buts_sans_csc_et_cumuls(con):
    s = pdta.season_stats(con, "1", SEASON, "T1")
    assert s["goals"] == 2                       # 3 buts dont 1 csc
    assert s["shots"] == 6 and s["assists"] == 2 and s["passes_open"] == 80 and s["shots_on_target"] == 2
    t = pdta.stats_table(s, starts=2).set_index("Statistique")
    assert t.loc["Précision des passes (%)", "Total"] == "75.0" and t.loc["Buts (penaltys compris)", "Par 90 min"] == "1.00"
    assert t.loc["Tacles gagnés / tentés", "Total"] == "2 / 4"


def test_joueur_sans_ligne_donne_des_zeros(con):
    s = pdta.season_stats(con, "99", SEASON, "T1")
    assert s["goals"] == 0 and s["minutes"] == 0 and pdta.stats_table(s).loc[3, "Par 90 min"] == "–"


def test_joueurs_similaires_un_par_championnat(con, cat):
    prof = pdta.load_profiles(con)
    sim = pdta.similar_players(prof, cat, "1", SEASON)
    assert sorted(sim.league) == ["L1", "L2", "L3"] and "1" not in set(sim.str_player_id)
    best = sim.set_index("league").player
    assert best["L1"] == "Joueur 2" and best["L2"] == "Joueur 3" and best["L3"] == "Joueur 5"   # 5 (0,3) plus proche que 6 (-0,4)
    assert (sim.percentile > 0).all()


def test_joueurs_similaires_meme_poste_et_gardiens_exclus(con, cat):
    prof = pdta.load_profiles(con)
    same = pdta.similar_players(prof, cat, "1", SEASON)
    assert set(same.str_macro_role) == {"ST"} and "7" not in set(same.str_player_id)
    tous = pdta.similar_players(prof, cat, "1", SEASON, same_role=False, per_league=5)
    assert "7" in set(tous.str_player_id)                   # le latéral entre quand on ouvre aux autres postes
    assert "8" not in set(tous.str_player_id)               # un gardien n'est jamais proposé
    assert len(tous[tous.league == "L1"]) == 2              # 2 et 7 ; 1 (le joueur) et 8 (gardien) exclus


def test_types_et_traduction(con):
    prof, lab = pdta.load_profiles(con), pdta.load_type_labels(con)
    row = prof[prof.str_player_id == "1"].iloc[0]
    t = pdta.player_types(row, lab)
    assert len(t) == len(pdta.tt.FAMILIES) + 1 and "Duels aériens gagnés (%)" in t.Description.iloc[0]
    assert "aerial_win_rate" not in t.Description.iloc[0] and "valeurs les plus hautes" in t.Description.iloc[0]
    assert len(pdta.family_scores(row)) == sum(len(v) for v in pdta.tt.FAMILIES.values())


def test_traduction_ne_touche_pas_les_mots_composes():
    assert pdta.translate_description("tackles +1.0") == "Tacles +1.0"
    assert pdta.translate_description("tackle_win_rate +1.0") == "Tacles gagnés (%) +1.0"
