"""
features/rolling.py — Rolling Feature Engineering (FBref / Understat / WhoScored)
==================================================================================
Ex-03_features.py, déplacé dans le package features/.

Transforme les tables silver.* en features gold.* via 3 blocs :
  Bloc 1 — Staging       : jointures multi-sources, casting, corrections Home/Away
  Bloc 2 — Rolling       : fenêtres glissantes [3, 5, 10] matchs
  Bloc 3 — Match-up      : fusion team × opponent, différentiels, H2H, Giant Killer

ANTI-LEAKAGE :
  Toutes les window functions utilisent ROWS BETWEEN W PRECEDING AND 1 PRECEDING.
  Les ratings de saison sont LAG(1) sur la saison précédente.

Appelable :
  python -m features.rolling            # run complet
  python -m features.rolling --reset    # supprime et recrée le schéma gold
"""

from __future__ import annotations

import argparse
import os
import yaml
from loguru import logger
from pathlib import Path
import duckdb

# ── Config ────────────────────────────────────────────────────────────────────
os.chdir(Path(__file__).resolve().parent.parent.parent)  # racine du projet

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = CFG["paths"]["db"]

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [rolling] {message}",
)

# Fenêtres rolling
WINDOWS = [3, 5, 10]
WINDOW  = 5   # fenêtre de référence pour rétrocompatibilité Bloc 3

FRA_end_covid_date = "2020-03-08"
FRA_covid_Season   = "2019-2020"


# ── Générateurs de colonnes rolling (internes) ───────────────────────────────

def _frames(w: int) -> tuple[str, str]:
    fg = (f"PARTITION BY team, season, league_source "
          f"ORDER BY date ROWS BETWEEN {w} PRECEDING AND 1 PRECEDING")
    fv = (f"PARTITION BY team, season, league_source, venue "
          f"ORDER BY date ROWS BETWEEN {w} PRECEDING AND 1 PRECEDING")
    return fg, fv


def _rolling_cols(w: int) -> str:
    fg, fv = _frames(w)
    return f"""
            -- ══ ROLLING W={w} ══════════════════════════════════════════════
            AVG(np_xg)           OVER ({fg})   AS np_xg_roll_{w},
            AVG(np_xg)           OVER ({fv})   AS np_xg_roll_venue_{w},
            AVG(np_xg_conceded)  OVER ({fg})   AS np_xg_conceded_roll_{w},
            AVG(np_xg - np_xg_conceded) OVER ({fg}) AS xg_net_roll_{w},
            AVG(np_xg) OVER ({fg})
                / NULLIF(AVG(shots_total) OVER ({fg}), 0)
                AS shot_quality_ratio_{w},
            AVG(np_xg) OVER ({fv})
                / NULLIF(AVG(shots_total) OVER ({fv}), 0)
                AS shot_quality_ratio_venue_{w},
            AVG(shots_on_target) OVER ({fg})
                / NULLIF(AVG(shots_total) OVER ({fg}), 0)
                AS shot_accuracy_roll_{w},
            AVG(saves) OVER ({fg})
                / NULLIF(AVG(shots_on_target_faced) OVER ({fg}), 0)
                AS save_rate_roll_{w},
            AVG(save_pct) OVER ({fg}) AS roll_save_pct_{w},
            AVG(shots_on_target_faced) OVER ({fg}) AS roll_sota_{w},
            AVG(possession) OVER ({fg})   AS poss_roll_{w},
            AVG(possession) OVER ({fv})   AS poss_roll_venue_{w},
            AVG(ppda)         OVER ({fg}) AS ppda_roll_{w},
            AVG(ppda_allowed) OVER ({fg}) AS ppda_allowed_roll_{w},
            AVG(ppda) OVER ({fg})
                / NULLIF(AVG(ppda_allowed) OVER ({fg}), 0)
                AS ppda_ratio_roll_{w},
            AVG(tackles_won + interceptions) OVER ({fg})
                AS defensive_actions_roll_{w},
            AVG(fouls_committed) OVER ({fg})
                / NULLIF(AVG(tackles_won) OVER ({fg}), 0)
                AS fouls_per_tackle_roll_{w},
            AVG(gf) OVER ({fg})
                / NULLIF(AVG(np_xg) OVER ({fg}), 0) - 1
                AS xg_overperformance_{w},
            AVG(possession) OVER ({fg})
                / NULLIF(
                    AVG(np_xg) OVER ({fg})
                        / NULLIF(AVG(shots_total) OVER ({fg}), 0)
                , 0)
                AS sterility_index_{w},
            AVG(shots_on_target_faced) OVER ({fg})
                / NULLIF(AVG(ga) OVER ({fg}), 0)
                AS shots_faced_per_goal_conceded_{w},
            (AVG(possession) OVER ({fg}) / 50.0)
                * (AVG(possession) OVER ({fg})
                    / NULLIF(
                        AVG(np_xg) OVER ({fg})
                            / NULLIF(AVG(shots_total) OVER ({fg}), 0)
                    , 0))
                AS sterility_weighted_{w},
            AVG(possession) OVER ({fg})
                / NULLIF(AVG(ppda_allowed) OVER ({fg}), 0)
                AS press_resistance_{w},
            AVG(save_pct) OVER ({fg})
                / NULLIF(1 + AVG(np_xg_conceded) OVER ({fg}), 0)
                AS shield_efficiency_{w},
            AVG(
                CASE WHEN (red_cards + second_yellow_cards) > 0 THEN 1.0 ELSE 0.0 END
            ) OVER ({fg})
                AS red_card_rate_roll_{w},
            AVG(CASE WHEN result_1n2 = 'W' THEN 1.0 ELSE 0.0 END) OVER ({fg})
                AS win_rate_roll_{w},
            AVG(CASE
                    WHEN result_1n2 = 'W' THEN 3.0
                    WHEN result_1n2 = 'D' THEN 1.0
                    ELSE 0.0
                END) OVER ({fg})
                AS points_pg_roll_{w},
"""


def _opp_cols(w: int) -> str:
    return f"""
                np_xg_roll_{w}              AS opp_np_xg_{w},
                np_xg_roll_venue_{w}        AS opp_np_xg_venue_{w},
                np_xg_conceded_roll_{w}     AS opp_np_xg_conceded_{w},
                xg_net_roll_{w}             AS opp_xg_net_{w},
                shot_quality_ratio_{w}      AS opp_sqr_{w},
                shot_accuracy_roll_{w}      AS opp_shot_accuracy_{w},
                ppda_roll_{w}               AS opp_ppda_{w},
                ppda_allowed_roll_{w}       AS opp_ppda_allowed_{w},
                ppda_ratio_roll_{w}         AS opp_ppda_ratio_{w},
                defensive_actions_roll_{w}  AS opp_defensive_actions_{w},
                xg_overperformance_{w}      AS opp_xg_opi_{w},
                save_rate_roll_{w}          AS opp_save_rate_{w},
                roll_save_pct_{w}           AS opp_roll_save_pct_{w},
                red_card_rate_roll_{w}      AS opp_red_card_rate_{w},
                sterility_index_{w}         AS opp_sterility_index_{w},
                shots_faced_per_goal_conceded_{w} AS opp_shots_faced_per_goal_conceded_{w},
                sterility_weighted_{w}      AS opp_sterility_weighted_{w},
                press_resistance_{w}        AS opp_press_resistance_{w},
                shield_efficiency_{w}       AS opp_shield_efficiency_{w},
                win_rate_roll_{w}           AS opp_win_rate_{w},
                points_pg_roll_{w}          AS opp_points_pg_{w},
"""


def _team_cols(w: int) -> str:
    return f"""
            t.np_xg_roll_{w},
            t.np_xg_roll_venue_{w},
            t.np_xg_conceded_roll_{w},
            t.xg_net_roll_{w},
            t.shot_quality_ratio_{w},
            t.shot_quality_ratio_venue_{w},
            t.shot_accuracy_roll_{w},
            t.save_rate_roll_{w},
            t.poss_roll_{w},
            t.poss_roll_venue_{w},
            t.ppda_roll_{w},
            t.ppda_allowed_roll_{w},
            t.ppda_ratio_roll_{w},
            t.defensive_actions_roll_{w},
            t.fouls_per_tackle_roll_{w},
            t.xg_overperformance_{w},
            t.red_card_rate_roll_{w},
            t.sterility_index_{w},
            t.shots_faced_per_goal_conceded_{w},
            t.sterility_weighted_{w},
            t.press_resistance_{w},
            t.shield_efficiency_{w},
            t.roll_save_pct_{w},
            t.roll_sota_{w},
            t.win_rate_roll_{w},
            t.points_pg_roll_{w},
            o.opp_np_xg_{w},
            o.opp_np_xg_venue_{w},
            o.opp_np_xg_conceded_{w},
            o.opp_xg_net_{w},
            o.opp_sqr_{w},
            o.opp_shot_accuracy_{w},
            o.opp_ppda_{w},
            o.opp_ppda_allowed_{w},
            o.opp_ppda_ratio_{w},
            o.opp_defensive_actions_{w},
            o.opp_xg_opi_{w},
            o.opp_save_rate_{w},
            o.opp_red_card_rate_{w},
            o.opp_win_rate_{w},
            o.opp_points_pg_{w},
"""


def _diff_cols(w: int) -> str:
    return f"""
            (t.xg_net_roll_{w}             - o.opp_xg_net_{w})             AS xg_net_diff_{w},
            (t.shot_quality_ratio_{w}      - o.opp_sqr_{w})                AS sqr_diff_{w},
            (t.ppda_roll_{w}               - o.opp_ppda_{w})               AS ppda_diff_{w},
            (t.ppda_ratio_roll_{w}         - o.opp_ppda_ratio_{w})         AS ppda_ratio_diff_{w},
            (t.xg_overperformance_{w}      - o.opp_xg_opi_{w})             AS xg_opi_diff_{w},
            (t.save_rate_roll_{w}          - o.opp_save_rate_{w})          AS save_rate_diff_{w},
            (t.defensive_actions_roll_{w}  - o.opp_defensive_actions_{w})  AS defensive_actions_diff_{w},
            (t.roll_save_pct_{w}           - o.opp_roll_save_pct_{w})      AS keeper_form_diff_{w},
            (t.red_card_rate_roll_{w}      - o.opp_red_card_rate_{w})      AS red_card_rate_diff_{w},
            (o.opp_sterility_index_{w}     - t.sterility_index_{w})        AS sterility_diff_{w},
            (t.shots_faced_per_goal_conceded_{w} - o.opp_shots_faced_per_goal_conceded_{w}) AS shots_faced_per_goal_conceded_diff_{w},
            (t.sterility_weighted_{w}      - o.opp_sterility_weighted_{w}) AS sterility_weighted_diff_{w},
            (t.press_resistance_{w}        - o.opp_press_resistance_{w})   AS press_resistance_diff_{w},
            (t.shield_efficiency_{w}       - o.opp_shield_efficiency_{w})  AS shield_efficiency_diff_{w},
            (t.win_rate_roll_{w}           - o.opp_win_rate_{w})           AS win_rate_diff_{w},
            (t.points_pg_roll_{w}          - o.opp_points_pg_{w})          AS points_pg_diff_{w},
"""


# ── Pipeline principal ────────────────────────────────────────────────────────

def run_rolling_features(reset: bool = False) -> None:
    """
    Pipeline complet : Staging → Rolling → Match-up → H2H → League Draw Rate
    → Giant Killer.
    """
    logger.info("═══ Pipeline rolling — Silver → Gold (FBref/Understat/WhoScored) ═══")
    conn = duckdb.connect(DB_PATH)

    if reset:
        logger.info("Reset Gold Layer...")
        conn.execute("DROP SCHEMA IF EXISTS gold CASCADE")

    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # ── BLOC 1 : Staging ──────────────────────────────────────────────────────
    logger.info("Bloc 1 : Staging — jointures et corrections Home/Away...")

    conn.execute(f"""
    CREATE OR REPLACE TABLE gold.stg_backbone AS
    WITH
    fbref_base AS (
        SELECT
            date, team, opponent, raw_team, raw_opponent, venue, season,
            league_source, result_1n2, comp_category,
            CAST(gf AS INTEGER)                                        AS gf,
            CAST(ga AS INTEGER)                                        AS ga,
            CAST(NULLIF(TRIM(CAST(poss AS VARCHAR)), '') AS DOUBLE)    AS possession
        FROM silver.fbref_schedule
    ),
    fbref_keeper_cte AS (
        SELECT date, team, opponent, league_source,
            sota AS shots_on_target_faced, saves, save_pct,
            cs   AS clean_sheet, pk_att AS pk_faced, pk_allowed AS pk_conceded
        FROM silver.fbref_keeper
    ),
    fbref_shooting_cte AS (
        SELECT date, team, opponent, league_source,
            standard_sh      AS shots_total,
            standard_sot     AS shots_on_target,
            standard_sot_pct AS shots_on_target_pct,
            standard_g_sh    AS goals_per_shot,
            standard_pk      AS pk_goals,
            standard_pkatt   AS pk_attempts
        FROM silver.fbref_shooting
    ),
    fbref_misc_cte AS (
        SELECT date, team, opponent, league_source,
            crdy  AS yellow_cards, crdr  AS red_cards,
            crdy2 AS second_yellow_cards,
            fls   AS fouls_committed, fld AS fouls_drawn,
            off   AS offsides, crosses,
            int   AS interceptions, tklw AS tackles_won,
            pkwon AS pk_won, pkcon AS pk_conceded_misc, og AS own_goals
        FROM silver.fbref_misc
    ),
    understat_base AS (
        SELECT
            u.season, u.home_team, u.away_team, u.match_id, u.league_source,
            CAST(NULLIF(TRIM(CAST(us.home_np_xg      AS VARCHAR)), '') AS DOUBLE) AS home_np_xg,
            CAST(NULLIF(TRIM(CAST(us.away_np_xg      AS VARCHAR)), '') AS DOUBLE) AS away_np_xg,
            CAST(NULLIF(TRIM(CAST(us.home_ppda       AS VARCHAR)), '') AS DOUBLE) AS home_ppda,
            CAST(NULLIF(TRIM(CAST(us.away_ppda       AS VARCHAR)), '') AS DOUBLE) AS away_ppda,
            CAST(NULLIF(TRIM(CAST(us.home_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS home_np_xg_diff,
            CAST(NULLIF(TRIM(CAST(us.away_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS away_np_xg_diff
        FROM silver.understat_schedule u
        LEFT JOIN silver.understat_stats us ON u.match_id = us.match_id
    ),
    fbref_merged AS (
        SELECT
            b.date, b.team, b.opponent, b.raw_team, b.raw_opponent,
            b.venue, b.season, b.league_source, b.result_1n2, b.comp_category,
            b.gf, b.ga, b.possession,
            k.shots_on_target_faced, k.saves, k.save_pct, k.clean_sheet,
            k.pk_faced, k.pk_conceded,
            s.shots_total, s.shots_on_target, s.shots_on_target_pct,
            s.goals_per_shot, s.pk_goals, s.pk_attempts,
            m.yellow_cards, m.red_cards, m.second_yellow_cards,
            m.fouls_committed, m.fouls_drawn, m.offsides, m.crosses,
            m.interceptions, m.tackles_won, m.pk_won,
            m.pk_conceded_misc, m.own_goals
        FROM fbref_base b
        LEFT JOIN fbref_keeper_cte   k ON b.date=k.date AND b.team=k.team AND b.opponent=k.opponent AND b.league_source=k.league_source
        LEFT JOIN fbref_shooting_cte s ON b.date=s.date AND b.team=s.team AND b.opponent=s.opponent AND b.league_source=s.league_source
        LEFT JOIN fbref_misc_cte     m ON b.date=m.date AND b.team=m.team AND b.opponent=m.opponent AND b.league_source=m.league_source
    ),
    fbref_understat AS (
        SELECT
            f.league_source, f.season, f.venue, u.match_id,
            f.team, f.opponent, f.raw_team, f.raw_opponent, f.date,
            f.result_1n2, f.comp_category, f.gf, f.ga, f.possession,
            f.shots_on_target_faced, f.saves, f.save_pct, f.clean_sheet,
            f.shots_total, f.shots_on_target, f.shots_on_target_pct,
            f.goals_per_shot, f.yellow_cards, f.second_yellow_cards,
            f.red_cards, f.fouls_committed, f.fouls_drawn,
            f.interceptions, f.tackles_won,
            CASE WHEN f.venue='Home' THEN u.home_np_xg      ELSE u.away_np_xg      END AS np_xg,
            CASE WHEN f.venue='Home' THEN u.away_np_xg      ELSE u.home_np_xg      END AS np_xg_conceded,
            CASE WHEN f.venue='Home' THEN u.home_ppda       ELSE u.away_ppda       END AS ppda,
            CASE WHEN f.venue='Home' THEN u.away_ppda       ELSE u.home_ppda       END AS ppda_allowed,
            CASE WHEN f.venue='Home' THEN u.home_np_xg_diff ELSE u.away_np_xg_diff END AS np_xg_diff_match
        FROM fbref_merged f
        LEFT JOIN understat_base u
            ON  f.season=u.season AND f.league_source=u.league_source
            AND f.team     = (CASE WHEN f.venue='Home' THEN u.home_team ELSE u.away_team END)
            AND f.opponent = (CASE WHEN f.venue='Home' THEN u.away_team ELSE u.home_team END)
    ),
    whoscored_base AS (
        SELECT team, season, league_source,
            ws_home_att_rating, ws_away_att_rating,
            ws_home_def_rating, ws_away_def_rating,
            ws_home_dribbles_pg, ws_away_dribbles_pg,
            ws_home_fouled_pg, ws_away_fouled_pg,
            ws_home_shots_ot_pg, ws_away_shots_ot_pg
        FROM silver.whoscored_team_season
    ),
    whoscored_features AS (
        SELECT
            b.date, b.team, b.season, b.league_source,
            CASE WHEN b.venue='Home' THEN ws.ws_home_att_rating  ELSE ws.ws_away_att_rating  END AS season_att_rating,
            CASE WHEN b.venue='Home' THEN ws.ws_home_def_rating  ELSE ws.ws_away_def_rating  END AS season_def_rating,
            CASE WHEN b.venue='Home' THEN ws.ws_home_dribbles_pg ELSE ws.ws_away_dribbles_pg END AS ws_dribbles_pg,
            CASE WHEN b.venue='Home' THEN ws.ws_home_fouled_pg   ELSE ws.ws_away_fouled_pg   END AS ws_fouled_pg,
            CASE WHEN b.venue='Home' THEN ws.ws_home_shots_ot_pg ELSE ws.ws_away_shots_ot_pg END AS ws_shots_ot_pg
        FROM fbref_understat b
        LEFT JOIN whoscored_base ws
            ON b.team=ws.team AND b.season=ws.season AND b.league_source=ws.league_source
    ),
    odds_base AS (
        SELECT
            date::DATE AS date, season, league_source, home_team, away_team,
            odds_pinnacle_h, odds_pinnacle_d, odds_pinnacle_a,
            odds_avg_h, odds_avg_d, odds_avg_a,
            pinnacle_prob_h, pinnacle_prob_d, pinnacle_prob_a,
            market_prob_h, market_prob_d, market_prob_a
        FROM silver.odds
        WHERE pinnacle_prob_h IS NOT NULL
    )
    SELECT
        f.date, f.team, f.opponent, f.raw_team, f.raw_opponent,
        f.venue, f.season, f.league_source, f.comp_category, f.result_1n2, f.match_id,
        f.gf, f.ga, f.possession,
        f.shots_on_target_faced, f.saves, f.save_pct, f.clean_sheet,
        f.shots_total, f.shots_on_target, f.goals_per_shot,
        f.yellow_cards, f.second_yellow_cards, f.red_cards,
        f.fouls_committed, f.interceptions, f.tackles_won,
        f.np_xg, f.np_xg_conceded, f.ppda, f.ppda_allowed, f.np_xg_diff_match,
        wf.season_att_rating, wf.season_def_rating,
        wf.ws_dribbles_pg, wf.ws_fouled_pg, wf.ws_shots_ot_pg,
        CASE WHEN f.venue='Home' THEN o.odds_pinnacle_h ELSE o.odds_pinnacle_a END AS odds_pinnacle_team,
        o.odds_pinnacle_d AS odds_pinnacle_draw,
        CASE WHEN f.venue='Home' THEN o.odds_pinnacle_a ELSE o.odds_pinnacle_h END AS odds_pinnacle_opp,
        CASE WHEN f.venue='Home' THEN o.odds_avg_h      ELSE o.odds_avg_a      END AS odds_avg_team,
        o.odds_avg_d AS odds_avg_draw,
        CASE WHEN f.venue='Home' THEN o.odds_avg_a      ELSE o.odds_avg_h      END AS odds_avg_opp,
        CASE WHEN f.venue='Home' THEN o.pinnacle_prob_h ELSE o.pinnacle_prob_a END AS pinnacle_prob_team,
        o.pinnacle_prob_d AS pinnacle_prob_draw,
        CASE WHEN f.venue='Home' THEN o.pinnacle_prob_a ELSE o.pinnacle_prob_h END AS pinnacle_prob_opp,
        CASE WHEN f.venue='Home' THEN o.market_prob_h   ELSE o.market_prob_a   END AS market_prob_team,
        o.market_prob_d AS market_prob_draw,
        CASE WHEN f.venue='Home' THEN o.market_prob_a   ELSE o.market_prob_h   END AS market_prob_opp
    FROM fbref_understat f
    LEFT JOIN whoscored_features wf
        ON f.date=wf.date AND f.team=wf.team AND f.season=wf.season AND f.league_source=wf.league_source
    LEFT JOIN odds_base o
        ON  f.date::DATE=o.date AND f.season=o.season AND f.league_source=o.league_source
        AND (CASE WHEN f.venue='Home' THEN f.team     ELSE f.opponent END = o.home_team)
        AND (CASE WHEN f.venue='Home' THEN f.opponent ELSE f.team     END = o.away_team)
    """)

    # Suppression données COVID Ligue 1
    conn.execute(f"""
        DELETE FROM gold.stg_backbone
        WHERE season='{FRA_covid_Season}' AND league_source='Ligue 1' AND date>='{FRA_end_covid_date}'
    """)

    n_backbone = conn.execute("SELECT COUNT(*) FROM gold.stg_backbone").fetchone()[0]
    logger.info(f"  stg_backbone : {n_backbone:,} lignes")

    # ── BLOC 2 : Rolling ──────────────────────────────────────────────────────
    logger.info("Bloc 2 : Rolling features (multi-window)...")

    all_rolling_cols = "\n".join(_rolling_cols(w) for w in WINDOWS)

    conn.execute(f"""
        CREATE OR REPLACE TABLE gold.features_training AS
        WITH base_with_gap AS (
            SELECT *,
                CAST(date AS DATE)
                    - LAG(CAST(date AS DATE))
                    OVER (PARTITION BY team ORDER BY date) AS days_since_last_match
            FROM gold.stg_backbone
        ),
        season_ratings_lagged AS (
            SELECT team, league_source, season,
                MAX(season_att_rating) AS season_att_rating_raw,
                MAX(season_def_rating) AS season_def_rating_raw
            FROM gold.stg_backbone
            GROUP BY team, league_source, season
        ),
        season_ratings_prev AS (
            SELECT team, league_source, season,
                LAG(season_att_rating_raw) OVER (
                    PARTITION BY team, league_source ORDER BY season
                ) AS season_att_rating,
                LAG(season_def_rating_raw) OVER (
                    PARTITION BY team, league_source ORDER BY season
                ) AS season_def_rating
            FROM season_ratings_lagged
        ),
        features_raw AS (
            SELECT
                date, team, opponent, venue, season, league_source,
                comp_category, match_id, result_1n2,
                CASE WHEN venue='Home' THEN 1 ELSE 0 END AS is_home,
                days_since_last_match,
                CASE WHEN days_since_last_match > 20 THEN 1 ELSE 0 END AS is_return_from_break,
                CASE WHEN days_since_last_match < 4  THEN 1 ELSE 0 END AS is_short_rest,
                {all_rolling_cols}
                ws_dribbles_pg, ws_fouled_pg, ws_shots_ot_pg,
                odds_pinnacle_team, odds_pinnacle_draw, odds_pinnacle_opp,
                odds_avg_team, odds_avg_draw, odds_avg_opp,
                pinnacle_prob_team, pinnacle_prob_draw, pinnacle_prob_opp,
                market_prob_team, market_prob_draw, market_prob_opp
            FROM base_with_gap
        )
        SELECT f.*, sr.season_att_rating, sr.season_def_rating
        FROM features_raw f
        LEFT JOIN season_ratings_prev sr
            ON f.team=sr.team AND f.league_source=sr.league_source AND f.season=sr.season
    """)

    n_training = conn.execute("SELECT COUNT(*) FROM gold.features_training").fetchone()[0]
    logger.info(f"  features_training : {n_training:,} lignes")

    # ── BLOC 3 : Match-up + différentiels ─────────────────────────────────────
    logger.info("Bloc 3 : Match-up final + différentiels (multi-window)...")

    all_opp_cols  = "\n".join(_opp_cols(w)  for w in WINDOWS)
    all_team_cols = "\n".join(_team_cols(w) for w in WINDOWS)
    all_diff_cols = "\n".join(_diff_cols(w) for w in WINDOWS)

    conn.execute(f"""
        CREATE OR REPLACE TABLE gold.features_final AS
        WITH opponent_stats AS (
            SELECT
                date, team AS opp_team,
                season_att_rating AS opp_season_att_rating,
                season_def_rating AS opp_season_def_rating,
                ws_dribbles_pg    AS opp_ws_dribbles_pg,
                ws_fouled_pg      AS opp_ws_fouled_pg,
                ws_shots_ot_pg    AS opp_ws_shots_ot_pg,
                odds_pinnacle_team  AS opp_odds_pinnacle,
                pinnacle_prob_team  AS opp_pinnacle_prob,
                market_prob_team    AS opp_market_prob,
                {all_opp_cols}
                1 AS _dummy
            FROM gold.features_training
        )
        SELECT
            t.date, t.team, t.opponent, t.venue, t.is_home,
            t.season, t.league_source, t.comp_category, t.match_id, t.result_1n2,
            t.days_since_last_match, t.is_return_from_break, t.is_short_rest,
            t.season_att_rating, t.season_def_rating,
            t.ws_dribbles_pg, t.ws_fouled_pg, t.ws_shots_ot_pg,
            o.opp_season_att_rating, o.opp_season_def_rating,
            o.opp_ws_dribbles_pg, o.opp_ws_shots_ot_pg,
            o.opp_odds_pinnacle, o.opp_pinnacle_prob, o.opp_market_prob,
            {all_team_cols}
            {all_diff_cols}
            -- Rétrocompatibilité W=5
            (t.xg_net_roll_{WINDOW}            - o.opp_xg_net_{WINDOW})            AS xg_net_diff,
            (t.season_att_rating               - o.opp_season_def_rating)          AS tactical_advantage,
            (t.ws_dribbles_pg                  - o.opp_ws_dribbles_pg)             AS ws_dribble_style_diff,
            (t.ws_fouled_pg                    - o.opp_ws_fouled_pg)               AS ws_fouled_diff,
            (t.shot_quality_ratio_{WINDOW}     - o.opp_sqr_{WINDOW})               AS sqr_diff,
            (t.ppda_roll_{WINDOW}              - o.opp_ppda_{WINDOW})              AS ppda_diff,
            (t.ppda_ratio_roll_{WINDOW}        - o.opp_ppda_ratio_{WINDOW})        AS ppda_ratio_diff,
            (t.xg_overperformance_{WINDOW}     - o.opp_xg_opi_{WINDOW})            AS xg_opi_diff,
            (t.save_rate_roll_{WINDOW}         - o.opp_save_rate_{WINDOW})         AS save_rate_diff,
            (t.defensive_actions_roll_{WINDOW} - o.opp_defensive_actions_{WINDOW}) AS defensive_actions_diff,
            (t.roll_save_pct_{WINDOW}          - o.opp_roll_save_pct_{WINDOW})     AS keeper_form_diff,
            (t.red_card_rate_roll_{WINDOW}     - o.opp_red_card_rate_{WINDOW})     AS red_card_rate_diff,
            (o.opp_sterility_index_{WINDOW}    - t.sterility_index_{WINDOW})       AS sterility_diff,
            (t.press_resistance_{WINDOW}       - o.opp_press_resistance_{WINDOW})  AS press_resistance_diff,
            (t.shield_efficiency_{WINDOW}      - o.opp_shield_efficiency_{WINDOW}) AS shield_efficiency_diff,
            -- Cotes
            t.odds_pinnacle_team, t.odds_pinnacle_draw, t.odds_pinnacle_opp,
            t.odds_avg_team, t.odds_avg_draw, t.odds_avg_opp,
            t.pinnacle_prob_team, t.pinnacle_prob_draw, t.pinnacle_prob_opp,
            t.market_prob_team, t.market_prob_draw, t.market_prob_opp,
            (t.pinnacle_prob_team - t.pinnacle_prob_opp) AS pinnacle_edge,
            (t.market_prob_team   - t.market_prob_opp)   AS market_edge
        FROM gold.features_training t
        LEFT JOIN opponent_stats o ON t.date=o.date AND t.opponent=o.opp_team
    """)

    # ── BLOC 3.5 : H2H ────────────────────────────────────────────────────────
    logger.info("Bloc 3.5 : Head-to-Head features...")

    H2H_WINDOW = CFG.get("features", {}).get("h2h_window", 10)

    conn.execute("""
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS h2h_win_rate       DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS h2h_draw_rate      DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS h2h_goals_scored   DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS h2h_goals_conceded DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS h2h_xg_diff        DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS h2h_n_matches      INTEGER;
    """)

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_h2h AS
        SELECT
            t.date AS ft_date, t.team AS ft_team,
            t.opponent AS ft_opponent, t.league_source AS ft_league,
            AVG(CASE WHEN h.result_1n2='W' THEN 1.0 ELSE 0.0 END) AS win_rate,
            AVG(CASE WHEN h.result_1n2='D' THEN 1.0 ELSE 0.0 END) AS draw_rate,
            AVG(h.gf)                                               AS goals_scored,
            AVG(h.ga)                                               AS goals_conceded,
            AVG(COALESCE(h.np_xg - h.np_xg_conceded,
                CAST(h.gf AS DOUBLE) - CAST(h.ga AS DOUBLE)))       AS xg_diff,
            COUNT(*)                                                 AS n_matches
        FROM (SELECT DISTINCT date, team, opponent, league_source FROM gold.stg_backbone) t
        JOIN (
            SELECT h1.*,
                ROW_NUMBER() OVER (
                    PARTITION BY h1.team, h1.opponent, h1.league_source
                    ORDER BY h1.date DESC
                ) AS rn
            FROM gold.stg_backbone h1
        ) h
            ON  h.team          = t.team
            AND h.opponent      = t.opponent
            AND h.league_source = t.league_source
            AND h.date          < t.date
            AND h.rn            <= {H2H_WINDOW}
        GROUP BY t.date, t.team, t.opponent, t.league_source
    """)

    conn.execute("""
        UPDATE gold.features_final AS ff
        SET
            h2h_win_rate       = h.win_rate,
            h2h_draw_rate      = h.draw_rate,
            h2h_goals_scored   = h.goals_scored,
            h2h_goals_conceded = h.goals_conceded,
            h2h_xg_diff        = h.xg_diff,
            h2h_n_matches      = h.n_matches
        FROM tmp_h2h h
        WHERE CAST(ff.date AS DATE)=h.ft_date
          AND ff.team=h.ft_team AND ff.opponent=h.ft_opponent
          AND ff.league_source=h.ft_league
    """)

    n_h2h = conn.execute(
        "SELECT COUNT(*) FROM gold.features_final WHERE h2h_n_matches IS NOT NULL"
    ).fetchone()[0]
    logger.info(f"  H2H renseigné : {n_h2h:,} lignes")

    # ── League Draw Rate ───────────────────────────────────────────────────────
    conn.execute("ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS league_draw_rate DOUBLE;")
    conn.execute("""
        UPDATE gold.features_final AS ff
        SET league_draw_rate = ldr.draw_rate
        FROM (
            WITH season_rates AS (
                SELECT league_source, season,
                    AVG(CASE WHEN result_1n2='D' THEN 1.0 ELSE 0.0 END) AS season_draw_rate
                FROM gold.features_final GROUP BY league_source, season
            )
            SELECT s1.league_source, s1.season,
                AVG(s2.season_draw_rate) AS draw_rate
            FROM season_rates s1
            JOIN season_rates s2 ON s2.league_source=s1.league_source AND s2.season < s1.season
            GROUP BY s1.league_source, s1.season
        ) ldr
        WHERE ff.league_source=ldr.league_source AND ff.season=ldr.season
    """)

    # ── BLOC 3.7 : Giant Killer ───────────────────────────────────────────────
    logger.info("Bloc 3.7 : Giant Killer features...")
    conn.execute("""
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS rating_ratio_att DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS rating_ratio_def DOUBLE;
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS upset_risk_index DOUBLE;
    """)
    conn.execute("""
        UPDATE gold.features_final SET
            rating_ratio_att = CASE
                WHEN opp_season_att_rating IS NOT NULL AND opp_season_att_rating<>0
                THEN season_att_rating / opp_season_att_rating ELSE NULL END,
            rating_ratio_def = CASE
                WHEN opp_season_def_rating IS NOT NULL AND opp_season_def_rating<>0
                THEN season_def_rating / opp_season_def_rating ELSE NULL END,
            upset_risk_index = CASE
                WHEN opp_season_att_rating IS NOT NULL AND opp_season_att_rating<>0
                 AND opp_season_def_rating IS NOT NULL AND opp_season_def_rating<>0
                THEN GREATEST(
                    season_att_rating / opp_season_att_rating,
                    season_def_rating / opp_season_def_rating
                ) * is_home ELSE NULL END
    """)

    # ── final_match_id ────────────────────────────────────────────────────────
    conn.execute("""
        ALTER TABLE gold.features_final ADD COLUMN IF NOT EXISTS final_match_id VARCHAR;
        UPDATE gold.features_final SET
            final_match_id = 'fbref_' || LEFT(md5(
                CAST(date AS VARCHAR) || '|' ||
                LEAST(team, opponent) || '|' ||
                GREATEST(team, opponent) || '|' || league_source
            ), 10);
    """)

    # ── Rapport final ─────────────────────────────────────────────────────────
    count = conn.execute("SELECT COUNT(*) FROM gold.features_final").fetchone()[0]
    checks = []
    for w in WINDOWS:
        checks += [f"sqr_diff_{w}", f"ppda_diff_{w}", f"xg_net_diff_{w}",
                   f"win_rate_diff_{w}", f"points_pg_diff_{w}"]
    checks += ["sqr_diff", "ppda_diff", "xg_net_diff", "save_rate_diff",
               "defensive_actions_diff", "h2h_win_rate", "h2h_xg_diff",
               "rating_ratio_att", "upset_risk_index"]
    for col in checks:
        n_ok = conn.execute(
            f"SELECT COUNT(*) FROM gold.features_final WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        pct = n_ok / count * 100 if count else 0
        logger.info(f"    {col:<40} : {n_ok:,}/{count:,} ({pct:.1f}%)")

    logger.success(f"═══ Pipeline rolling terminé — {count:,} lignes gold.features_final ═══")
    conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feature Engineering rolling Silver → Gold")
    parser.add_argument("--reset", action="store_true",
                        help="Supprime et recrée le schéma gold")
    args = parser.parse_args()
    run_rolling_features(reset=args.reset)
