"""
Pipeline 03 — Feature Engineering (Silver DuckDB → Gold DuckDB)
================================================================
Transforme les tables silver.* (Silver) en features prêtes pour le modèle.

ARCHITECTURE 3 BLOCS
─────────────────────
  Bloc 1 — Staging       : jointures multi-sources, casting, corrections Home/Away
  Bloc 2 — Rolling       : fenêtres glissantes (forme récente)
  Bloc 3 — Match-up      : fusion team vs opponent, features différentielles

3 NOUVELLES FEATURES POUR LA DÉTECTION D'UPSETS
─────────────────────────────────────────────────
  F1 — shot_quality_ratio  : xG par tir (rolling 5) → mesure la QUALITÉ des occasions
       Formule  : AVG(np_xg) / NULLIF(AVG(standard_sh), 0) sur 5 matchs
       Différentiel : sqr_diff = team - opp
       Rationale : une équipe qui génère peu de tirs mais de haute qualité
                   peut battre une équipe plus "active" mais peu efficace.
                   Signal fort pour les outsiders bien organisés.

  F2 — pressing_intensity  : PPDA rolling (bas = pressing intense)
       Formule  : AVG(ppda) sur 5 matchs (corrigé Home/Away)
       Différentiel : pressing_diff = team_ppda - opp_ppda
                      < 0 → team presse plus fort = avantage tactique outsider
       Rationale : le pressing différentiel est la signature des upsets de type
                   Bielsa/Klopp : une équipe B qui presse une équipe A qui ne
                   sait pas jouer sous pression. ppda_diff négatif = danger.

  F3 — xg_overperformance  : ratio buts_réels / xG_saison (grain WhoScored)
       Formule  : (ws_{venue}_goals_for / NULLIF(ws_{venue}_xg_for, 0)) - 1
       Différentiel : xg_opi_diff = team_opi - opp_opi
                      > 0 → team surperforme + (régression attendue = risque upset)
                      < 0 → team sous-performe (explosion imminente possible)
       Rationale : une équipe qui marque 30% de plus que son xG est "chanceuse".
                   Face à une équipe qui sous-performe, le différentiel prédit
                   un rééquilibrage — exactly le scénario d'upset.

BUGS CORRIGÉS VS VERSION PRÉCÉDENTE
──────────────────────────────────────
  BUG 1 — match_id alias : t.match_id → t.match_id (ColNotFound)
  BUG 2 — WhoScored Home/Away : ws_home_* utilisé quel que soit venue
           → CASE WHEN venue='Home' THEN ws_home_* ELSE ws_away_* END
  BUG 3 — Understat Home/Away : home_np_xg/home_ppda utilisé pour tous
           → CASE WHEN venue='Home' THEN home_col ELSE away_col END

Usage :
    python pipelines/03_features.py
    python pipelines/03_features.py --reset
"""

import argparse
import yaml
from loguru import logger
from pathlib import Path
import duckdb

# ── Config ────────────────────────────────────────────────────────────────────
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
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# Fenêtres de rolling (matchs précédents, excluant le match courant)
WINDOW = 5

FRA_end_covid_date = "2020-03-08" # Saison arretee le 8 mars 2020, pas de matchs après cette date dans la base
FRA_covid_Season = "2019-2020" # Saison 2019-2020 impactée par la covid


def run_features_pipeline(reset: bool = False) -> None:
    conn = duckdb.connect(DB_PATH)

    if reset:
        logger.info("Reset Gold Layer...")
        conn.execute("DROP SCHEMA IF EXISTS gold CASCADE")

    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # ──────────────────────────────────────────────────────────────────────────
    # BLOC 1 — STAGING
    # Jointures multi-sources + corrections Home/Away + casting robuste
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("Bloc 1 : Staging — jointures et corrections Home/Away...")
 
    conn.execute(f"""
    CREATE OR REPLACE TABLE gold.stg_backbone AS
    WITH
 
    -- ══════════════════════════════════════════════════════════════════════
    -- 1.1 Base FBref schedule — table pivot, définit la cardinalité finale
    -- Clé : (date, team, opponent, league_source)
    -- raw_team / raw_opponent existent grâce à normalize_team_col() dans 02
    -- ══════════════════════════════════════════════════════════════════════
    fbref_base AS (
        SELECT
            date,
            team,
            opponent,
            raw_team,
            raw_opponent,
            venue,
            season,
            league_source,
            result_1n2,
            comp_category,
            CAST(gf AS INTEGER)                                        AS gf,
            CAST(ga AS INTEGER)                                        AS ga,
            CAST(NULLIF(TRIM(CAST(poss AS VARCHAR)), '') AS DOUBLE)    AS possession
        FROM silver.fbref_schedule
    ),
 
    -- ══════════════════════════════════════════════════════════════════════
    -- 1.2 Keeper
    -- FIX BUG 4 : league_source ajouté pour clé complète dans fbref_merged
    -- ══════════════════════════════════════════════════════════════════════
    fbref_keeper_cte AS (
        SELECT
            date, team, opponent, league_source,
            sota       AS shots_on_target_faced,
            saves,
            save_pct,
            cs         AS clean_sheet,
            pk_att     AS pk_faced,
            pk_allowed AS pk_conceded
        FROM silver.fbref_keeper
    ),
 
    -- ══════════════════════════════════════════════════════════════════════
    -- 1.3 Shooting
    -- FIX BUG 4 : league_source ajouté
    -- ══════════════════════════════════════════════════════════════════════
    fbref_shooting_cte AS (
        SELECT
            date, team, opponent, league_source,
            standard_sh      AS shots_total,
            standard_sot     AS shots_on_target,
            standard_sot_pct AS shots_on_target_pct,
            standard_g_sh    AS goals_per_shot,
            standard_pk      AS pk_goals,
            standard_pkatt   AS pk_attempts
        FROM silver.fbref_shooting
    ),
 
    -- ══════════════════════════════════════════════════════════════════════
    -- 1.4 Misc
    -- FIX BUG 4 : league_source ajouté
    -- ══════════════════════════════════════════════════════════════════════
    fbref_misc_cte AS (
        SELECT
            date, team, opponent, league_source,
            crdy  AS yellow_cards,
            crdr  AS red_cards,
            crdy2 AS second_yellow_cards,
            fls   AS fouls_committed,
            fld   AS fouls_drawn,
            off   AS offsides,
            crosses,
            int   AS interceptions,
            tklw  AS tackles_won,
            pkwon AS pk_won,
            pkcon AS pk_conceded_misc,
            og    AS own_goals
        FROM silver.fbref_misc
    ),
 
    -- ══════════════════════════════════════════════════════════════════════
    -- 2. Base Understat
    -- understat_schedule ne contient PAS de date.
    -- La clé naturelle est (season, league_source, home_team, away_team).
    -- Cette combinaison est unique dans le Big 5 : une équipe joue à domicile
    -- contre un adversaire donné exactement une fois par saison.
    -- La jointure sans date est donc sûre pour ce contexte.
    -- ══════════════════════════════════════════════════════════════════════
    understat_base AS (
        SELECT
            u.season,
            u.home_team,
            u.away_team,
            u.match_id,
            u.league_source,
            CAST(NULLIF(TRIM(CAST(us.home_np_xg      AS VARCHAR)), '') AS DOUBLE) AS home_np_xg,
            CAST(NULLIF(TRIM(CAST(us.away_np_xg      AS VARCHAR)), '') AS DOUBLE) AS away_np_xg,
            CAST(NULLIF(TRIM(CAST(us.home_ppda       AS VARCHAR)), '') AS DOUBLE) AS home_ppda,
            CAST(NULLIF(TRIM(CAST(us.away_ppda       AS VARCHAR)), '') AS DOUBLE) AS away_ppda,
            CAST(NULLIF(TRIM(CAST(us.home_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS home_np_xg_diff,
            CAST(NULLIF(TRIM(CAST(us.away_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS away_np_xg_diff
        FROM silver.understat_schedule u
        LEFT JOIN silver.understat_stats us ON u.match_id = us.match_id
    ),
 
    
    -- ══════════════════════════════════════════════════════════════════════
    -- 4. Assemblage des 4 tables FBref
    -- FIX BUG 4 : league_source ajouté dans les 3 conditions ON
    --   Clé complète = (date, team, opponent, league_source)
    --   Empêche les doublons si une même équipe joue le même adversaire
    --   le même jour dans deux compétitions différentes (Coupe + Championnat)
    -- ══════════════════════════════════════════════════════════════════════
    fbref_merged AS (
        SELECT
            b.date,
            b.team,
            b.opponent,
            b.raw_team,
            b.raw_opponent,
            b.venue,
            b.season,
            b.league_source,
            b.result_1n2,
            b.comp_category,
            b.gf,
            b.ga,
            b.possession,
 
            -- Keeper
            k.shots_on_target_faced,
            k.saves,
            k.save_pct,
            k.clean_sheet,
            k.pk_faced,
            k.pk_conceded,
 
            -- Shooting
            s.shots_total,
            s.shots_on_target,
            s.shots_on_target_pct,
            s.goals_per_shot,
            s.pk_goals,
            s.pk_attempts,
 
            -- Misc
            m.yellow_cards,
            m.red_cards,
            m.second_yellow_cards,
            m.fouls_committed,
            m.fouls_drawn,
            m.offsides,
            m.crosses,
            m.interceptions,
            m.tackles_won,
            m.pk_won,
            m.pk_conceded_misc,
            m.own_goals
 
        FROM fbref_base b
        LEFT JOIN fbref_keeper_cte k
            ON  b.date          = k.date
            AND b.team          = k.team
            AND b.opponent      = k.opponent
            AND b.league_source = k.league_source   -- FIX BUG 4
        LEFT JOIN fbref_shooting_cte s
            ON  b.date          = s.date
            AND b.team          = s.team
            AND b.opponent      = s.opponent
            AND b.league_source = s.league_source   -- FIX BUG 4
        LEFT JOIN fbref_misc_cte m
            ON  b.date          = m.date
            AND b.team          = m.team
            AND b.opponent      = m.opponent
            AND b.league_source = m.league_source   -- FIX BUG 4
    ),
 
    -- ══════════════════════════════════════════════════════════════════════
    -- 5. Jointure FBref × Understat
    -- La jointure utilise (season, league_source, home_team, away_team).
    -- Pas de date disponible dans Understat, mais cette clé est unique
    -- dans le Big 5 : une paire d'équipes ne joue qu'une fois à domicile
    -- par saison. Le match_id Understat est récupéré pour traçabilité.
    -- ══════════════════════════════════════════════════════════════════════
    fbref_understat AS (
        SELECT
            f.league_source,
            f.season,
            f.venue,
            u.match_id,
            f.team,
            f.opponent,
            f.raw_team,
            f.raw_opponent,
            f.date,
            f.result_1n2,
            f.comp_category,
            f.gf,
            f.ga,
            f.possession,
 
            -- Keeper
            f.shots_on_target_faced,
            f.saves,
            f.save_pct,
            f.clean_sheet,
 
            -- Shooting
            f.shots_total,
            f.shots_on_target,
            f.shots_on_target_pct,
            f.goals_per_shot,
 
            -- Misc
            f.yellow_cards,
            f.second_yellow_cards,
            f.red_cards,
            f.fouls_committed,
            f.fouls_drawn,
            f.interceptions,
            f.tackles_won,
 
            -- Understat — venue-aware (np_xg exclut les pénaltys)
            CASE WHEN f.venue = 'Home' THEN u.home_np_xg      ELSE u.away_np_xg      END AS np_xg,
            CASE WHEN f.venue = 'Home' THEN u.away_np_xg      ELSE u.home_np_xg      END AS np_xg_conceded,
            CASE WHEN f.venue = 'Home' THEN u.home_ppda       ELSE u.away_ppda       END AS ppda,
            CASE WHEN f.venue = 'Home' THEN u.away_ppda       ELSE u.home_ppda       END AS ppda_allowed,
            CASE WHEN f.venue = 'Home' THEN u.home_np_xg_diff ELSE u.away_np_xg_diff END AS np_xg_diff_match
 
        FROM fbref_merged f
        LEFT JOIN understat_base u
            ON  f.season        = u.season
            AND f.league_source = u.league_source
            AND f.team     = (CASE WHEN f.venue = 'Home' THEN u.home_team ELSE u.away_team END)
            AND f.opponent = (CASE WHEN f.venue = 'Home' THEN u.away_team ELSE u.home_team END)
    ),
                 
    -- ══════════════════════════════════════════════════════════════════════
    -- 3. WhoScored — sélection incrémentale
    -- Grain : team × season × league_source
    -- Règle : on ne garde que ce qu'Understat/FBref ne couvre pas déjà.
    -- Mapping venue-aware dans whoscored_features (CTE suivant).
    -- ══════════════════════════════════════════════════════════════════════
    whoscored_base AS (
        SELECT
            team,
            season,
            league_source,

            -- Niveau structurel (Fiabiliser)
            ws_home_att_rating,
            ws_away_att_rating,
            ws_home_def_rating,
            ws_away_def_rating,

            -- Style offensif (Optimiser)
            -- Dribbles : absent de FBref dans notre stack
            ws_home_dribbles_pg,
            ws_away_dribbles_pg,

            -- Capacité à obtenir des fautes : proxy d'intensité de contact
            -- et d'agressivité adverse — distinct des fouls_committed FBref
            ws_home_fouled_pg,
            ws_away_fouled_pg,

            -- Cadrage saison : baseline vs shots_on_target FBref par match
            ws_home_shots_ot_pg,
            ws_away_shots_ot_pg

        FROM silver.whoscored_team_season
    ),

    -- ══════════════════════════════════════════════════════════════════════
    -- Mapping venue-aware
    -- Pour chaque ligne du backbone (une équipe, un match),
    -- on sélectionne les métriques home ou away selon la venue.
    -- Ce CTE est jointé dans le SELECT final sur (team, season, league_source).
    -- ══════════════════════════════════════════════════════════════════════
    whoscored_features AS (
        SELECT
            b.date,
            b.team,
            b.season,
            b.league_source,

            -- Ratings structurels
            CASE WHEN b.venue = 'Home' THEN ws.ws_home_att_rating
                ELSE ws.ws_away_att_rating END                  AS season_att_rating,
            CASE WHEN b.venue = 'Home' THEN ws.ws_home_def_rating
                ELSE ws.ws_away_def_rating END                  AS season_def_rating,

            -- Style
            CASE WHEN b.venue = 'Home' THEN ws.ws_home_dribbles_pg
                ELSE ws.ws_away_dribbles_pg END                 AS ws_dribbles_pg,
            CASE WHEN b.venue = 'Home' THEN ws.ws_home_fouled_pg
                ELSE ws.ws_away_fouled_pg END                   AS ws_fouled_pg,

            -- Cadrage
            CASE WHEN b.venue = 'Home' THEN ws.ws_home_shots_ot_pg
                ELSE ws.ws_away_shots_ot_pg END                 AS ws_shots_ot_pg

        FROM fbref_understat b  -- ou fbref_understat si dans le même WITH
        LEFT JOIN whoscored_base ws
            ON  b.team          = ws.team
            AND b.season        = ws.season
            AND b.league_source = ws.league_source
    ),
    -- ══════════════════════════════════════════════════════════════════════
    -- 6. Cotes de paris — silver.odds
    -- Grain : (date, home_team, away_team, league_source, season)
    -- On joint sur la perspective Home uniquement puis on remappe
    -- en venue-aware dans le SELECT final.
    -- ══════════════════════════════════════════════════════════════════════
    odds_base AS (
        SELECT
            date::DATE          AS date,
            season,
            league_source,
            home_team,
            away_team,

            -- Cotes brutes Pinnacle
            odds_pinnacle_h,
            odds_pinnacle_d,
            odds_pinnacle_a,

            -- Cotes brutes Average marché
            odds_avg_h,
            odds_avg_d,
            odds_avg_a,

            -- Probabilités implicites Pinnacle (sans marge bookmaker)
            pinnacle_prob_h,
            pinnacle_prob_d,
            pinnacle_prob_a,

            -- Probabilités implicites Average marché
            market_prob_h,
            market_prob_d,
            market_prob_a

        FROM silver.odds
        WHERE pinnacle_prob_h IS NOT NULL   -- exclure matchs sans cotes Pinnacle
    )
 
    -- ══════════════════════════════════════════════════════════════════════
    -- ASSEMBLAGE FINAL
    -- FIX BUG 3 : league_source ajouté dans la jointure WhoScored
    --   whoscored_team_season a pour clé (team, season, league_source).
    --   Sans league_source, une équipe présente dans WhoScored pour deux
    --   ligues différentes la même saison (ex : scraping Ligue 1 + CL)
    --   produirait un doublon ici → 1 ligne FBref × 2 ws = 2 lignes,
    --   ce qui double certains matchs et casse la cardinalité attendue.
    -- ══════════════════════════════════════════════════════════════════════
    SELECT
        -- Identifiants
        f.date,
        f.team,
        f.opponent,
        f.raw_team,
        f.raw_opponent,
        f.venue,
        f.season,
        f.league_source,
        f.comp_category,
        f.result_1n2,
        f.match_id,
 
        -- Performance réelle
        f.gf,
        f.ga,
        f.possession,
 
        -- Métriques FBref
        f.shots_on_target_faced,
        f.saves,
        f.save_pct,
        f.clean_sheet,
        f.shots_total,
        f.shots_on_target,
        f.goals_per_shot,
        f.yellow_cards,
        f.second_yellow_cards,
        f.red_cards,
        f.fouls_committed,
        f.interceptions,
        f.tackles_won,
 
        -- Métriques Understat
        f.np_xg,
        f.np_xg_conceded,
        f.ppda,
        f.ppda_allowed,
        f.np_xg_diff_match,
 
        -- WhoScored — remplace les 3 colonnes actuelles
        wf.season_att_rating,
        wf.season_def_rating,
        wf.ws_dribbles_pg,
        wf.ws_fouled_pg,
        wf.ws_shots_ot_pg,
    
        -- ══════════════════════════════════════════════════════════════════
        -- COTES DE PARIS — venue-aware
        -- La table silver.odds est au grain match (1 ligne par match).
        -- On restitue les cotes du point de vue de chaque équipe :
        --   venue = Home → cotes telles quelles (H = victoire équipe, A = défaite)
        --   venue = Away → H et A inversés (A = victoire équipe, H = défaite)
        -- Les probabilités implicites suivent la même logique.
        -- ══════════════════════════════════════════════════════════════════

        -- Cotes brutes Pinnacle (venue-aware)
        CASE WHEN f.venue = 'Home' THEN o.odds_pinnacle_h ELSE o.odds_pinnacle_a END AS odds_pinnacle_team,
        o.odds_pinnacle_d                                                              AS odds_pinnacle_draw,
        CASE WHEN f.venue = 'Home' THEN o.odds_pinnacle_a ELSE o.odds_pinnacle_h END AS odds_pinnacle_opp,

        -- Cotes brutes Average (venue-aware)
        CASE WHEN f.venue = 'Home' THEN o.odds_avg_h ELSE o.odds_avg_a END AS odds_avg_team,
        o.odds_avg_d                                                         AS odds_avg_draw,
        CASE WHEN f.venue = 'Home' THEN o.odds_avg_a ELSE o.odds_avg_h END AS odds_avg_opp,

        -- Probabilités implicites Pinnacle (venue-aware)
        CASE WHEN f.venue = 'Home' THEN o.pinnacle_prob_h ELSE o.pinnacle_prob_a END AS pinnacle_prob_team,
        o.pinnacle_prob_d                                                              AS pinnacle_prob_draw,
        CASE WHEN f.venue = 'Home' THEN o.pinnacle_prob_a ELSE o.pinnacle_prob_h END AS pinnacle_prob_opp,

        -- Probabilités implicites Average (venue-aware)
        CASE WHEN f.venue = 'Home' THEN o.market_prob_h ELSE o.market_prob_a END AS market_prob_team,
        o.market_prob_d                                                             AS market_prob_draw,
        CASE WHEN f.venue = 'Home' THEN o.market_prob_a ELSE o.market_prob_h END AS market_prob_opp
 
        FROM fbref_understat f
        LEFT JOIN whoscored_features wf
            ON  f.date          = wf.date
            AND f.team          = wf.team
            AND f.season        = wf.season
            AND f.league_source = wf.league_source
        LEFT JOIN odds_base o
        ON  f.date::DATE    = o.date
        AND f.season        = o.season
        AND f.league_source = o.league_source
        AND (
            CASE WHEN f.venue = 'Home' THEN f.team     ELSE f.opponent END = o.home_team
        AND CASE WHEN f.venue = 'Home' THEN f.opponent ELSE f.team     END = o.away_team
        )
    """)

    # ──────────────────────────────────────────────────────────────────────────
    # SUPPRESSION DE L'ANNÉE 2019-2020 DE LA LIGUE 1 (IMPACT COVID)
    # ──────────────────────────────────────────────────────────────────────────

    conn.execute(f"""
        DELETE FROM gold.stg_backbone
        WHERE season = '{FRA_covid_Season}'
        and league_source = 'Ligue 1'
        and date >= '{FRA_end_covid_date}'
    """)

    n_backbone = conn.execute("SELECT COUNT(*) FROM gold.stg_backbone").fetchone()[0]
    logger.info(f"  stg_backbone : {n_backbone:,} lignes")

    # ──────────────────────────────────────────────────────────────────────────
    # BLOC 2 — ROLLING FEATURES
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("Bloc 2 : Rolling features...")

    # FRAME_GEN   : 5 matchs précédents toutes compétitions confondues
    # FRAME_VENUE : 5 matchs précédents À DOMICILE ou À L'EXTÉRIEUR séparément
    #               Capture la dualité tactique home/away (pressing différent,
    #               bloc défensif différent selon le statut du match)
    # FRAME_GEN   = f"PARTITION BY team ORDER BY date ROWS BETWEEN {WINDOW} PRECEDING AND 1 PRECEDING"
    # FRAME_VENUE = f"PARTITION BY team, venue ORDER BY date ROWS BETWEEN {WINDOW} PRECEDING AND 1 PRECEDING"

    FRAME_GEN   = f"PARTITION BY team, season, league_source ORDER BY date ROWS BETWEEN {WINDOW} PRECEDING AND 1 PRECEDING"
    FRAME_VENUE = f"PARTITION BY team, season, league_source, venue ORDER BY date ROWS BETWEEN {WINDOW} PRECEDING AND 1 PRECEDING"

    conn.execute(f"""
        CREATE OR REPLACE TABLE gold.features_training AS
        WITH base_with_gap AS (
            SELECT *,
                CAST(date AS DATE)
                    - LAG(CAST(date AS DATE))
                      OVER (PARTITION BY team ORDER BY date) AS days_since_last_match
            FROM gold.stg_backbone
        )
        SELECT
            -- ══════════════════════════════════════════════════════════════════
            -- IDENTIFIANTS
            -- ══════════════════════════════════════════════════════════════════
            date,
            team,
            opponent,
            venue,
            season,
            league_source,
            comp_category,
            match_id,
            result_1n2,

            -- ══════════════════════════════════════════════════════════════════
            -- CONTEXTE MATCH
            -- ══════════════════════════════════════════════════════════════════
            CASE WHEN venue = 'Home' THEN 1 ELSE 0 END AS is_home,

            -- Fatigue / fraîcheur
            days_since_last_match,
            CASE WHEN days_since_last_match > 20 THEN 1 ELSE 0 END
                AS is_return_from_break,     -- repos prolongé (CL, trêve intl.)
            CASE WHEN days_since_last_match < 4  THEN 1 ELSE 0 END
                AS is_short_rest,            -- < 4 jours : fatigue cumulative

            -- ══════════════════════════════════════════════════════════════════
            -- AXE xG — FORME OFFENSIVE ET DÉFENSIVE
            -- ══════════════════════════════════════════════════════════════════

            -- xG offensif (np = hors pénaltys, plus stable que xg brut)
            AVG(np_xg)           OVER ({FRAME_GEN})   AS np_xg_roll_{WINDOW},
            AVG(np_xg)           OVER ({FRAME_VENUE})  AS np_xg_roll_venue_{WINDOW},

            -- xG défensif (xG concédé sur 5 matchs)
            -- Complément indispensable du xG offensif pour évaluer le niveau
            AVG(np_xg_conceded)  OVER ({FRAME_GEN})   AS np_xg_conceded_roll_{WINDOW},

            -- Balance xG nette : offensive - défensive
            -- > 0 → l'équipe crée plus qu'elle ne concède → dominance attendue
            -- < 0 → l'équipe subit → upset potentiel si elle gagne quand même
            AVG(np_xg - np_xg_conceded) OVER ({FRAME_GEN}) AS xg_net_roll_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════
            -- AXE QUALITÉ DE TIR (Shot Quality Ratio)
            -- ══════════════════════════════════════════════════════════════════

            -- SQR : xG par tir — mesure la QUALITÉ des occasions vs le volume
            -- Signal clé d'upset : un outsider qui tire peu mais bien (contre-attaque)
            -- peut battre une équipe qui domine territorialement mais tire mal.
            AVG(np_xg) OVER ({FRAME_GEN})
                / NULLIF(AVG(shots_total) OVER ({FRAME_GEN}), 0)
                AS shot_quality_ratio_{WINDOW},

            -- Version par venue (counter-attacking style peut varier home/away)
            AVG(np_xg) OVER ({FRAME_VENUE})
                / NULLIF(AVG(shots_total) OVER ({FRAME_VENUE}), 0)
                AS shot_quality_ratio_venue_{WINDOW},

            -- Précision des tirs cadrés : shots_on_target / shots_total
            -- Distinct du SQR : mesure la direction, pas la dangerosité
            AVG(shots_on_target) OVER ({FRAME_GEN})
                / NULLIF(AVG(shots_total) OVER ({FRAME_GEN}), 0)
                AS shot_accuracy_roll_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════
            -- AXE GARDIEN (Save Rate — Regression to Mean)
            -- ══════════════════════════════════════════════════════════════════

            -- Taux d'arrêts observé en rolling (plus stable que save_pct par match)
            -- Si save_rate_roll >> baseline historique → gardien en forme OU chance
            -- Signal de régression : une équipe qui gagne grâce à son gardien
            -- finit par revenir à la moyenne → risque d'upset à venir

            AVG(saves) OVER ({FRAME_GEN})
                / NULLIF(AVG(shots_on_target_faced) OVER ({FRAME_GEN}), 0)
                AS save_rate_roll_{WINDOW},

            AVG(save_pct) OVER ({FRAME_GEN}) AS roll_save_pct_{WINDOW},

            AVG(shots_on_target_faced) OVER ({FRAME_GEN}) AS roll_sota_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════
            -- AXE POSSESSION
            -- ══════════════════════════════════════════════════════════════════
            AVG(possession) OVER ({FRAME_GEN})   AS poss_roll_{WINDOW},
            AVG(possession) OVER ({FRAME_VENUE})  AS poss_roll_venue_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════
            -- AXE PRESSING (PPDA)
            -- ppda bas = pressing intense (peu de passes adverses autorisées
            --           avant une action défensive)
            -- ══════════════════════════════════════════════════════════════════

            -- Pressing exercé par l'équipe
            AVG(ppda) OVER ({FRAME_GEN}) AS ppda_roll_{WINDOW},

            -- Pressing subi par l'équipe (ppda de ses adversaires vs elle)
            -- Mesure si l'équipe joue souvent contre des équipes qui pressent
            AVG(ppda_allowed) OVER ({FRAME_GEN}) AS ppda_allowed_roll_{WINDOW},

            -- Ratio de pressing : ppda / ppda_allowed
            -- < 1 → l'équipe presse plus qu'elle n'est pressée → avantage tactique
            -- Signal upset : une équipe avec ratio < 0.7 contre un adversaire à > 1.3
            --                crée une asymétrie de pressing typique des upsets Bielsa/Klopp
            AVG(ppda) OVER ({FRAME_GEN})
                / NULLIF(AVG(ppda_allowed) OVER ({FRAME_GEN}), 0)
                AS ppda_ratio_roll_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════
            -- AXE DÉFENSE ACTIVE (Intensity Index)
            -- Distinct du pressing (ppda) : mesure les duels REMPORTÉS
            -- plutôt que la pression sur la possession adverse
            -- ══════════════════════════════════════════════════════════════════

            -- Actions défensives gagnées = tackles_won + interceptions
            -- Élevé = bloc défensif actif, équipe qui récupère le ballon
            AVG(tackles_won + interceptions) OVER ({FRAME_GEN})
                AS defensive_actions_roll_{WINDOW},

            -- Brutalité défensive : fautes / duels gagnés
            -- Élevé = équipe qui faute beaucoup pour peu de duels gagnés
            -- Signal de stress défensif ou d'indiscipline sous pression
            AVG(fouls_committed) OVER ({FRAME_GEN})
                / NULLIF(AVG(tackles_won) OVER ({FRAME_GEN}), 0)
                AS fouls_per_tackle_roll_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════
            -- AXE SURPERFORMANCE xG (Regression to Mean — Upset Signal)
            -- ══════════════════════════════════════════════════════════════════
            -- Ratio buts réels / xG sur 5 matchs
            -- > 1 → l'équipe marque plus que son xG → "chanceuse" → régression attendue
            -- < 1 → l'équipe marque moins → "malchanceuse" → rebond possible
            -- Face à une équipe qui sous-performe (< 1), une équipe surperformante
            -- est plus à risque qu'il n'y paraît → classique setup d'upset
            AVG(gf) OVER ({FRAME_GEN})
                / NULLIF(AVG(np_xg) OVER ({FRAME_GEN}), 0) - 1
                AS xg_overperformance_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════════
            -- STERILITY INDEX
            -- Croise la possession avec la qualité de tir (SQR).
            -- Une équipe qui domine la possession mais génère peu de xG par tir
            -- est un "faux favori" — elle contrôle sans menacer réellement.
            -- Valeur haute = possession stérile → signal upset pour l'adversaire.
            -- Formule : possession rolling / SQR rolling
            -- Interprétation : plus c'est haut, plus il faut de possession
            --                  pour générer 1 unité de danger réel.
            -- ══════════════════════════════════════════════════════════════════════
            AVG(possession) OVER ({FRAME_GEN})
                / NULLIF(
                    AVG(np_xg) OVER ({FRAME_GEN})
                        / NULLIF(AVG(shots_total) OVER ({FRAME_GEN}), 0)
                , 0)
                AS sterility_index_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════════
            -- PRESS RESISTANCE INDEX
            -- Mesure la capacité à maintenir la possession SOUS pression adverse.
            -- ppda_allowed bas = l'adversaire presse fort contre cette équipe.
            -- possession élevée malgré ppda_allowed bas = résiste au pressing.
            -- Formule : possession rolling × (1 / ppda_allowed rolling)
            -- Interprétation : élevé = l'équipe garde le ballon même quand pressée.
            -- Signal upset : une équipe avec PRI élevé contre un presseur intense
            --               peut neutraliser le plan de jeu adverse.
            -- ══════════════════════════════════════════════════════════════════════
            AVG(possession) OVER ({FRAME_GEN})
                / NULLIF(AVG(ppda_allowed) OVER ({FRAME_GEN}), 0)
                AS press_resistance_{WINDOW},

            -- ══════════════════════════════════════════════════════════════════════
            -- SHIELD EFFICIENCY INDEX
            -- Combine save_pct rolling avec le volume de xG concédé.
            -- Une équipe avec save_pct élevé ET xG concédé élevé tient grâce à son
            -- gardien — situation non répétable → signal de régression imminente.
            -- Une équipe avec save_pct modéré ET xG concédé faible est
            -- structurellement solide — le gardien n'est pas en surcharge.
            -- Formule : save_pct rolling / (1 + np_xg_conceded rolling)
            -- Interprétation : élevé = efficacité défensive soutenable.
            --                  save_pct haut mais np_xg_conceded haut → ratio bas
            --                  malgré la forme → alerte régression.
            -- ══════════════════════════════════════════════════════════════════════
            AVG(save_pct) OVER ({FRAME_GEN})
                / NULLIF(1 + AVG(np_xg_conceded) OVER ({FRAME_GEN}), 0)
                AS shield_efficiency_{WINDOW},

            
            -- ══════════════════════════════════════════════════════════════════════
            -- CONTEXTE DISCIPLINAIRE
            -- Un carton rouge change structurellement le match à partir du moment
            -- où il est donné. Les stats post-expulsion sont biaisées (possession
            -- adverse gonflée, ppda de l'équipe réduite, shots concédés en hausse).
            -- On expose le flag — le modèle apprend à dévaluer ces matchs.
            -- ══════════════════════════════════════════════════════════════════════
            CASE WHEN (red_cards + second_yellow_cards) > 0 THEN 1 ELSE 0 END
                AS had_red_card,

            -- Proportion de matchs récents avec carton rouge dans la fenêtre
            -- Élevé → pattern disciplinaire structurel, pas un accident
            AVG(
                CASE WHEN (red_cards + second_yellow_cards) > 0 THEN 1.0 ELSE 0.0 END
            ) OVER ({FRAME_GEN})
                AS red_card_rate_roll_{WINDOW},

			-- ══════════════════════════════════════════════════════════════════
            -- GRAIN SAISON (WhoScored) — NIVEAU STRUCTUREL
            -- Constant sur toute la saison, pas de rolling nécessaire.
            -- ws_season_xg_for supprimé — remplacé par ws_xg_diff_for
            -- qui encode la surperformance buts/xG saison (signal régression).
            -- ══════════════════════════════════════════════════════════════════

            -- Niveau structurel
            season_att_rating,          -- niveau offensif agrégé (WhoScored)
            season_def_rating,          -- niveau défensif agrégé (WhoScored)

            -- Style offensif (Optimiser)
            ws_dribbles_pg,             -- intensité de dribble — absent FBref
            ws_fouled_pg,               -- capacité à obtenir des fautes
            ws_shots_ot_pg,              -- cadrage moyen saison
            -- ══════════════════════════════════════════════════════════════════
            -- COTES DE PARIS — propagation depuis stg_backbone
            -- ══════════════════════════════════════════════════════════════════
            odds_pinnacle_team,
            odds_pinnacle_draw,
            odds_pinnacle_opp,
            odds_avg_team,
            odds_avg_draw,
            odds_avg_opp,
            pinnacle_prob_team,
            pinnacle_prob_draw,
            pinnacle_prob_opp,
            market_prob_team,
            market_prob_draw,
            market_prob_opp

        FROM base_with_gap
    """)

    n_training = conn.execute(
        "SELECT COUNT(*) FROM gold.features_training"
    ).fetchone()[0]
    logger.info(f"  features_training : {n_training:,} lignes")

    # ──────────────────────────────────────────────────────────────────────────
    # BLOC 3 — MATCH-UP FINAL + DIFFÉRENTIELS
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("Bloc 3 : Match-up final + différentiels upset...")

    conn.execute(f"""
        CREATE OR REPLACE TABLE gold.features_final AS

        WITH opponent_stats AS (
            -- Récupère les features pré-calculées de l'adversaire (même match,
            -- même date → clé : date + team_adversaire = opponent de la ligne principale)
            SELECT
                date,
                team                              AS opp_team,
                 
                -- xG
                np_xg_roll_{WINDOW}               AS opp_np_xg,
                np_xg_roll_venue_{WINDOW}         AS opp_np_xg_venue,
                np_xg_conceded_roll_{WINDOW}      AS opp_np_xg_conceded,
                xg_net_roll_{WINDOW}              AS opp_xg_net,

                -- Qualité de tir
                shot_quality_ratio_{WINDOW}       AS opp_sqr,
                shot_accuracy_roll_{WINDOW}       AS opp_shot_accuracy,

                -- Pressing
                ppda_roll_{WINDOW}                AS opp_ppda,
                ppda_allowed_roll_{WINDOW}        AS opp_ppda_allowed,
                ppda_ratio_roll_{WINDOW}          AS opp_ppda_ratio,

                -- Défense active
                defensive_actions_roll_{WINDOW}   AS opp_defensive_actions,

                -- Surperformance xG
                xg_overperformance_{WINDOW}       AS opp_xg_opi,

                -- Gardien
                save_rate_roll_{WINDOW}           AS opp_save_rate,
                roll_save_pct_{WINDOW}            AS opp_roll_save_pct,
                
                -- Saison
                season_att_rating       AS opp_season_att_rating,
                season_def_rating       AS opp_season_def_rating,

                ws_dribbles_pg          AS opp_ws_dribbles_pg,
                ws_fouled_pg            AS opp_ws_fouled_pg,
                ws_shots_ot_pg          AS opp_ws_shots_ot_pg,

                -- Discipline
                had_red_card                     AS opp_had_red_card,
                red_card_rate_roll_{WINDOW}      AS opp_red_card_rate,

                sterility_index_{WINDOW}          AS opp_sterility_index,
                press_resistance_{WINDOW}         AS opp_press_resistance,
                shield_efficiency_{WINDOW}        AS opp_shield_efficiency,

                -- Cotes adversaire
                odds_pinnacle_team      AS opp_odds_pinnacle,
                pinnacle_prob_team      AS opp_pinnacle_prob,
                market_prob_team        AS opp_market_prob

            FROM gold.features_training
        )

        SELECT
            -- ── Identifiants ─────────────────────────────────────────────────
            t.date,
            t.team,
            t.opponent,
            t.venue,
            t.is_home,
            t.season,
            t.league_source,
            t.comp_category,
            t.match_id,
            t.result_1n2,

            -- ── Contexte ─────────────────────────────────────────────────────
            t.days_since_last_match,
            t.is_return_from_break,
            t.is_short_rest,



            -- ── Features équipe ───────────────────────────────────────────────
            t.np_xg_roll_{WINDOW},
            t.np_xg_roll_venue_{WINDOW},
            t.np_xg_conceded_roll_{WINDOW},
            t.xg_net_roll_{WINDOW},
            t.shot_quality_ratio_{WINDOW},
            t.shot_quality_ratio_venue_{WINDOW},
            t.shot_accuracy_roll_{WINDOW},
            t.save_rate_roll_{WINDOW},
            t.poss_roll_{WINDOW},
            t.poss_roll_venue_{WINDOW},
            t.ppda_roll_{WINDOW},
            t.ppda_allowed_roll_{WINDOW},
            t.ppda_ratio_roll_{WINDOW},
            t.defensive_actions_roll_{WINDOW},
            t.fouls_per_tackle_roll_{WINDOW},
            t.xg_overperformance_{WINDOW},
            t.red_card_rate_roll_{WINDOW},

            -- Super-features standalone
            t.sterility_index_{WINDOW},
            t.press_resistance_{WINDOW},
            t.shield_efficiency_{WINDOW},
            t.roll_save_pct_{WINDOW},
            t.roll_sota_{WINDOW},

            -- Adversaire
            o.opp_sterility_index,
            o.opp_press_resistance,
            o.opp_shield_efficiency,
            o.opp_roll_save_pct,
            o.opp_odds_pinnacle,
            o.opp_pinnacle_prob,
            o.opp_market_prob,

            
            t.season_att_rating,
            t.season_def_rating,

            -- ── Features adversaire ───────────────────────────────────────────
            o.opp_np_xg,
            o.opp_np_xg_venue,
            o.opp_np_xg_conceded,
            o.opp_xg_net,
            o.opp_sqr,
            o.opp_shot_accuracy,
            o.opp_ppda,
            o.opp_ppda_allowed,
            o.opp_ppda_ratio,
            o.opp_defensive_actions,
            o.opp_xg_opi,
            o.opp_save_rate,
            o.opp_season_att_rating,
            o.opp_season_def_rating,
            o.opp_red_card_rate,

            -- ══════════════════════════════════════════════════════════════════
            -- DIFFÉRENTIELS — SIGNAUX UPSET
            -- Chaque différentiel encode l'avantage relatif de l'équipe
            -- sur un axe donné. Ces variables sont les plus informatives
            -- pour les modèles arborescents (un seul split suffit).
            -- ══════════════════════════════════════════════════════════════════

            -- xG net différentiel
            -- > 0 → l'équipe domine les deux côtés du terrain vs l'adversaire
            (t.xg_net_roll_{WINDOW} - o.opp_xg_net)
                AS xg_net_diff,

            -- Avantage tactique (niveau WhoScored)
            -- Mesure la dominance de niveau saison, pas de forme récente
            (t.season_att_rating - o.opp_season_def_rating)
                AS tactical_advantage,

            -- Style différentiel
            (t.ws_dribbles_pg - o.opp_ws_dribbles_pg)
                AS ws_dribble_style_diff,       -- équipe plus technique/directe que l'adversaire

            (t.ws_fouled_pg - o.opp_ws_fouled_pg)
                AS ws_fouled_diff,               -- capacité à obtenir des fautes vs l'adversaire

            -- Shot Quality Differential
            -- > 0 → l'équipe crée des occasions de meilleure qualité par tir
            -- Signal upset : outsider avec SQR élevé vs favori qui tire beaucoup mais mal
            (t.shot_quality_ratio_{WINDOW} - o.opp_sqr)
                AS sqr_diff,

            -- Pressing Differential
            -- < 0 → l'équipe presse PLUS fort que l'adversaire
            -- Signal upset : si ppda_diff très négatif → supériorité de pressing
            -- probable origine d'un résultat surprenant
            (t.ppda_roll_{WINDOW} - o.opp_ppda)
                AS ppda_diff,

            -- Pressing Ratio Differential
            -- Encode l'asymétrie de pressing en une seule valeur
            -- < 0 → l'équipe a un meilleur ratio pressing que l'adversaire
            (t.ppda_ratio_roll_{WINDOW} - o.opp_ppda_ratio)
                AS ppda_ratio_diff,

            -- xG Overperformance Differential
            -- > 0 → l'équipe surperforme PLUS son xG que l'adversaire
            --       → l'équipe est "chanceuse" → régression attendue = risque upset
            -- < 0 → l'adversaire est le chanceux → l'équipe sous-évaluée
            (t.xg_overperformance_{WINDOW} - o.opp_xg_opi)
                AS xg_opi_diff,

            -- Save Rate Differential
            -- > 0 → le gardien de l'équipe sur-performe vs l'adversaire
            -- Signal de régression : si très positif → performance non répétable
            (t.save_rate_roll_{WINDOW} - o.opp_save_rate)
                AS save_rate_diff,

            -- Defensive Actions Differential
            -- > 0 → l'équipe récupère plus de ballons en duel que l'adversaire
            -- Signal upset : une équipe D qui récupère beaucoup peut frustrer
            -- une équipe A qui domine la possession (xG mais peu de buts)
            (t.defensive_actions_roll_{WINDOW} - o.opp_defensive_actions)
                AS defensive_actions_diff,

            -- Keeper Form Differential
            -- > 0 → le gardien de l'équipe est en meilleure forme récente
            -- Signal : peut "voler" un match même en étant dominé
            -- < 0 → déficit de confiance, le gardien adverse est plus performant
            (t.roll_save_pct_{WINDOW} - o.opp_roll_save_pct)
                AS keeper_form_diff,

            (t.red_card_rate_roll_{WINDOW} - o.opp_red_card_rate) AS red_card_rate_diff,

            -- Sterility Differential
            -- > 0 → l'adversaire a une possession plus stérile que l'équipe
            -- Signal upset : l'équipe crée plus de danger par unité de possession
            (o.opp_sterility_index - t.sterility_index_{WINDOW})
                AS sterility_diff,

            -- Press Resistance Differential
            -- > 0 → l'équipe résiste mieux au pressing que l'adversaire
            -- Signal upset : équipe capable de sortir proprement face à un presseur
            (t.press_resistance_{WINDOW} - o.opp_press_resistance)
                AS press_resistance_diff,

            -- Shield Efficiency Differential
            -- > 0 → la défense de l'équipe est plus soutenable que celle de l'adversaire
            -- < 0 → l'adversaire défend mieux structurellement → risque si l'équipe
            --        comptait sur la régression défensive adverse
            (t.shield_efficiency_{WINDOW} - o.opp_shield_efficiency)
                AS shield_efficiency_diff,

            -- ══════════════════════════════════════════════════════════════════
            -- COTES DE PARIS
            -- Propagées depuis stg_backbone via features_training
            -- ══════════════════════════════════════════════════════════════════
            t.odds_pinnacle_team,
            t.odds_pinnacle_draw,
            t.odds_pinnacle_opp,
            t.odds_avg_team,
            t.odds_avg_draw,
            t.odds_avg_opp,
            t.pinnacle_prob_team,
            t.pinnacle_prob_draw,
            t.pinnacle_prob_opp,
            t.market_prob_team,
            t.market_prob_draw,
            t.market_prob_opp,

            -- Différentiel marché — signal upset clé
            -- > 0 → le marché favorise l'équipe
            -- < 0 → le marché favorise l'adversaire → terrain d'upset
            (t.pinnacle_prob_team - t.pinnacle_prob_opp) AS pinnacle_edge,
            (t.market_prob_team   - t.market_prob_opp)   AS market_edge


        FROM gold.features_training t
        LEFT JOIN opponent_stats o
            ON  t.date     = o.date
            AND t.opponent = o.opp_team
    """)

    # Après conn.execute(""" CREATE OR REPLACE TABLE gold.features_final ... """)

    conn.execute("""
            ALTER TABLE gold.features_final
            ADD COLUMN IF NOT EXISTS final_match_id VARCHAR;

            UPDATE gold.features_final
            SET final_match_id = 'fbref_' || LEFT(
                md5(
                    CAST(date AS VARCHAR) || '|' ||
                    LEAST(team, opponent)  || '|' ||
                    GREATEST(team, opponent) || '|' ||
                    league_source
                ),
                10
            );
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM gold.features_final"
    ).fetchone()[0]

    # Vérification de couverture sur les features clés
    checks = [
        "sqr_diff", "ppda_diff", "ppda_ratio_diff",
        "xg_opi_diff", "xg_net_diff", "save_rate_diff", "defensive_actions_diff"
    ]
    coverage = {}
    for col in checks:
        n_null = conn.execute(
            f"SELECT COUNT(*) FROM gold.features_final WHERE {col} IS NULL"
        ).fetchone()[0]
        coverage[col] = count - n_null

    logger.success(f"  gold.features_final : {count:,} lignes")
    for col, n_ok in coverage.items():
        pct = n_ok / count * 100 if count else 0
        logger.info(f"    {col:<30} : {n_ok:,}/{count:,} non-null ({pct:.1f}%)")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering Silver → Gold"
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Supprime et recrée le schéma gold"
    )
    args = parser.parse_args()
    run_features_pipeline(reset=args.reset)