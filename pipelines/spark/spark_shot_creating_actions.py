#!/usr/bin/env python3
"""
spark_shot_creating_actions.py — Actions créatrices de tir (SCA / GCA) dans Spark
=================================================================================
Remplace le modèle dbt intermediate.intermediate_shot_creating_actions
(1 h 57 en dbt/DuckDB sur 18 saisons).

Pour chaque tir, on crédite les 2 actions offensives qui l'ont précédé DANS LA
MÊME CHAÎNE DE POSSESSION, côté équipe qui tire (passe, dribble, tir précédent),
plus la faute obtenue qui a provoqué un coup franc (hors chaîne, seq = 0).

Entrée (produite par spark_possession_chains.py) :
    data/spark_out/int_possession_chains/   str_season=*/
Sortie (Parquet zstd, partitionné par str_season) :
    data/spark_out/int_shot_creating_actions/

Une session Spark PAR SAISON : le spill est libéré à chaque arrêt de session.

Usage (depuis la racine du projet) :
    python pipelines/spark/spark_shot_creating_actions.py --season 2023-2024 --dry-run
    python pipelines/spark/spark_shot_creating_actions.py --season 2023-2024
    python pipelines/spark/spark_shot_creating_actions.py --from-report     # orchestrateur
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spark_session import get_spark, ROOT_DIR, SPARK_CFG
from spark_events import log_plan, write_out, RUN_REPORT


SPARK_OUT = ROOT_DIR / SPARK_CFG["paths"]["output"]
IN_NAME   = "int_possession_chains"
OUT_NAME  = "int_shot_creating_actions"

SHOT_TYPE_IDS    = [13, 14, 15, 16]          # tirs ; 16 = Goal -> crédits GCA
CREATOR_TYPE_IDS = [1, 3, 13, 14, 15, 16]    # Pass, TakeOn, tir précédent (rebond)


def event_order():
    """Ordre chronologique, NULLS LAST comme le défaut de DuckDB (Spark : NULLS FIRST).

    `second` est NULL sur ~8 800 événements par saison : sans cela, ils passent en
    tête et tous les rangs (ROW_NUMBER) divergent du SQL d'origine.
    """
    from pyspark.sql import functions as F
    return [F.col("expanded_minute").asc_nulls_last(),
            F.col("second").asc_nulls_last(),
            F.col("row_num").asc_nulls_last()]


def _seasons_in(path: Path) -> set[str]:
    return {d.name.split("=", 1)[1] for d in path.glob("str_season=*") if d.is_dir()}


def seasons_from_report() -> list[str]:
    """Saisons à calculer pour l'orchestrateur : écrites par spark_events + manquantes.

    Même règle que spark_possession_chains : une saison présente en entrée (chaînes)
    mais absente de la sortie serait lue manquante par dbt SANS erreur.
    """
    import json

    disponibles = _seasons_in(SPARK_OUT / IN_NAME)
    try:
        ecrites = set(json.loads(RUN_REPORT.read_text(encoding="utf-8")).get("seasons") or [])
    except Exception as e:
        logger.warning(f"Rapport spark_events illisible ({e}) : seules les saisons "
                       f"manquantes en sortie seront calculées.")
        ecrites = set()
    manquantes = disponibles - _seasons_in(SPARK_OUT / OUT_NAME)
    return sorted((ecrites & disponibles) | manquantes)


def read_chains(spark, seasons: list[str]):
    """Lit les chaînes de possession sous les noms de travail du SQL dbt."""
    from pyspark.sql import functions as F

    path = SPARK_OUT / IN_NAME
    if not path.exists():
        raise FileNotFoundError(
            f"Entrée absente : {path}\n"
            f"Lance d'abord : python pipelines/spark/spark_possession_chains.py --from-report")
    df = (spark.read.option("basePath", str(path)).parquet(str(path))
               .filter(F.col("str_season").isin(seasons)))
    return df.select(
        F.col("str_match_id").alias("match_id"),
        F.col("str_season").alias("season"),
        F.col("str_league_source").alias("league_source"),
        F.col("str_chain_id").alias("chain_id"),
        F.col("str_chain_team_id").cast("bigint").alias("chain_team_id"),
        F.col("str_team_id").cast("bigint").alias("team_id"),
        F.col("str_player_id").cast("int").alias("player_id"),
        F.col("str_event_id").cast("int").alias("event_id"),
        F.col("int_row_num").alias("row_num"),
        F.col("int_expanded_minute").alias("expanded_minute"),
        F.col("int_second").alias("second"),
        F.col("str_type_id").cast("int").alias("type_id"),
        F.col("str_type_name").alias("type_name"),
        F.col("str_outcome_id").cast("int").alias("outcome_id"),
        F.col("str_chain_trigger").alias("chain_trigger"),
        F.col("dt_scraped_at").alias("scraped_at"),
    )


def shot_creating_actions(ev):
    """Blocs SQL : chains_with_shot, chain_seq, shots, fk_origin_stream,
    freekick_origin_foul, sca_candidates, sca (+ filtre sca_order <= 2).

    Rend une ligne par crédit : (tir, sca_order 1|2), avec les colonnes de travail
    du SQL (is_goal = le tir est un but -> crédit GCA).
    """
    from pyspark.sql import Window, functions as F

    is_shot = F.col("type_id").isin(*SHOT_TYPE_IDS)

    # chains_with_shot : seules les chaînes contenant un tir peuvent produire un
    # crédit (~8 % des chaînes). On y restreint tout ce qui suit.
    chains_with_shot = ev.filter(is_shot).select("match_id", "chain_id").distinct()

    # chain_seq : rang de chaque événement DANS sa chaîne (ordre chronologique).
    chain_seq = (
        ev.join(chains_with_shot, on=["match_id", "chain_id"], how="inner")
          .withColumn("seq_in_chain", F.row_number().over(
              Window.partitionBy("match_id", "chain_id").orderBy(*event_order())))
    )

    # shots : le pivot. Colonnes préfixées pour éviter toute ambiguïté de jointure.
    shots = chain_seq.filter(is_shot).select(
        "match_id", "chain_id", "season", "league_source", "scraped_at",
        F.col("row_num").alias("shot_row_num"),
        F.col("event_id").alias("shot_event_id"),
        F.col("team_id").alias("attacking_team_id"),
        F.col("player_id").alias("shot_taker_player_id"),
        (F.col("type_id") == 16).alias("is_goal"),
        F.col("seq_in_chain").alias("shot_seq"),
    )

    # --- Source 1 : actions éligibles internes à la chaîne, de l'équipe qui tire,
    # strictement avant le tir. Préfixe c_ sur le côté « action ».
    creators = chain_seq.filter(F.col("type_id").isin(*CREATOR_TYPE_IDS)).select(
        F.col("match_id").alias("c_match_id"),
        F.col("chain_id").alias("c_chain_id"),
        F.col("team_id").alias("c_team_id"),
        F.col("player_id").alias("creator_player_id"),
        F.col("type_name").alias("action_type"),
        F.col("row_num").alias("action_row_num"),
        F.col("seq_in_chain").alias("c_seq"),
    )
    from_chain = shots.join(
        creators,
        on=[F.col("match_id") == F.col("c_match_id"),
            F.col("chain_id") == F.col("c_chain_id"),
            F.col("c_team_id") == F.col("attacking_team_id"),
            F.col("c_seq") < F.col("shot_seq")],
        how="inner",
    ).select("match_id", "shot_row_num", "shot_event_id", "chain_id", "season",
             "league_source", "scraped_at", "attacking_team_id",
             "shot_taker_player_id", "is_goal", "creator_player_id", "action_type",
             "action_row_num", F.col("c_seq").alias("seq_in_chain"))

    # --- Source 2 : la faute obtenue qui a provoqué le coup franc (fk_origin_stream
    # + freekick_origin_foul). Une faute obtenue TERMINE sa chaîne : elle vit dans
    # la chaîne N, le coup franc et le tir dans la N+1. On relie, par (match,
    # équipe), la DERNIÈRE faute obtenue AVANT le 1er événement de la chaîne.
    foul_struct = F.struct(F.col("player_id").alias("pid"),
                           F.col("type_name").alias("tn"),
                           F.col("row_num").alias("rn"))
    fouls = (
        ev.filter((F.col("type_id") == 4) & (F.col("outcome_id") == 1))
          .select("match_id", "team_id", "row_num", foul_struct.alias("foul"),
                  F.lit(None).cast("string").alias("chain_id"),
                  F.lit(0).alias("is_target"))
    )
    foul_type = fouls.schema["foul"].dataType
    targets = (
        ev.join(chains_with_shot, on=["match_id", "chain_id"], how="inner")
          .filter(F.col("chain_trigger") == "free_kick")
          .groupBy("match_id", "chain_id", "chain_team_id")
          .agg(F.min("row_num").alias("first_row"))
          .select("match_id", F.col("chain_team_id").alias("team_id"),
                  F.col("first_row").alias("row_num"),
                  F.lit(None).cast(foul_type).alias("foul"),
                  "chain_id", F.lit(1).alias("is_target"))
    )
    stream = fouls.unionByName(targets)

    w_fk = (Window.partitionBy("match_id", "team_id")
                  .orderBy(F.col("row_num").asc_nulls_last(), F.col("is_target").asc())
                  .rowsBetween(Window.unboundedPreceding, -1))
    freekick_origin_foul = (
        stream.withColumn("prev_foul", F.last("foul", ignorenulls=True).over(w_fk))
              .filter((F.col("is_target") == 1) & F.col("prev_foul").isNotNull())
              .select(F.col("match_id").alias("f_match_id"),
                      F.col("team_id").alias("f_team_id"),
                      F.col("chain_id").alias("f_chain_id"),
                      F.col("prev_foul.pid").alias("f_player_id"),
                      F.col("prev_foul.tn").alias("f_type_name"),
                      F.col("prev_foul.rn").alias("f_row_num"))
    )
    from_foul = shots.join(
        freekick_origin_foul,
        on=[F.col("match_id") == F.col("f_match_id"),
            F.col("chain_id") == F.col("f_chain_id"),
            F.col("f_team_id") == F.col("attacking_team_id")],
        how="inner",
    ).select("match_id", "shot_row_num", "shot_event_id", "chain_id", "season",
             "league_source", "scraped_at", "attacking_team_id",
             "shot_taker_player_id", "is_goal",
             F.col("f_player_id").alias("creator_player_id"),
             F.col("f_type_name").alias("action_type"),
             F.col("f_row_num").alias("action_row_num"),
             F.lit(0).alias("seq_in_chain"))     # avant tout événement de la chaîne

    # sca_candidates -> sca : 1 = action juste avant le tir, 2 = celle d'avant.
    candidates = from_chain.unionByName(from_foul)
    w_sca = Window.partitionBy("match_id", "shot_row_num").orderBy(
        F.col("seq_in_chain").desc())
    return (candidates.withColumn("sca_order", F.row_number().over(w_sca))
                      .filter(F.col("sca_order") <= 2))


def to_output(df):
    """Renommage et casts du bloc mdl_out : le contrat de sortie du modèle dbt."""
    from pyspark.sql import functions as F

    out = df.select(
        F.col("match_id").alias("str_match_id"),
        F.col("shot_row_num").alias("int_shot_row_num"),
        F.col("sca_order").cast("bigint").alias("int_sca_order"),
        F.col("shot_event_id").cast("string").alias("str_shot_event_id"),
        F.col("chain_id").alias("str_chain_id"),
        F.col("season").alias("str_season"),
        F.col("league_source").alias("str_league_source"),
        F.col("scraped_at").alias("dt_scraped_at"),
        F.col("attacking_team_id").cast("string").alias("str_attacking_team_id"),
        F.col("shot_taker_player_id").cast("string").alias("str_shot_taker_player_id"),
        F.col("creator_player_id").cast("string").alias("str_creator_player_id"),
        F.col("action_type").alias("str_action_type"),
        F.col("action_row_num").alias("int_action_row_num"),
        F.col("is_goal").alias("bool_is_gca"),
    )
    return out   # clés en tête (match, tir, ordre) : convention DuckDB 1.5.1


def log_checks(df) -> None:
    """Agrégats de contrôle, à comparer à la référence (SQL d'origine / table dev)."""
    from pyspark.sql import functions as F

    rows = df.groupBy("season").agg(
        F.count("*").alias("n"),
        F.sum("sca_order").alias("sum_order"),
        F.sum(F.col("shot_row_num") * F.col("sca_order")).alias("w_shot"),
        F.sum("action_row_num").alias("sum_action_row"),
        F.countDistinct("match_id", "shot_row_num").alias("n_shots"),
        F.sum(F.col("is_goal").cast("int")).alias("n_gca"),
        F.sum(F.when(F.col("action_type") == "Foul", 1).otherwise(0)).alias("n_foul"),
        F.sum(F.when(F.col("sca_order") == 1, 1).otherwise(0)).alias("n_order1"),
        F.sum(F.when(F.col("sca_order") == 2, 1).otherwise(0)).alias("n_order2"),
    ).orderBy("season").collect()
    keys = ("n", "sum_order", "w_shot", "sum_action_row", "n_shots", "n_gca",
            "n_foul", "n_order1", "n_order2")
    for r in rows:
        logger.info(f"[checks] {r['season']} " + " ".join(f"{k}={r[k]}" for k in keys))


def process_season(season: str, dry_run: bool) -> None:
    spark = get_spark("3etoiles-sca")
    logger.info(f"Spark {spark.version} — UI : {spark.sparkContext.uiWebUrl}")
    try:
        sca = shot_creating_actions(read_chains(spark, [season]))
        if dry_run:
            log_checks(sca)
        else:
            out = to_output(sca)
            log_plan(out, OUT_NAME)
            write_out(spark, out, OUT_NAME)
    finally:
        spark.stop()


def main(seasons: list[str] | None = None, all_seasons: bool = False,
         dry_run: bool = False, from_report: bool = False) -> int:
    if from_report:
        seasons = seasons_from_report()
        if not seasons:
            logger.info("Aucune saison à (re)calculer : crédits SCA déjà à jour.")
            return 0
    elif all_seasons:
        seasons = sorted(_seasons_in(SPARK_OUT / IN_NAME))
    elif not seasons:
        logger.error("Précise --season SAISON (répétable), --all ou --from-report.")
        return 2

    logger.info(f"→ {len(seasons)} saison(s), une session Spark chacune : {', '.join(seasons)}")
    t_all = time.time()
    for i, season in enumerate(seasons, 1):
        t0 = time.time()
        logger.info(f"══ [{i}/{len(seasons)}] {season} ══")
        try:
            process_season(season, dry_run)
        except Exception:
            logger.exception(f"Échec sur {season} après {i - 1} saison(s) réussie(s).")
            return 1
        logger.success(f"{season} terminée en {time.time() - t0:,.0f}s "
                       f"(total {time.time() - t_all:,.0f}s)")
    return 0


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/spark_shot_creating_actions.log", level="DEBUG", rotation="5 MB",
               retention=10, encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--season", action="append", default=None, metavar="SAISON",
                    help="traite cette saison (répétable).")
    ap.add_argument("--all", action="store_true", dest="all_seasons",
                    help="traite toutes les saisons présentes en entrée.")
    ap.add_argument("--from-report", action="store_true", dest="from_report",
                    help="mode orchestrateur : saisons écrites par spark_events "
                         "+ saisons manquantes en sortie.")
    ap.add_argument("--dry-run", action="store_true",
                    help="affiche les agrégats de contrôle sans rien écrire.")
    args = ap.parse_args()
    sys.exit(main(seasons=args.season, all_seasons=args.all_seasons,
                  dry_run=args.dry_run, from_report=args.from_report))
