#!/usr/bin/env python3
"""
spark_events.py — Chaîne événementielle WhoScored dans Spark
=============================================================
Remplace trois modèles dbt par un job Spark unique :
    intermediate.int_whoscored_events   (jointure d'identité + remapping team_id)
    intermediate.events_qual            (explosion du JSON des qualifiers)
    intermediate.int_event_enriched     (score courant + pivot des 10 flags)

Entrées  (produites par export_to_parquet.py) :
    data/spark_in/events/season=*/        stg_whoscored_events, brut
    data/spark_in/match_index/*.parquet   int_whoscored_match_index

Sorties (Parquet zstd, partitionnées par season) :
    data/spark_out/int_whoscored_events/
    data/spark_out/events_qual/
    data/spark_out/int_event_enriched/

FIDÉLITÉ : la logique reproduit les modèles dbt À L'IDENTIQUE, y compris là
où elle est perfectible (score détecté via type_id/outcome_id/is_shot plutôt
que via la colonne is_goal qui existe pourtant). Objectif : des chiffres
strictement comparables pour la validation. Les simplifications viendront
après la parité.

DIVERGENCE ASSUMÉE : qualifiers_json n'est pas repris dans la sortie
int_whoscored_events. Vérifié : aucun modèle dbt ne la lit hors events_qual,
qui est calculée ici. C'est la colonne la plus lourde du jeu.

Usage (venv principal, depuis la racine du projet) :
    python pipelines/spark/spark_events.py
    python pipelines/spark/spark_events.py --season 2023-2024
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spark_session import get_spark, ROOT_DIR, SPARK_CFG


IN_DIR  = ROOT_DIR / SPARK_CFG["paths"]["input"]
OUT_DIR = ROOT_DIR / SPARK_CFG["paths"]["output"]


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES MÉTIER — reprises telles quelles des modèles dbt
# ══════════════════════════════════════════════════════════════════════════════

# Les 10 qualifiers pivotés par int_event_enriched. L'ordre est celui du SQL,
# pour que la comparaison colonne à colonne reste lisible.
QUAL_FLAGS: dict[int, str] = {
    170:   "is_leading_to_goal",
    11111: "is_intentional_goal_assist",
    154:   "is_intentional_assist",
    11112: "is_big_chance_created",
    11113: "is_key_pass",
    210:   "is_shot_assist",
    169:   "is_leading_to_attempt",
    285:   "has_defensive_qual",
    286:   "has_offensive_qual",
    233:   "has_opposite_event",
}

# Détection d'un but, à l'identique du SQL dbt :
#   type_id = 16 AND outcome_id = 1 AND is_shot = TRUE
TYPE_ID_GOAL = 16
OUTCOME_SUCCESS = 1


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Lecture
# ══════════════════════════════════════════════════════════════════════════════

def read_inputs(spark, season: str | None):
    """
    Lit les deux jeux Parquet d'entrée.

    Args:
        season : si fourni, ne lit que cette saison (partition pruning —
                 Spark élimine les dossiers season=… sans ouvrir un fichier).

    Returns:
        (events, index) : deux DataFrames.
    """
    from pyspark.sql import functions as F

    events_path = IN_DIR / "events"
    index_path  = IN_DIR / "match_index"

    for p in (events_path, index_path):
        if not p.exists():
            raise FileNotFoundError(
                f"Entrée absente : {p}\n"
                f"Lance d'abord : python pipelines/spark/export_to_parquet.py"
            )

    # basePath : indique à Spark la racine de l'arborescence partitionnée, pour
    # qu'il reconstruise la colonne `season` depuis les noms de dossiers même
    # quand on pointe un sous-chemin.
    events = spark.read.option("basePath", str(events_path)).parquet(str(events_path))
    if season:
        events = events.filter(F.col("season") == season)

    index = spark.read.parquet(str(index_path))

    return events, index


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Résolution d'identité  (= modèle int_whoscored_events)
# ══════════════════════════════════════════════════════════════════════════════

def resolve_identity(events, index):
    """
    Attache le match_id unifié et le contexte match aux événements.

    Une seule traduction : ws_match_id → match_id. Les identités d'équipe sont
    déjà résolues en amont dans silver.stg_whoscored_events — team_id passe
    donc tel quel, sans CASE de conversion.

    La jointure est diffusée (broadcast) : l'index fait quelques milliers de
    lignes, Spark en envoie une copie à chaque tâche qui joint en local.
    Aucun shuffle.

    INNER JOIN délibéré (décision B) : un événement dont le match n'est pas
    résolu au registre est écarté — c'est déjà ce que fait int_event_enriched
    via son INNER JOIN match_dates. Le volume écarté est journalisé.

    idx_home_team_id / idx_away_team_id ne servent qu'au contrôle de cohérence
    de report_join_quality(). Elles sont retirées avant écriture.
    """
    from pyspark.sql import functions as F

    idx = index.select(
        F.col("ws_match_id"),
        F.col("match_id"),
        # Les deux équipes du match, côté index — uniquement pour le contrôle.
        F.col("ws_home_team_id").alias("ws_home"),
        F.col("ws_away_team_id").alias("ws_away"),
        F.col("team_id").alias("canon_home"),
        F.col("opponent_id").alias("canon_away"),
        # Préfixées idx_ : int_event_enriched prend son contexte match de
        # l'INDEX, pas des événements (fidélité au CTE match_dates du SQL dbt).
        F.col("match_date").alias("idx_match_date"),
        F.col("season").alias("idx_season"),
        F.col("league_source").alias("idx_league_source"),
        F.col("scraped_at").alias("idx_scraped_at"),
    ).filter(F.col("match_id").isNotNull())

    return (
        events
        .join(F.broadcast(idx), on="ws_match_id", how="inner")
        .withColumn(
            "_team_canon",
            F.when(F.col("team_id") == F.col("ws_home"),
                   F.col("canon_home"))
             .when(F.col("team_id") == F.col("ws_away"),
                   F.col("canon_away"))
             .otherwise(F.lit(None)),
        )
        .drop("team_id", "ws_match_id", "ws_home", "ws_away",
              "canon_home", "canon_away")
        .withColumnRenamed("_team_canon", "team_id")
    )


def report_join_quality(spark, events, resolved) -> None:
    """
    Chiffre ce que la jointure a écarté et contrôle la cohérence des identités.

    Le contrôle central : chaque événement doit porter le team_id de l'une des
    DEUX équipes de son match. Si ce n'est pas le cas, c'est que team_id n'est
    pas dans le même espace d'identifiants que l'index — et tout l'aval
    attribuerait les actions aux mauvaises équipes, SANS lever d'erreur.
    On le mesure ici pour que ça se voie dans le log.
    """
    from pyspark.sql import functions as F

    n_in  = events.count()
    n_out = resolved.count()
    n_orph = n_in - n_out

    logger.info(f"Événements en entrée        : {n_in:,}")
    logger.info(f"Événements après jointure   : {n_out:,}")
    if n_orph:
        logger.warning(
            f"Écartés (match non résolu)  : {n_orph:,} ({n_orph / n_in:.2%})"
        )

    n_team_null = resolved.filter(F.col("team_id").isNull()).count()
    if n_team_null:
        logger.warning(
            f"team_id NULL                : {n_team_null:,} ({n_team_null / n_out:.2%})"
        )

    # ── Contrôle d'espace d'identifiants ──────────────────────────────────
    # eqNullSafe : un NULL des deux côtés ne doit pas compter comme un match.
    # Après le CASE, un team_id NULL signifie que l'id WhoScored de l'événement
    # ne correspondait NI à ws_home_team_id NI à ws_away_team_id de son match.
    n_team_null = resolved.filter(F.col("team_id").isNull()).count()
    if n_team_null == 0:
        logger.success(f"Identités d'équipe          : {n_out:,} / {n_out:,} traduites.")
    else:
        logger.warning(
            f"team_id non traduit         : {n_team_null:,} ({n_team_null / n_out:.2%})"
        )

    # Cohérence season entre les deux sources.
    n_season_mismatch = resolved.filter(
        ~F.col("season").eqNullSafe(F.col("idx_season"))
    ).count()
    if n_season_mismatch:
        logger.warning(
            f"season divergente events/idx: {n_season_mismatch:,} lignes"
        )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — events_qual  (explosion du JSON)
# ══════════════════════════════════════════════════════════════════════════════

def _qual_schema():
    """
    Schéma explicite du tableau JSON des qualifiers.

    Déclaré en dur plutôt que laissé à l'inférence : schema inference sur du
    JSON oblige Spark à SCANNER TOUT LE JEU une première fois rien que pour
    deviner la structure — un job complet gaspillé.

    Forme attendue : [{"type": {"value": 170, "displayName": "..."},
                       "value": "..."}, ...]
    """
    from pyspark.sql import types as T

    return T.ArrayType(T.StructType([
        T.StructField("type", T.StructType([
            T.StructField("value",       T.IntegerType()),
            T.StructField("displayName", T.StringType()),
        ])),
        T.StructField("value", T.StringType()),
    ]))


def build_events_qual(resolved):
    """
    Une ligne par (événement × qualifier) — équivalent du UNNEST dbt.

    Même filtre que le SQL : on écarte les qualifiers_json NULL et '[]' AVANT
    de parser. Inutile de faire tourner le parseur JSON sur des tableaux vides.

    Colonnes de sortie : celles du modèle dbt events_qual, plus `season` pour
    le partitionnement.
    """
    from pyspark.sql import functions as F

    exploded = (
        resolved
        .filter(F.col("qualifiers_json").isNotNull()
                & (F.col("qualifiers_json") != F.lit("[]")))
        .withColumn("_quals", F.from_json("qualifiers_json", _qual_schema()))
        .withColumn("_q", F.explode("_quals"))
    )

    return exploded.select(
        "match_id", "team_id", "player_id", "event_id",
        "minute", "second", "expanded_minute", "period",
        "x", "y", "end_x", "end_y",
        "type_id", "type_name", "outcome_id",
        "is_touch", "is_shot", "row_num",
        F.col("_q.type.value").alias("qual_type_id"),
        F.col("_q.type.displayName").alias("qual_type_name"),
        F.col("_q.value").alias("qual_value"),
        "season",
    )


def build_qual_flags(events_qual):
    """
    Pivot des 10 qualifiers en 10 colonnes 0/1, au grain (match_id, row_num).

    On n'utilise PAS .groupBy().pivot() : cette API lance un job SUPPLÉMENTAIRE
    pour découvrir les valeurs distinctes de la colonne pivotée. Comme les 10
    qual_type_id sont connus à l'avance, on écrit des agrégations explicites —
    exactement la forme du SQL dbt, et zéro job en plus.

    Le groupBy porte sur match_id : le DataFrame étant déjà repartitionné par
    match_id, Spark reconnaît que la distribution requise est satisfaite et
    n'ajoute pas d'Exchange. L'agrégation se fait en local dans chaque partition.
    """
    from pyspark.sql import functions as F

    aggs = [
        F.max(F.when(F.col("qual_type_id") == tid, 1).otherwise(0)).alias(name)
        for tid, name in QUAL_FLAGS.items()
    ]
    return (
        events_qual
        .filter(F.col("qual_type_id").isin(list(QUAL_FLAGS)))
        .groupBy("match_id", "row_num")
        .agg(*aggs)
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — int_event_enriched  (score courant + jointure des flags)
# ══════════════════════════════════════════════════════════════════════════════

def build_event_enriched(resolved, qual_flags):
    """
    Score courant de chaque équipe à l'instant de l'événement, puis jointure
    des 10 flags.

    Trois subtilités de fidélité au SQL dbt :

    1. team_1 / team_2 (MIN/MAX des team_id du match) sont calculés sur TOUS
       les événements — le CTE match_teams du SQL n'a pas de filtre player_id.
    2. Les sommes cumulées, elles, portent sur les événements filtrés
       (player_id IS NOT NULL), puisque le SQL les calcule dans events_scored
       qui a ce WHERE.
       → d'où l'ordre : team_1/team_2, PUIS filtre, PUIS cumuls.
    3. match_date / season / league_source / scraped_at viennent de l'INDEX
       (CTE match_dates), pas des événements.

    Les deux fenêtres ne peuvent pas être imbriquées dans un même select —
    même contrainte qu'en DuckDB. On les sépare en deux étapes ; Spark planifie
    deux opérateurs Window, tous deux locaux à la partition match_id.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # ── Étape 1 : les deux équipes du match ──────────────────────────────────
    w_match = Window.partitionBy("match_id")
    with_teams = (
        resolved
        .withColumn("team_1", F.min("team_id").over(w_match))
        .withColumn("team_2", F.max("team_id").over(w_match))
        .filter(F.col("player_id").isNotNull())
    )

    # ── Étape 2 : sommes cumulées des buts, par équipe ───────────────────────
    # rowsBetween(unboundedPreceding, currentRow) = ROWS BETWEEN UNBOUNDED
    # PRECEDING AND CURRENT ROW. Le tri se fait sur row_num, l'ordre
    # chronologique des actions du match.
    w_cum = (
        Window.partitionBy("match_id")
        .orderBy("row_num")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    is_goal_event = (
        (F.col("type_id") == TYPE_ID_GOAL)
        & (F.col("outcome_id") == OUTCOME_SUCCESS)
        & (F.col("is_shot") == F.lit(True))
    )

    scored = (
        with_teams
        .withColumn(
            "t1_score",
            F.sum(F.when(is_goal_event & (F.col("team_id") == F.col("team_1")), 1)
                   .otherwise(0)).over(w_cum),
        )
        .withColumn(
            "t2_score",
            F.sum(F.when(is_goal_event & (F.col("team_id") == F.col("team_2")), 1)
                   .otherwise(0)).over(w_cum),
        )
    )

    # ── Étape 3 : jointure des flags ─────────────────────────────────────────
    # Sur (match_id, row_num). Les deux DataFrames descendent du même
    # repartition("match_id") : la jointure est co-partitionnée, donc locale.
    # LEFT : un événement sans qualifier pertinent garde des flags à 0.
    joined = scored.join(qual_flags, on=["match_id", "row_num"], how="left")

    flag_cols = [
        F.coalesce(F.col(name), F.lit(0)).alias(name)
        for name in QUAL_FLAGS.values()
    ]

    return joined.select(
        "match_id", "team_id", "player_id", "event_id", "row_num",
        "expanded_minute", "second", "period",
        "type_id", "type_name", "outcome_id",
        "is_shot", "is_touch",
        "x", "y", "end_x", "end_y",
        "is_own_goal", "related_event_id", "related_player_id", "card_type",
        "goal_mouth_y", "goal_mouth_z", "blocked_x", "blocked_y",
        # Contexte match : pris de l'index, comme le CTE match_dates du SQL.
        F.col("idx_match_date").alias("match_date"),
        F.col("idx_league_source").alias("league_source"),
        F.col("idx_scraped_at").alias("scraped_at"),
        *flag_cols,
        # Routage : si je suis l'équipe 1, mon score est t1_score, sinon t2_score.
        F.when(F.col("team_id") == F.col("team_1"), F.col("t1_score"))
         .otherwise(F.col("t2_score")).alias("team_score"),
        F.when(F.col("team_id") == F.col("team_1"), F.col("t2_score"))
         .otherwise(F.col("t1_score")).alias("opp_score"),
        # Colonne de partitionnement, en dernier (convention Spark).
        F.col("idx_season").alias("season"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Écriture
# ══════════════════════════════════════════════════════════════════════════════
def log_plan(df, name: str) -> int:
    """
    Imprime le plan physique dans le log et compte les Exchange.

    Un `Exchange` = un shuffle : Spark redistribue les données entre partitions
    (écriture disque + transfert + relecture). C'est LE poste de coût d'un job
    Spark, et le nombre d'Exchange est la meilleure métrique de qualité d'un
    plan — bien avant le temps d'exécution, qui dépend de la machine.

    df.explain() écrit sur la sortie standard (un print Python), pas via un
    logger. On redirige stdout dans un tampon pour récupérer le texte et le
    passer à loguru, afin qu'il atterrisse dans logs/spark_events.log.

    Note AQE : avec l'optimisation adaptative activée, ce plan est celui
    d'AVANT exécution (`isFinalPlan=false`). Le plan réellement exécuté peut
    différer — c'est celui que montre la Spark UI.
    """
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain()
    plan = buf.getvalue()

    n_exchange = plan.count("Exchange")
    logger.info(f"╔═ Plan physique : {name} — {n_exchange} Exchange")
    for line in plan.splitlines():
        if line.strip():
            logger.info(f"║ {line}")
    logger.info("╚" + "═" * 60)
    return n_exchange

def write_out(spark, df, name: str) -> int:
    """
    Écrit un DataFrame en Parquet partitionné par saison, et rend son nombre
    de lignes.

    Le comptage se fait en RELISANT le Parquet, pas sur le DataFrame : Spark
    lit alors les métadonnées des row groups au lieu de rejouer tout le calcul.
    Compter le DataFrame déclencherait un second job complet.
    """
    path = OUT_DIR / name
    t0 = time.time()
    (
        df.write
        .mode("overwrite")
        .partitionBy("season")
        .parquet(str(path))
    )
    n = spark.read.parquet(str(path)).count()
    logger.success(
        f"{name:<22} {n:>12,} lignes   {time.time() - t0:>6,.0f}s   → {path}"
    )
    return n


# ══════════════════════════════════════════════════════════════════════════════

def main(season: str | None = None) -> int:
    from pyspark import StorageLevel

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("3etoiles-events")
    logger.info(f"Spark {spark.version} — UI : {spark.sparkContext.uiWebUrl}")

    try:
        events, index = read_inputs(spark, season)

        # ── Résolution d'identité ────────────────────────────────────────────
        resolved = resolve_identity(events, index)

        # ── LE shuffle, une seule fois ───────────────────────────────────────
        # Tout le travail qui suit se groupe par match : le pivot des
        # qualifiers (match_id, row_num), les fenêtres du score
        # (PARTITION BY match_id), la jointure finale. Une seule
        # redistribution, puis tout est local à la partition.
        resolved = resolved.repartition("match_id")

        # ── Cache : ce DataFrame alimente les TROIS sorties ──────────────────
        # Sans persist, Spark rejouerait lecture + jointure + shuffle à chaque
        # write : un DataFrame n'est pas un résultat, c'est une recette, et
        # chaque action la réexécute intégralement.
        # MEMORY_AND_DISK : ce qui tient dans le heap y reste, le reste déborde
        # sur spark.local.dir. Garantie de ne pas tomber en OOM.
        resolved = resolved.persist(StorageLevel.MEMORY_AND_DISK)

        report_join_quality(spark, events, resolved)   # remplit aussi le cache

        # ── Sortie 1 : int_whoscored_events ──────────────────────────────────
        out1 = resolved.drop("qualifiers_json", "idx_match_date", "idx_season",
                             "idx_league_source", "idx_scraped_at")
        
        # CONTOURNEMENT bug DuckDB 1.5.1 : lors de la réécriture d'un QUALIFY
        # sur une vue read_parquet(), DuckDB résout la colonne par position et
        # non par nom. Si match_id n'est pas la colonne 1, il lit la mauvaise
        # et lève "Failed to bind column reference ... inequal types".
        # CONVENTION : toute sortie Parquet lue via une vue DuckDB commence
        # par ses clés.
        _keys = ["match_id", "team_id", "row_num", "event_id"]
        out1 = out1.select(*_keys, *[c for c in out1.columns if c not in _keys])
        log_plan(out1, "int_whoscored_events")
        write_out(spark, out1, "int_whoscored_events")

        # ── Sortie 2 : events_qual ───────────────────────────────────────────
        events_qual = build_events_qual(resolved)
        log_plan(events_qual, "events_qual")
        write_out(spark, events_qual, "events_qual")

        # ── Sortie 3 : int_event_enriched ────────────────────────────────────
        qual_flags = build_qual_flags(
            spark.read.parquet(str(OUT_DIR / "events_qual"))
        )
        enriched = build_event_enriched(resolved, qual_flags)
        log_plan(enriched, "int_event_enriched")
        write_out(spark, enriched, "int_event_enriched")

        resolved.unpersist()

    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/spark_events.log", level="DEBUG", rotation="5 MB",
               retention=10, encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default=None,
                    help="ne traite qu'une saison, ex: 2023-2024")
    args = ap.parse_args()

    sys.exit(main(season=args.season))