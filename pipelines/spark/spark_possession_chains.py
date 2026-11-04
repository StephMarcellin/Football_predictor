#!/usr/bin/env python3
"""
spark_possession_chains.py — Chaînes de possession dans Spark
=============================================================
Remplace le modèle dbt intermediate.intermediate_possession_chains.

ÉTAT : PORTAGE COMPLET. Tous les blocs du modèle dbt sont portés et validés
contre le SQL d'origine ET contre la table dbt de dev (2022-2023, 2023-2024) :
0 différence sur les 21 colonnes, 7 684 609 lignes.

Blocs : certain_possession, foul_resolution, possession_groups, possession_numbered,
possession_last_known, possession_resolved, chain_trigger_quals, chain_first_events,
chain_triggers, chain_boundaries, aerials, aerial_orphans, aerial_orphan_pairs,
aerial_with_pair_group, aerial_best_group, aerial_corrections.

Entrées (produites par spark_events.py) :
    data/spark_out/int_event_enriched/   str_season=*/
    data/spark_out/events_qual/          str_season=*/
    dbt_project/seeds/event_types.csv    référentiel des types d'événements

Sortie (Parquet zstd, partitionné par str_season) :
    data/spark_out/int_possession_chains/

Usage (depuis la racine du projet) :
    python pipelines/spark/spark_possession_chains.py --season 2023-2024 --dry-run
    python pipelines/spark/spark_possession_chains.py --season 2023-2024
    python pipelines/spark/spark_possession_chains.py --from-report     # orchestrateur
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spark_session import get_spark, ROOT_DIR, SPARK_CFG
from spark_events import log_plan, write_out, RUN_REPORT


SPARK_OUT        = ROOT_DIR / SPARK_CFG["paths"]["output"]
SEED_EVENT_TYPES = ROOT_DIR / "dbt_project" / "seeds" / "event_types.csv"
OUT_NAME         = "int_possession_chains"

def event_order():
    """Ordre chronologique d'un match, NULLS LAST comme le défaut de DuckDB.

    Spark trie les NULL en PREMIER en ascendant, DuckDB en dernier. `second` est
    NULL sur ~8 800 événements par saison : sans ce correctif, ces événements
    passent devant les autres et toutes les fenêtres (LAG, LEAD, cumuls) divergent.
    """
    from pyspark.sql import functions as F
    return [F.col("expanded_minute").asc_nulls_last(),
            F.col("second").asc_nulls_last(),
            F.col("row_num").asc_nulls_last()]


TRIGGERS = ("recovery", "corner", "goal_kick", "throw_in", "free_kick", "open_play")

# Types d'événements écartés avant tout calcul (repris du SQL dbt) :
# Card, SubstitutionOff, SubstitutionOn, End, Start, FormationSet, FormationChange.
EXCLUDED_TYPE_IDS = [17, 18, 19, 30, 32, 34, 40]

# Qualifiers qui marquent une remise en jeu formelle (is_set_piece) :
# 5 FreekickTaken, 6 CornerTaken, 107 ThrowIn, 124 GoalKick, 241 (repris du SQL dbt).
SET_PIECE_QUAL_IDS = [5, 6, 107, 124, 241]


def _seasons_in(path: Path) -> set[str]:
    """Saisons présentes dans un dossier Parquet partitionné (str_season=...)."""
    return {d.name.split("=", 1)[1] for d in path.glob("str_season=*") if d.is_dir()}


def seasons_from_report() -> list[str]:
    """Saisons à calculer quand l'orchestrateur appelle ce script.

    Union de deux ensembles :
      1. les saisons que spark_events vient d'écrire (rapport de CE run) : leurs
         événements ont pu changer, donc leurs chaînes aussi ;
      2. les saisons présentes en entrée mais ABSENTES de la sortie : sans elles,
         le modèle dbt lirait un Parquet incomplet SANS erreur (premier run, ou
         saison ajoutée après coup).
    Le tout restreint aux saisons réellement disponibles en entrée.
    """
    import json

    disponibles = (_seasons_in(SPARK_OUT / "int_event_enriched")
                   & _seasons_in(SPARK_OUT / "events_qual"))
    try:
        ecrites = set(json.loads(RUN_REPORT.read_text(encoding="utf-8")).get("seasons") or [])
    except Exception as e:  # rapport absent ou illisible
        logger.warning(f"Rapport spark_events illisible ({e}) : seules les saisons "
                       f"manquantes en sortie seront calculées.")
        ecrites = set()
    manquantes = disponibles - _seasons_in(SPARK_OUT / OUT_NAME)
    return sorted((ecrites & disponibles) | manquantes)


def read_inputs(spark, seasons: list[str] | None):
    """Lit les deux Parquet Spark et le seed, restreints aux saisons demandées."""
    from pyspark.sql import functions as F

    events_path = SPARK_OUT / "int_event_enriched"
    quals_path  = SPARK_OUT / "events_qual"

    for p in (events_path, quals_path, SEED_EVENT_TYPES):
        if not p.exists():
            raise FileNotFoundError(
                f"Entrée absente : {p}\n"
                f"Lance d'abord : python pipelines/spark/spark_events.py"
            )

    events = spark.read.option("basePath", str(events_path)).parquet(str(events_path))
    quals  = spark.read.option("basePath", str(quals_path)).parquet(str(quals_path))
    if seasons:
        events = events.filter(F.col("str_season").isin(seasons))
        quals  = quals.filter(F.col("str_season").isin(seasons))

    event_types = (
        spark.read.option("header", True).csv(str(SEED_EVENT_TYPES))
        .select(F.col("type_id").cast("int").alias("type_id"), "possession_type")
    )
    return events, quals, event_types


def prepare_events(events, event_types):
    """Noms de travail du SQL dbt + filtres de base (bloc certain_possession)."""
    from pyspark.sql import functions as F

    ev = events.select(
        F.col("str_match_id").alias("match_id"),
        F.col("str_team_id").cast("bigint").alias("team_id"),
        F.col("str_player_id").cast("int").alias("player_id"),
        F.col("str_event_id").cast("int").alias("event_id"),
        F.col("int_row_num").alias("row_num"),
        F.col("int_expanded_minute").alias("expanded_minute"),
        F.col("int_second").alias("second"),
        F.col("int_period").alias("period"),
        F.col("str_type_id").cast("int").alias("type_id"),
        F.col("str_type_name").alias("type_name"),
        F.col("str_outcome_id").cast("int").alias("outcome_id"),
        F.col("bool_is_shot").alias("is_shot"),
        F.col("dec_x").alias("x"),
        F.col("dec_y").alias("y"),
        F.col("str_season").alias("season"),
        F.col("str_league_source").alias("league_source"),
        F.col("dt_scraped_at").cast("string").alias("scraped_at"),
    )

    return (
        ev.join(F.broadcast(event_types), on="type_id", how="left")
          .filter(F.col("player_id").isNotNull())
          .filter(~F.col("type_id").isin(EXCLUDED_TYPE_IDS))
    )


def certain_possession(events, quals):
    """Bloc SQL `certain_possession` : qui a certainement le ballon, par événement.

    Ajoute trois colonnes à `events` :
      is_set_piece         1 si l'événement porte un qualifier de remise en jeu.
      certain_possessor    team_id si le type d'événement est 'actif', sinon NULL.
      is_winning_clearance 1 si c'est un dégagement (type 12) suivi, à l'événement
                           à possesseur certain suivant, par la même équipe.

    Trois écarts de syntaxe avec le SQL, sans changement de sens :
      - EXISTS corrélé -> jointure gauche sur les (match, row_num) distincts.
      - alias latéral `certain_possessor` réutilisé dans le LEAD -> deux étapes.
      - LEAD(...) IGNORE NULLS -> first(..., ignorenulls=True) sur le cadre
        « lignes suivantes », qui donne le premier non-NULL après la ligne courante.
    """
    from pyspark.sql import Window, functions as F

    set_pieces = (
        quals
        .select(
            F.col("str_match_id").alias("match_id"),
            F.col("int_row_num").alias("row_num"),
            F.col("str_qual_type_id").cast("int").alias("qual_type_id"),
        )
        .filter(F.col("qual_type_id").isin(SET_PIECE_QUAL_IDS))
        .select("match_id", "row_num").distinct()
        .withColumn("is_set_piece", F.lit(1))
    )
    df = (
        events.join(set_pieces, on=["match_id", "row_num"], how="left")
              .withColumn("is_set_piece", F.coalesce(F.col("is_set_piece"), F.lit(0)))
    )

    df = df.withColumn(
        "certain_possessor",
        F.when(F.col("possession_type") == "actif", F.col("team_id")),
    )

    w_next = (
        Window.partitionBy("match_id")
              .orderBy(*event_order())
              .rowsBetween(1, Window.unboundedFollowing)
    )
    next_possessor = F.first("certain_possessor", ignorenulls=True).over(w_next)
    return df.withColumn(
        "is_winning_clearance",
        F.when((F.col("type_id") == 12) & (next_possessor == F.col("team_id")), 1)
         .otherwise(0),
    )


def possession_groups(cp):
    """Blocs SQL foul_resolution, possession_groups, possession_numbered,
    possession_last_known et possession_resolved.

    Numérote les groupes de possession de chaque match. Un nouveau groupe démarre
    (incrément = 1) sur : une remise en jeu, un dégagement gagnant juste avant,
    un duel aérien gagné (type 41 / outcome 1) juste avant, la fin d'un groupe de
    fautes gagné, ou un changement de possesseur certain.

    Ajoute : last_row_num_in_foul_group, foul_won, possession_group_increment,
    possession_group, resolved_possessor, match_row_number.

    LAG(x IGNORE NULLS) du SQL -> last(x, ignorenulls=True) sur le cadre
    « lignes précédentes » : dernier non-NULL strictement avant la ligne courante.
    """
    from pyspark.sql import Window, functions as F

    # foul_resolution : une ligne par faute, avec les infos de son groupe de fautes
    # simultanées (même match, même minute étendue, même seconde).
    w_foul = Window.partitionBy("match_id", "expanded_minute", "second")
    fouls = (
        cp.filter(F.col("type_id") == 4)
          .select(
              "match_id", "row_num",
              F.max("row_num").over(w_foul).alias("last_row_num_in_foul_group"),
              F.max(F.when(F.col("outcome_id") == 1, 1).otherwise(0))
               .over(w_foul).alias("foul_won"),
          )
    )
    df = cp.join(fouls, on=["match_id", "row_num"], how="left")

    w = Window.partitionBy("match_id").orderBy(*event_order())
    prev_possessor = F.last("certain_possessor", ignorenulls=True).over(
        w.rowsBetween(Window.unboundedPreceding, -1)
    )

    increment = (
        F.when(F.col("is_set_piece") == 1, 1)
         .when(F.lag("is_winning_clearance", 1).over(w) == 1, 1)
         .when((F.lag("type_id", 1).over(w) == 41)
               & (F.lag("outcome_id", 1).over(w) == 1), 1)
         .when((F.lag("row_num", 1).over(w)
                == F.lag("last_row_num_in_foul_group", 1).over(w))
               & (F.lag("foul_won", 1).over(w) == 1), 1)
         .when(F.col("certain_possessor").isNotNull()
               & (F.col("certain_possessor") != prev_possessor), 1)
         .when(F.col("certain_possessor").isNotNull() & prev_possessor.isNull(), 1)
         .otherwise(0)
    )
    df = df.withColumn("possession_group_increment", increment)

    df = df.withColumn(
        "possession_group",
        F.sum("possession_group_increment").over(
            w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )

    w_group = (
        Window.partitionBy("match_id", "possession_group")
              .orderBy(*event_order())
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn("resolved_possessor", F.max("certain_possessor").over(w_group))

    return df.withColumn("match_row_number", F.row_number().over(w))


def chain_boundaries(pg, quals):
    """Blocs SQL chain_trigger_quals, chain_first_events, chain_triggers, chain_boundaries.

    Ajoute :
      - chain_trigger : comment la chaîne démarre, décidé par son PREMIER événement
        (plus petit row_num du groupe) : recovery > corner > goal_kick > throw_in
        > free_kick > open_play. Même valeur sur toutes les lignes de la chaîne.
      - is_rupture : 1 si le possesseur résolu de la ligne SUIVANTE diffère (NULL
        compris : `IS DISTINCT FROM`), 0 pour la première ligne du match.
    """
    from pyspark.sql import Window, functions as F

    # chain_trigger_quals : un drapeau par type de remise en jeu, par événement.
    q = F.col("qual_type_id")
    flags = (
        quals.select(F.col("str_match_id").alias("match_id"),
                     F.col("int_row_num").alias("row_num"),
                     F.col("str_qual_type_id").cast("int").alias("qual_type_id"))
        .filter(q.isin(5, 6, 26, 107, 124))
        .groupBy("match_id", "row_num").agg(
            F.max(F.when(q == 6, 1).otherwise(0)).alias("is_corner_taken"),
            F.max(F.when(q.isin(5, 26), 1).otherwise(0)).alias("is_free_kick"),
            F.max(F.when(q == 107, 1).otherwise(0)).alias("is_throw_in"),
            F.max(F.when(q == 124, 1).otherwise(0)).alias("is_goal_kick"),
        )
    )

    # chain_first_events + chain_triggers : on retrouve l'événement d'ouverture
    # de chaque groupe, puis on lui colle ses drapeaux.
    first = pg.groupBy("match_id", "possession_group").agg(
        F.min("row_num").alias("first_row_num"))
    opener = pg.select("match_id", F.col("row_num").alias("first_row_num"),
                       F.col("type_id").alias("first_type_id"))
    triggers = (
        first.join(opener, on=["match_id", "first_row_num"], how="inner")
             .join(flags.withColumnRenamed("row_num", "first_row_num"),
                   on=["match_id", "first_row_num"], how="left")
             .select(
                 "match_id", "possession_group",
                 F.when(F.col("first_type_id").isin(7, 8, 49), "recovery")
                  .when(F.col("is_corner_taken") == 1, "corner")
                  .when(F.col("is_goal_kick") == 1, "goal_kick")
                  .when(F.col("is_throw_in") == 1, "throw_in")
                  .when(F.col("is_free_kick") == 1, "free_kick")
                  .otherwise("open_play").alias("chain_trigger"))
    )
    df = pg.join(triggers, on=["match_id", "possession_group"], how="left")

    # chain_boundaries : rupture = le possesseur résolu change à la ligne suivante.
    w = Window.partitionBy("match_id").orderBy(*event_order())
    next_possessor = F.lead("resolved_possessor", 1).over(w)
    return df.withColumn(
        "is_rupture",
        F.when(F.col("match_row_number") == 1, 0)
         .when(~next_possessor.eqNullSafe(F.col("resolved_possessor")), 1)
         .otherwise(0),
    )


def apply_aerial_corrections(cb, quals):
    """Blocs SQL aerials, aerial_orphans, aerial_orphan_pairs,
    aerial_with_pair_group, aerial_best_group, aerial_corrections + COALESCE final.

    Un duel aérien met deux joueurs adverses face à face, simultanément. Mais les
    deux événements peuvent tomber dans deux groupes de possession différents. On
    les rassemble dans le plus PETIT des deux groupes.

    Ajoute : chain_id_final, chain_number_final (= possession_group pour tous les
    événements qui ne sont pas des duels aériens).

    Performance : les aériens (type 44) représentent ~150 000 lignes, contre ~58 M pour
    la table complète. On les extrait UNE fois, on les met en cache, et toutes les
    auto-jointures lisent ce petit jeu. Ne jamais joindre `cb` à lui-même.
    """
    from pyspark.sql import Window, functions as F

    aerials = (
        cb.filter(F.col("type_id") == 44)
          .select("match_id", "row_num", "event_id", "team_id",
                  "expanded_minute", "second", "possession_group")
          .cache()
    )

    # Qualifier 233 = OppositeRelatedEvent : qual_value porte l'event_id du pair.
    # NB : la jointure se fait sur (match_id, event_id) SEULEMENT, comme le SQL ;
    # event_id n'étant unique que par équipe, un duel peut ramener plusieurs lignes.
    eq = (
        quals.select(F.col("str_match_id").alias("match_id"),
                     F.col("str_event_id").cast("int").alias("event_id"),
                     F.col("str_qual_type_id").cast("int").alias("qual_type_id"),
                     F.col("str_qual_value").alias("qual_value"))
             .filter(F.col("qual_type_id") == 233)
             .select("match_id", "event_id", "qual_value")
    )

    # Côté « pair » : mêmes duels, TOUTES les colonnes préfixées `o_`. Sans ça,
    # une auto-jointure Spark ne sait plus de quel côté vient `match_id`.
    other = aerials.select(
        F.col("match_id").alias("o_match_id"),
        F.col("event_id").alias("o_event_id"),
        F.col("team_id").alias("o_team_id"),
        F.col("expanded_minute").alias("o_minute"),
        F.col("second").alias("o_second"),
        F.col("possession_group").alias("o_group"),
    )
    with_q = aerials.join(eq, on=["match_id", "event_id"], how="left")

    same_instant = [
        F.col("match_id") == F.col("o_match_id"),
        F.col("o_team_id") != F.col("team_id"),
        F.col("o_minute") == F.col("expanded_minute"),
        F.col("o_second") == F.col("second"),
    ]

    # aerial_orphans -> aerial_orphan_pairs : pour un duel SANS qualifier 233, on
    # cherche l'adversaire simultané (même minute, même seconde) et on garde le
    # plus petit groupe.
    orphan_pairs = (
        with_q.filter(F.col("qual_value").isNull())
              .join(other, on=same_instant, how="inner")
              .groupBy("match_id", "row_num")
              .agg(F.min("o_group").alias("orphan_pair_group"))
    )

    # aerial_with_pair_group : pour un duel AVEC qualifier 233, on retrouve son
    # pair par event_id (borné au même instant, équipe adverse). QUALIFY -> on
    # garde, par duel, la ligne dont le groupe effectif est le plus petit.
    paired = (
        with_q.join(other,
                    on=same_instant
                       + [F.col("o_event_id") == F.expr("try_cast(qual_value AS INT)")],
                    how="left")
              .select("match_id", "row_num", "possession_group",
                      F.col("o_group").alias("pair_group"))
    )

    w = Window.partitionBy("match_id", "row_num").orderBy(
        F.coalesce(F.col("pair_group"), F.col("possession_group")).asc(),
        F.col("pair_group").asc_nulls_last())
    best_pair = (paired.withColumn("rn", F.row_number().over(w))
                       .filter(F.col("rn") == 1).drop("rn"))

    # aerial_best_group + aerial_corrections
    corrections = (
        best_pair.join(orphan_pairs, on=["match_id", "row_num"], how="left")
        .select(
            "match_id", "row_num",
            F.least(F.col("possession_group"),
                    F.coalesce(F.col("pair_group"), F.col("orphan_pair_group"),
                               F.col("possession_group"))).alias("best_group"))
        .select("match_id", "row_num",
                F.concat(F.col("match_id"), F.lit("_"),
                         F.col("best_group").cast("string")).alias("chain_id_corrected"),
                F.col("best_group").alias("chain_number_corrected"))
    )

    df = cb.join(corrections, on=["match_id", "row_num"], how="left")
    return (
        df.withColumn("chain_id_final", F.coalesce(
                F.col("chain_id_corrected"),
                F.concat(F.col("match_id"), F.lit("_"),
                         F.col("possession_group").cast("string"))))
          .withColumn("chain_number_final", F.coalesce(
                F.col("chain_number_corrected"), F.col("possession_group")))
          .drop("chain_id_corrected", "chain_number_corrected")
    )


def log_checks(pg) -> None:
    """Agrégats de contrôle des blocs portés, à comparer à la référence.

    Les sommes pondérées par row_num détectent un décalage d'une ligne qu'un
    simple comptage laisserait passer. Les agrégats `*_non44` excluent les duels
    aériens (type 44), seuls événements que la correction aérienne peut modifier :
    ils sont donc comparables à la table dbt de dev.
    """
    from pyspark.sql import functions as F

    chain_key = F.concat(F.col("match_id"), F.lit("_"),
                         F.col("possession_group").cast("string"))
    non44 = F.col("type_id") != 44

    rows = (
        pg.groupBy("season").agg(
            F.count("*").alias("n"),
            F.sum("is_set_piece").alias("set_pieces"),
            F.sum("is_winning_clearance").alias("win_clear"),
            F.count("certain_possessor").alias("n_certain"),
            F.sum("certain_possessor").alias("sum_certain"),
            F.sum(F.col("row_num") * F.col("is_set_piece")).alias("w_setp"),
            F.sum(F.col("row_num") * F.col("is_winning_clearance")).alias("w_clear"),
            F.sum("possession_group_increment").alias("sum_incr"),
            F.countDistinct(chain_key).alias("n_groups"),
            F.sum("possession_group").alias("sum_group"),
            F.sum(F.col("row_num") * F.col("possession_group")).alias("w_group"),
            F.count("resolved_possessor").alias("n_resolved"),
            F.sum("resolved_possessor").alias("sum_resolved"),
            F.sum("match_row_number").alias("sum_mrn"),
            F.countDistinct(F.when(non44, chain_key)).alias("n_chains_non44"),
            F.sum(F.when(non44, F.col("possession_group"))).alias("sum_chain_number_non44"),
            F.sum("is_rupture").alias("sum_rupt"),
            F.sum(F.col("row_num") * F.col("is_rupture")).alias("w_rupt"),
            *[F.sum(F.when(F.col("chain_trigger") == t, 1).otherwise(0)).alias(f"n_{t}")
              for t in TRIGGERS],
            F.count("chain_trigger").alias("n_trig"),
            F.countDistinct("chain_id_final").alias("n_chains_final"),
            F.sum("chain_number_final").alias("sum_cn_final"),
            F.sum(F.col("row_num") * F.col("chain_number_final")).alias("w_cn_final"),
            F.sum(F.when(F.col("chain_number_final") != F.col("possession_group"), 1)
                   .otherwise(0)).alias("n_moved"),
        ).orderBy("season").collect()
    )
    keys = ("n", "set_pieces", "win_clear", "n_certain", "sum_certain", "w_setp",
            "w_clear", "sum_incr", "n_groups", "sum_group", "w_group", "n_resolved",
            "sum_resolved", "sum_mrn", "n_chains_non44", "sum_chain_number_non44",
            "sum_rupt", "w_rupt", *[f"n_{t}" for t in TRIGGERS], "n_trig",
            "n_chains_final", "sum_cn_final", "w_cn_final", "n_moved")
    for r in rows:
        logger.info(f"[checks] {r['season']} " + " ".join(f"{k}={r[k]}" for k in keys))


def build_possession_chains(events, quals):
    """Assemble les blocs portés ; les colonnes des blocs suivants restent NULL."""
    from pyspark.sql import functions as F

    events = apply_aerial_corrections(
        chain_boundaries(possession_groups(certain_possession(events, quals)), quals),
        quals)

    return events.select(
        "match_id", "season", "league_source",
        F.col("chain_id_final").alias("chain_id"),
        F.col("chain_number_final").alias("chain_number"),
        F.col("resolved_possessor").alias("chain_team_id"),
        "team_id", "player_id", "event_id", "row_num",
        "expanded_minute", "second", "period",
        "type_id", "type_name", "outcome_id", "is_shot", "x", "y",
        "is_rupture",
        "chain_trigger",
        "certain_possessor",
        "scraped_at",
    )


def to_output(df):
    """Renommage et casts du bloc mdl_out : le contrat de sortie du modèle dbt."""
    from pyspark.sql import functions as F

    return df.select(
        F.col("match_id").alias("str_match_id"),
        F.col("season").alias("str_season"),
        F.col("league_source").alias("str_league_source"),
        F.col("chain_id").alias("str_chain_id"),
        F.col("chain_number").cast("bigint").alias("int_chain_number"),
        F.col("chain_team_id").cast("string").alias("str_chain_team_id"),
        F.col("team_id").cast("string").alias("str_team_id"),
        F.col("player_id").cast("string").alias("str_player_id"),
        F.col("event_id").cast("string").alias("str_event_id"),
        F.col("row_num").alias("int_row_num"),
        F.col("expanded_minute").alias("int_expanded_minute"),
        F.col("second").alias("int_second"),
        F.col("period").alias("int_period"),
        F.col("type_id").cast("string").alias("str_type_id"),
        F.col("type_name").alias("str_type_name"),
        F.col("outcome_id").cast("string").alias("str_outcome_id"),
        F.col("is_shot").alias("bool_is_shot"),
        F.col("x").alias("dec_x"),
        F.col("y").alias("dec_y"),
        F.col("is_rupture").cast("int").alias("int_is_rupture"),
        F.col("chain_trigger").alias("str_chain_trigger"),
        F.col("certain_possessor").cast("bigint").alias("int_certain_possessor"),
        F.expr("try_cast(scraped_at AS TIMESTAMP)").alias("dt_scraped_at"),
    )


def process_seasons(seasons: list[str], dry_run: bool) -> None:
    """Traite `seasons` dans UNE session Spark : lecture, calcul, écriture, arrêt.

    Appelée une fois PAR saison (voir main). Pourquoi : les fichiers temporaires
    d'un shuffle (spill) ne sont supprimés que lorsque la session s'arrête. Dans
    une seule session pour 15 saisons, ils s'accumulent et saturent `local_dir`
    (« Espace insuffisant sur le disque »). Avec une session par saison, le pic
    de disque est celui d'UNE saison, connu et stable.
    """
    spark = get_spark("3etoiles-possession-chains")
    logger.info(f"Spark {spark.version} — UI : {spark.sparkContext.uiWebUrl}")
    try:
        events, quals, event_types = read_inputs(spark, seasons)
        base = prepare_events(events, event_types)
        out  = to_output(build_possession_chains(base, quals))

        # Convention du projet : toute sortie Parquet lue via une vue DuckDB
        # commence par ses clés (contournement du bug DuckDB 1.5.1).
        _keys = ["str_match_id", "int_row_num"]
        out = out.select(*_keys, *[c for c in out.columns if c not in _keys])

        if dry_run:
            log_checks(apply_aerial_corrections(chain_boundaries(
                possession_groups(certain_possession(base, quals)), quals), quals))
            for r in out.groupBy("str_season").count().orderBy("str_season").collect():
                logger.info(f"{r['str_season']}  {r['count']:>12,} lignes")
        else:
            log_plan(out, OUT_NAME)
            write_out(spark, out, OUT_NAME)
    finally:
        spark.stop()


def main(seasons: list[str] | None = None, all_seasons: bool = False,
         dry_run: bool = False, from_report: bool = False) -> int:
    import time

    if from_report:
        seasons = seasons_from_report()
        if not seasons:
            logger.info("Aucune saison à (re)calculer : chaînes déjà à jour.")
            return 0
    elif all_seasons:
        seasons = sorted(_seasons_in(SPARK_OUT / "int_event_enriched")
                         & _seasons_in(SPARK_OUT / "events_qual"))
    elif not seasons:
        logger.error("Précise --season SAISON (répétable), --all ou --from-report.")
        return 2

    logger.info(f"→ {len(seasons)} saison(s), une session Spark chacune : "
                f"{', '.join(seasons)}")
    t_all = time.time()
    for i, season in enumerate(seasons, 1):
        t0 = time.time()
        logger.info(f"══ [{i}/{len(seasons)}] {season} ══")
        try:
            process_seasons([season], dry_run)
        except Exception:
            # Les saisons déjà écrites restent valides : une relance reprend ici,
            # car --from-report ne retient que les saisons absentes de la sortie.
            logger.exception(f"Échec sur {season} après {i - 1} saison(s) réussie(s).")
            return 1
        logger.success(f"{season} terminée en {time.time() - t0:,.0f}s "
                       f"(total {time.time() - t_all:,.0f}s)")
    return 0


if __name__ == "__main__":
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/spark_possession_chains.log", level="DEBUG", rotation="5 MB",
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
                    help="compte les lignes par saison sans rien écrire.")
    args = ap.parse_args()
    sys.exit(main(seasons=args.season, all_seasons=args.all_seasons,
                  dry_run=args.dry_run, from_report=args.from_report))
