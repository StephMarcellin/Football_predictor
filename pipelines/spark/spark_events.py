#!/usr/bin/env python3
"""
spark_events.py — Chaîne événementielle WhoScored dans Spark
=============================================================
Remplace trois modèles dbt par un job Spark unique :
    intermediate.intermediate_whoscored_events (jointure d'identité + remapping team_id)
    intermediate.intermediate_whoscored_event_qualifiers   (explosion du JSON des qualifiers)
    intermediate.intermediate_whoscored_events_enriched   (score courant + pivot des 10 flags)

Entrées  (produites par export_to_parquet.py) :
    data/spark_in/events/season=*/        stg_whoscored_events, brut
    data/spark_in/match_index/*.parquet   int_whoscored_match_index (noms refondus : str_, dt_…)

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

# Taux de team_id NULL au-dela duquel le job ECHOUE au lieu d'avertir.
#
# Pourquoi 0,5 % et pas davantage : l'etat sain est EXACTEMENT 0 — sur
# 2023-2024, 4 048 666 evenements sur 4 048 666 sont traduits. Un team_id NULL
# apres le CASE signifie que l'id WhoScored de l'evenement ne correspondait ni
# au domicile ni a l'exterieur de son match : une erreur dure, pas du bruit.
# Un seul match casse pese ~0,04 % d'une saison, donc 0,5 % laisse passer une
# douzaine de matchs aberrants sans bloquer le flux quotidien, tout en arretant
# net une panne systemique. A 2 % on laisserait filer ~50 matchs sans rien voir.
#
# Le 04/10 et le 12/10/2026, ce taux etait a 99,6 % puis 100 % et le pipeline a
# continue : le compteur existait, il ne faisait qu'avertir.
TEAM_NULL_FAIL_RATE = 0.005

# Taux d'evenements ECARTES par l'INNER JOIN sur l'index au-dela duquel le job
# echoue. L'INNER JOIN est delibere (un match absent du registre est ecarte),
# mais un taux eleve signifie que l'index ne resout plus les matchs — et un
# taux de 100 % produit une partition VIDE ecrite sans erreur.
# Un match non resolu pese ~0,04 % d'une saison ; 1 % en laisse passer ~25.
JOIN_ORPHAN_FAIL_RATE = 0.01

# Rapport du dernier run, consomme par l'orchestrateur (cf. main()).
RUN_REPORT = OUT_DIR / "_run_report.json"


# ══════════════════════════════════════════════════════════════════════════════
# NOMMAGE — convention du projet : préfixe de type en tête de nom
# (str_, int_, dec_, dt_, bool_). Les événements bruts viennent de la couche silver
# (stg_whoscored_events), hors périmètre du renommage : la logique Spark travaille
# donc avec les noms silver ; les colonnes prises à l'index (déjà refondu) sont
# lues sous leurs noms refondus. Le renommage (+ cast) des SORTIES a lieu à
# l'écriture, d'après docs/proposition_nommage_v2.csv — la même table que dbt.
# ══════════════════════════════════════════════════════════════════════════════
import csv

_NAMING_CSV = ROOT_DIR / "docs" / "proposition_nommage_v2.csv"
_SPARK_TYPES = {"VARCHAR": "string", "INTEGER": "int", "BIGINT": "bigint",
                "DOUBLE": "double", "BOOLEAN": "boolean", "DATE": "date",
                "TIMESTAMP": "timestamp"}
_PREFIX_BY_DTYPE = {"string": "str", "int": "int", "bigint": "int", "smallint": "int",
                    "double": "dec", "float": "dec", "boolean": "bool",
                    "date": "dt", "timestamp": "dt"}


def _naming(table: str) -> dict[str, tuple[str, str, str]]:
    """{ancien_nom: (nouveau_nom, type_final, type_actuel)} pour une table."""
    with open(_NAMING_CSV, encoding="utf-8") as f:
        return {r["nom_variable_actuel"]: (r["new_nom_variable"], r["type_final"], r["type_actuel"])
                for r in csv.DictReader(f, delimiter=";") if r["nom_table"] == table}


def to_final_names(df, table: str):
    """Renomme (et caste) les colonnes d'une sortie vers la convention du projet.
    Une colonne absente du CSV (ex. la partition season d'events_qual) reçoit le
    préfixe déduit de son type Spark, avec un avertissement."""
    from pyspark.sql import functions as F
    m = _naming(table)
    out = []
    for c, dtype in df.dtypes:
        if c in m:
            new, typ, _ = m[c]
            out.append(F.col(c).cast(_SPARK_TYPES.get(typ, typ.lower())).alias(new))
        else:
            pref = _PREFIX_BY_DTYPE.get(dtype.split("(")[0], "str")
            logger.warning(f"{table}.{c} absent de {_NAMING_CSV.name} → {pref}_{c}")
            out.append(F.col(c).alias(f"{pref}_{c}"))
    return df.select(*out)


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

def available_seasons() -> list[str]:
    """Saisons présentes dans l'ENTRÉE Spark, lues depuis les noms de dossiers."""
    d = IN_DIR / "events"
    return sorted(p.name.split("=", 1)[1] for p in d.glob("season=*") if p.is_dir())


def output_seasons() -> list[str]:
    """Saisons déjà présentes dans la SORTIE Spark, d'après les noms de dossiers."""
    d = OUT_DIR / "int_whoscored_events"
    if not d.exists():
        return []
    return sorted(p.name.split("=", 1)[1] for p in d.glob("str_season=*") if p.is_dir())


def write_run_report(seasons: list[str], reprocessed: list[str]) -> None:
    """
    Écrit le rapport du run, consommé par l'étape dbt de l'orchestrateur.

    POURQUOI UN FICHIER, alors que seasons_needing_work() refuse justement de
    s'appuyer sur un état stocké : ce fichier n'est pas une source de vérité
    sur ce qui RESTE à faire — il décrit ce qui VIENT d'être fait, et il est
    consommé immédiatement par l'étape suivante du même run. Il est réécrit à
    chaque exécution, y compris quand il n'y a rien à traiter (listes vides),
    pour qu'un rapport périmé ne puisse pas déclencher un full-refresh fantôme.

    `reprocessed` = saisons qui étaient DÉJÀ en sortie avant ce run. Pour
    celles-là, des matchs déjà présents dans les tables dbt ont pu changer de
    contenu. Or le filtre incrémental des modèles intermediate est un anti-join
    sur match_id (`WHERE t.match_id IS NULL`) : un match déjà présent est écarté
    AVANT toute fusion, même sur un modèle à unique_key. Une correction ne se
    propage donc jamais en incrémental — d'où le --full-refresh.
    Une saison entièrement NEUVE n'a pas ce problème : tous ses matchs sont
    absents des tables, l'incrémental les prend normalement.
    """
    import json
    from datetime import datetime, timezone

    RUN_REPORT.parent.mkdir(parents=True, exist_ok=True)
    RUN_REPORT.write_text(
        json.dumps(
            {
                "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "seasons": seasons,
                "reprocessed": reprocessed,
                "needs_intermediate_full_refresh": bool(reprocessed),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    if reprocessed:
        logger.warning(
            f"Saison(s) RECALCULÉE(S) : {', '.join(reprocessed)} → "
            f"l'étape dbt intermediate basculera en --full-refresh "
            f"(l'incrémental ne corrige pas un match déjà présent)."
        )


def seasons_needing_work() -> list[str]:
    """
    Déduit de la DONNÉE quelles saisons sont à (re)calculer, sans aucun état
    stocké : une saison est à traiter quand son contenu en ENTRÉE diffère de
    son contenu en SORTIE.

    Pourquoi pas un fichier d'état ou un `--season` passé à la main :
      · un manifeste est un artefact de plus, qui peut dériver du réel — et
        toute la panne du 12/10 est précisément une dérive entre ce que le code
        croyait traité et ce que la donnée contenait ;
      · `--season` met la connaissance dans la tête de l'opérateur : une erreur
        saute une saison ou, en mode `static`, efface les autres.
    La donnée se décrit elle-même, donc la règle ne peut pas dériver.

    Une seule règle couvre les trois cas :
      · saison terminée, déjà traitée   → mêmes compteurs  → sautée
      · saison jamais traitée, ou dont
        la partition a été effacée      → sortie absente   → traitée
      · saison en cours, n matchs de plus → compteurs différents → saison
        ENTIÈRE recalculée (choix assumé : moins efficient qu'un delta, mais
        sans état à maintenir, et le coût d'une saison est acceptable)

    Comparaison sur (nombre de matchs, nombre de lignes) : le nombre de matchs
    attrape les ajouts, le nombre de lignes attrape aussi une correction
    d'événements sur un match déjà présent. Le calcul tourne en DuckDB sur les
    Parquet, AVANT de démarrer Spark : si rien n'est à faire, Spark ne démarre
    pas du tout.
    """
    import duckdb

    in_glob  = (IN_DIR / "events" / "season=*" / "**" / "*.parquet").as_posix()
    out_glob = (OUT_DIR / "int_whoscored_events" / "str_season=*" / "**" / "*.parquet").as_posix()

    con = duckdb.connect()
    # L'entrée identifie le match par ws_match_id (id WhoScored natif) ; la
    # sortie par str_match_id (le SHA1 forgé par resolve_identity). Les deux ids
    # ne sont pas comparables, mais leurs COMPTES le sont : ils dénombrent les
    # mêmes matchs.
    src = {
        r[0]: (r[1], r[2])
        for r in con.sql(
            f"""SELECT season, COUNT(DISTINCT ws_match_id), COUNT(*)
                FROM read_parquet('{in_glob}', hive_partitioning = 1)
                GROUP BY 1"""
        ).fetchall()
    }
    try:
        dst = {
            r[0]: (r[1], r[2])
            for r in con.sql(
                f"""SELECT str_season, COUNT(DISTINCT str_match_id), COUNT(*)
                    FROM read_parquet('{out_glob}', hive_partitioning = 1)
                    GROUP BY 1"""
            ).fetchall()
        }
    except Exception:
        # Sortie absente (premier run, ou répertoire effacé) : tout est à faire.
        dst = {}
    con.close()

    todo = []
    for season in sorted(src):
        n_in = src[season]
        n_out = dst.get(season)
        if n_out is None:
            logger.info(f"  {season} : absente en sortie           → À TRAITER")
            todo.append(season)
        elif n_in != n_out:
            logger.info(
                f"  {season} : entrée {n_in[0]:,} matchs / {n_in[1]:,} lignes "
                f"vs sortie {n_out[0]:,} / {n_out[1]:,} → À TRAITER"
            )
            todo.append(season)
        else:
            logger.info(f"  {season} : à jour ({n_in[0]:,} matchs)      → sautée")

    for season in sorted(set(dst) - set(src)):
        logger.warning(
            f"  {season} : présente en SORTIE mais absente en entrée. "
            f"Partition orpheline, laissée en place (ni lue, ni supprimée)."
        )

    return todo


def read_inputs(spark, seasons: list[str] | None):
    """
    Lit les deux jeux Parquet d'entrée.

    Args:
        seasons : si fourni, ne lit que ces saisons (partition pruning —
                  Spark élimine les dossiers season=… sans ouvrir un fichier).
                  None = toutes.

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
    if seasons:
        events = events.filter(F.col("season").isin(seasons))

    index = spark.read.parquet(str(index_path))

    return events, index


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Résolution d'identité  (= modèle int_whoscored_events)
# ══════════════════════════════════════════════════════════════════════════════

def resolve_identity(events, index):
    """
    Attache le match_id unifié et le contexte match aux événements.

    DEUX traductions, pas une :
      1. ws_match_id → match_id (jointure sur l'index) ;
      2. team_id WhoScored → team_id canonique du projet (le CASE plus bas).

    silver.stg_whoscored_events porte des ids d'équipe NATIFS WhoScored, pas
    les ids canoniques. Vérifié le 05/10/2026 : sur le match 1732025, les
    événements portent {21, 192} alors que l'index donne {1028, 641} —
    1028/641 étant les ids de referentiel.team_mapping. Le CASE de traduction
    est donc INDISPENSABLE, et son otherwise(None) est le seul garde-fou qui
    signale une équipe non appariée.

    ⚠️ Ce CASE dépend entièrement de str_ws_home_team_id / str_ws_away_team_id
    de l'index. Si ces colonnes cessent de porter les ids WhoScored, la
    traduction échoue SILENCIEUSEMENT et team_id sort NULL sur toute la table.
    C'est arrivé : normalize_team_id_col n'était pas idempotente et écrasait
    ws_*_team_id par la valeur canonique au second passage (99,6 % de NULL le
    04/10/2026, propagés jusqu'à possession_chains et aux profils d'événements
    sans qu'aucun test ne le voie). report_join_quality() surveille ce taux :
    le lire à chaque run.

    La jointure est diffusée (broadcast) : l'index fait quelques milliers de
    lignes, Spark en envoie une copie à chaque tâche qui joint en local.
    Aucun shuffle.

    INNER JOIN délibéré (décision B) : un événement dont le match n'est pas
    résolu au registre est écarté — c'est déjà ce que fait int_event_enriched
    via son INNER JOIN match_dates. Le volume écarté est journalisé.

    ws_home / ws_away sont les CIBLES de la traduction d'équipe, et canon_home /
    canon_away ses valeurs de sortie. Les quatre sont retirées avant écriture :
    seul le team_id canonique survit dans le Parquet.
    """
    from pyspark.sql import functions as F

    # L'index est lu sous ses noms refondus (str_/dt_) ; chaque colonne est
    # rattachée au vocabulaire silver des événements pour la jointure. Les ids
    # d'équipe sont des VARCHAR côté index, des INTEGER côté silver : cast
    # explicite pour comparer des entiers à des entiers (et garder le MIN/MAX
    # numérique de team_1/team_2 dans build_event_enriched). La sortie est
    # recastée en VARCHAR (str_team_id) par to_final_names.
    idx = index.select(
        F.col("str_ws_match_id").alias("ws_match_id"),
        F.col("str_match_id").alias("match_id"),
        # Ids WhoScored des deux équipes : ce sont eux que le CASE compare au
        # team_id des événements. Sans eux, aucune traduction n'est possible.
        F.col("str_ws_home_team_id").cast("int").alias("ws_home"),
        F.col("str_ws_away_team_id").cast("int").alias("ws_away"),
        F.col("str_team_id").cast("bigint").alias("canon_home"),
        F.col("str_opponent_id").cast("bigint").alias("canon_away"),
        # Préfixées idx_ : int_event_enriched prend son contexte match de
        # l'INDEX, pas des événements (fidélité au CTE match_dates du SQL dbt).
        F.col("dt_match_date").alias("idx_match_date"),
        F.col("str_season").alias("idx_season"),
        F.col("str_league_source").alias("idx_league_source"),
        F.col("dt_scraped_at").alias("idx_scraped_at"),
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

    # ── Rien n'a survécu à la jointure ──────────────────────────────────────
    # Cas limite qu'il faut traiter AVANT tout calcul de taux : avec n_out = 0,
    # un taux de team_id NULL vaut 0/0, donc 0, et le contrôle d'identité
    # conclut « 0 / 0 traduites » en SUCCESS. Une partition vide s'écrit alors
    # sans la moindre erreur. C'est arrivé le 14/10/2026 sur 2022-2023 :
    # 3 822 202 événements en entrée, 0 en sortie, job en succès.
    if n_in and n_out == 0:
        raise RuntimeError(
            f"La jointure sur l'index a écarté la TOTALITÉ des "
            f"{n_in:,} événements : rien à écrire.\n"
            f"Cause probable : str_match_id NULL dans l'index pour ces saisons "
            f"(l'index filtre isnotnull(str_match_id), donc l'INNER JOIN ne "
            f"trouve aucune correspondance).\n"
            f"Contrôle : SELECT str_season, COUNT(*), "
            f"SUM(CASE WHEN str_match_id IS NULL THEN 1 ELSE 0 END) "
            f"FROM intermediate.intermediate_whoscored_match_bridge GROUP BY 1;"
        )

    if n_orph:
        taux_orph = n_orph / n_in
        if taux_orph > JOIN_ORPHAN_FAIL_RATE:
            raise RuntimeError(
                f"Écartés par la jointure : {n_orph:,} événements "
                f"({taux_orph:.2%}) — seuil d'échec "
                f"{JOIN_ORPHAN_FAIL_RATE:.1%}.\n"
                f"L'index ne résout plus les matchs de ces saisons."
            )
        logger.warning(
            f"Écartés (match non résolu)  : {n_orph:,} ({taux_orph:.2%}) — "
            f"sous le seuil de {JOIN_ORPHAN_FAIL_RATE:.1%}, on continue."
        )

    # ── Contrôle d'espace d'identifiants ──────────────────────────────────
    # Après le CASE, un team_id NULL signifie que l'id WhoScored de l'événement
    # ne correspondait NI à ws_home_team_id NI à ws_away_team_id de son match.
    # Un seul comptage : chaque .count() est une passe complète sur le cache.
    n_team_null = resolved.filter(F.col("team_id").isNull()).count()
    taux = n_team_null / n_out if n_out else 0.0

    if n_team_null == 0:
        logger.success(f"Identités d'équipe          : {n_out:,} / {n_out:,} traduites.")
    elif taux <= TEAM_NULL_FAIL_RATE:
        # Sous le seuil : on avertit en nommant les matchs touchés, pour que ce
        # soit actionnable sans avoir à requêter la base.
        n_matchs = (resolved.filter(F.col("team_id").isNull())
                            .select("match_id").distinct().count())
        logger.warning(
            f"team_id non traduit         : {n_team_null:,} ({taux:.3%}) "
            f"sur {n_matchs:,} match(s) — sous le seuil de "
            f"{TEAM_NULL_FAIL_RATE:.1%}, on continue."
        )
    else:
        # Au-delà du seuil : ÉCHEC. Laisser passer, c'est propager des actions
        # attribuées aux mauvaises équipes dans toute la couche aval, sans
        # qu'aucun test ne le voie (c'est ce qui s'est produit les 04 et
        # 12/10/2026). La cause est presque toujours en amont : les colonnes
        # str_ws_home_team_id / str_ws_away_team_id de l'index ne portent plus
        # les ids WhoScored natifs.
        raise RuntimeError(
            f"team_id non traduit sur {n_team_null:,} événements "
            f"({taux:.2%}) — seuil d'échec {TEAM_NULL_FAIL_RATE:.1%}.\n"
            f"Cause probable : silver.stg_whoscored_match_index.ws_home_team_id "
            f"/ ws_away_team_id ne portent plus les ids WhoScored natifs.\n"
            f"Contrôle : SELECT season, COUNT(*), "
            f"SUM(CASE WHEN ws_home_team_id IS NULL THEN 1 ELSE 0 END) "
            f"FROM silver.stg_whoscored_match_index GROUP BY 1;"
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

    Entrée : la sortie events_qual TELLE QU'ÉCRITE (noms refondus :
    str_match_id, int_row_num, str_qual_type_id VARCHAR). Les clés sont rendues
    sous match_id / row_num pour la jointure avec les événements (vocabulaire
    silver) dans build_event_enriched.

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
        F.max(F.when(F.col("str_qual_type_id") == str(tid), 1).otherwise(0)).alias(name)
        for tid, name in QUAL_FLAGS.items()
    ]
    return (
        events_qual
        .filter(F.col("str_qual_type_id").isin([str(t) for t in QUAL_FLAGS]))
        .groupBy(F.col("str_match_id").alias("match_id"),
                 F.col("int_row_num").alias("row_num"))
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
        # minute AJOUTEE le 07/10/2026. expanded_minute n'est pas la minute
        # de jeu : c'est la minute continue de WhoScored, qui absorbe le temps
        # additionnel de la 1re mi-temps. Ecart mesure en 2e MT : mediane 4,
        # maximum 28, sur 2 599 matchs. Sans cette colonne, tout seuil absolu
        # en aval (ex. "apres la 75e") derive d'un montant variable par match.
        "minute", "expanded_minute", "second", "period",
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
        .partitionBy("str_season")          # partition sous le nom refondu
        .parquet(str(path))
    )
    n = spark.read.parquet(str(path)).count()
    logger.success(
        f"{name:<22} {n:>12,} lignes   {time.time() - t0:>6,.0f}s   → {path}"
    )
    return n


# ══════════════════════════════════════════════════════════════════════════════

def main(seasons: list[str] | None = None, all_seasons: bool = False) -> int:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Quelles saisons ? ───────────────────────────────────────────────────
    # Priorité : --all  >  --season explicite  >  détection automatique.
    if all_seasons:
        seasons = available_seasons()
        logger.info(f"--all : {len(seasons)} saison(s) → {', '.join(seasons)}")
    elif seasons:
        logger.info(f"--season : {', '.join(seasons)}")
    else:
        logger.info("Détection automatique des saisons à traiter :")
        seasons = seasons_needing_work()
        if not seasons:
            logger.success("Rien à traiter : toutes les saisons sont à jour.")
            # Rapport écrit même à vide : sinon un rapport périmé d'un run
            # antérieur ferait basculer dbt en full-refresh sans raison.
            write_run_report([], [])
            return 0
        logger.info(f"→ {len(seasons)} saison(s) à traiter : {', '.join(seasons)}")

    # Saisons de ce run qui existaient DÉJÀ en sortie : ce sont celles dont des
    # matchs peuvent avoir changé de contenu, pas seulement s'être ajoutés.
    deja_presentes = sorted(set(seasons) & set(output_seasons()))
    write_run_report(seasons, deja_presentes)

    spark = get_spark("3etoiles-events")
    logger.info(f"Spark {spark.version} — UI : {spark.sparkContext.uiWebUrl}")

    try:
        events, index = read_inputs(spark, seasons)

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
        out1 = to_final_names(out1, "int_whoscored_events")
        _keys = ["str_match_id", "str_team_id", "int_row_num", "str_event_id"]
        out1 = out1.select(*_keys, *[c for c in out1.columns if c not in _keys])
        log_plan(out1, "int_whoscored_events")
        write_out(spark, out1, "int_whoscored_events")

        # ── Sortie 2 : events_qual ───────────────────────────────────────────
        events_qual = to_final_names(build_events_qual(resolved), "events_qual")
        log_plan(events_qual, "events_qual")
        write_out(spark, events_qual, "events_qual")

        # ── Sortie 3 : int_event_enriched ────────────────────────────────────
        # events_qual est relu depuis le DISQUE, donc il contient TOUTES les
        # saisons déjà écrites. On le restreint aux saisons du run : sans ce
        # filtre on relirait inutilement les saisons terminées (le join ne les
        # apparierait pas, mais on paierait la lecture).
        qual_src = spark.read.parquet(str(OUT_DIR / "events_qual"))
        if seasons:
            qual_src = qual_src.filter(F.col("str_season").isin(seasons))
        qual_flags = build_qual_flags(qual_src)
        enriched = to_final_names(build_event_enriched(resolved, qual_flags),
                                  "int_event_enriched")
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
    ap.add_argument("--season", action="append", default=None, metavar="SAISON",
                    help="traite cette saison (répétable : --season 2022-2023 "
                         "--season 2023-2024). Sans ce drapeau, les saisons à "
                         "traiter sont déduites de la donnée.")
    ap.add_argument("--all", action="store_true", dest="all_seasons",
                    help="force le recalcul de TOUTES les saisons présentes en entrée.")
    args = ap.parse_args()

    sys.exit(main(seasons=args.season, all_seasons=args.all_seasons))