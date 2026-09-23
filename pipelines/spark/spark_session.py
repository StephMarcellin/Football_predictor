"""
spark_session.py — Fabrique de SparkSession pour le Projet 3-Étoiles
=====================================================================
Point d'entrée unique de tous les jobs Spark du projet.

Responsabilité unique : poser l'environnement Windows/JVM AVANT l'import
de pyspark, puis rendre une SparkSession configurée pour la machine.

⚠️ À exécuter avec .venv-spark (Python 3.11), jamais avec .venv (3.13).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import yaml

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    _CFG = yaml.safe_load(f)

SPARK_CFG = _CFG.get("spark", {})

def _bootstrap_env() -> None:
    """
    Pose les variables d'environnement lues par spark-submit au lancement de
    la JVM. DOIT être appelée AVANT tout `import pyspark` — après, la JVM est
    déjà lancée et ces valeurs n'ont plus aucun effet.
    """
    # ── Garde-fou : pyspark 3.5.x ne supporte pas Python 3.13 ─────────────
    # Le support de Python 3.13 est arrivé en Spark 4.0.0 (SPARK-49870).
    # On refuse de démarrer plutôt que de laisser passer une incompatibilité
    # qui se manifesterait par des erreurs opaques en pleine exécution.
    import pyspark
    major = int(pyspark.__version__.split(".")[0])
    if major < 4 and sys.version_info >= (3, 13):
        raise RuntimeError(
            f"pyspark {pyspark.__version__} avec Python "
            f"{sys.version_info.major}.{sys.version_info.minor} : hors matrice. "
            f"Python 3.13 exige pyspark >= 4.0. Fais `pip install --upgrade \"pyspark>=4.2\"`."
        )

    # ── JAVA_HOME : quelle JVM spark-submit va lancer ─────────────────────
    java_home = SPARK_CFG.get("java_home")
    if not java_home:
        raise RuntimeError("config.yaml : section spark.java_home manquante.")
    java_exe = Path(java_home) / "bin" / "java.exe"
    if not java_exe.exists():
        raise FileNotFoundError(f"JDK introuvable : {java_exe}")

    os.environ["JAVA_HOME"] = str(Path(java_home))
    os.environ["PATH"] = str(Path(java_home) / "bin") + os.pathsep + os.environ["PATH"]

    # ── HADOOP_HOME : OPTIONNEL ────────────────────────────────────────────
    # Spark passe par les API fichier Hadoop même en local. Sous Windows elles
    # peuvent réclamer winutils.exe / hadoop.dll. On ne pose la variable que si
    # le binaire est réellement là : ça permet de tester SANS, et de n'installer
    # winutils qu'en cas d'échec avéré.
    hadoop_home = SPARK_CFG.get("hadoop_home")
    if hadoop_home and (Path(hadoop_home) / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(Path(hadoop_home))
        os.environ["PATH"] = str(Path(hadoop_home) / "bin") + os.pathsep + os.environ["PATH"]

    # ── Interpréteur des workers Python ───────────────────────────────────
    # sys.executable = le python de .venv-spark, puisque c'est lui qui nous
    # exécute. Sans ça, Spark chercherait `python` dans le PATH système —
    # Anaconda est installé sur cette machine, la collision est garantie.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # ── Mémoire du heap JVM ───────────────────────────────────────────────
    # En mode local le driver EST l'exécuteur : --driver-memory est LA mémoire
    # du job. Un .config("spark.driver.memory", ...) dans le builder serait lu
    # APRÈS le lancement de la JVM, donc ignoré.
    driver_memory = SPARK_CFG.get("driver_memory", "8g")
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {driver_memory} pyspark-shell"


def get_spark(app_name: str, shuffle_partitions: int | None = None):
    """
    Rend une SparkSession prête à l'emploi.

    Args:
        app_name           : nom affiché dans la Spark UI (localhost:4040)
        shuffle_partitions : nombre de partitions après un shuffle.
                             None -> valeur de config.yaml.
                             Cible : ~128 Mo de données par partition.
    """
    _bootstrap_env()

    from pyspark.sql import SparkSession   # import APRÈS _bootstrap_env()

    local_dir = Path(SPARK_CFG.get("local_dir", r"C:\spark-tmp"))
    local_dir.mkdir(parents=True, exist_ok=True)

    cores   = SPARK_CFG.get("cores", 6)
    n_parts = shuffle_partitions or SPARK_CFG.get("shuffle_partitions", 64)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{cores}]")

        # Disque de spill : explicite, hors OneDrive, hors dossier projet.
        .config("spark.local.dir", str(local_dir))

        # Partitions après shuffle. Trop peu -> partitions énormes qui spillent.
        # Trop -> surcoût d'ordonnancement sur des tâches minuscules.
        .config("spark.sql.shuffle.partitions", n_parts)

        # AQE : Spark ré-optimise le plan À L'EXÉCUTION à partir des statistiques
        # réelles des partitions — fusionne les trop petites, découpe celles en
        # skew. Filet de sécurité tant qu'on ne connaît pas les volumes.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        # zstd : meilleur ratio que snappy. Compte, avec un seul disque.
        .config("spark.sql.parquet.compression.codec", "zstd")

        # UTC partout : évite que match_date se décale selon le fuseau machine.
        .config("spark.sql.session.timeZone", "UTC")

        # Windows : la résolution du nom d'hôte échoue parfois et bloque le
        # démarrage du driver. On force la boucle locale.
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")

        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_CFG.get("log_level", "WARN"))
    return spark