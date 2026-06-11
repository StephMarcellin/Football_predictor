"""
enrich_team_mapping.py — Enrichissement de la seed team_mapping depuis Transfermarkt
======================================================================================

Lit deux fichiers CSV locaux (seeds/) :
  - transfermarkt_clubs.csv  : source de vérité des équipes D1/D2 Big5
  - team_mapping.csv         : seed de normalisation (alias, club_name)

Pour chaque club_name unique dans Transfermarkt :
  - club_name trouvé dans team_mapping.alias → déjà couvert, skip
  - club_name absent                         → ajout d'une ligne (club_name, 'NEW')

La sentinelle 'NEW' permet de retrouver facilement les clubs à mapper :
    Filtrer team_mapping.csv sur alias == 'NEW'

Le fichier team_mapping.csv est modifié en place (ou écrit dans --output).
Il sera ensuite injecté comme seed dbt normalement.

Usage :
    python enrich_team_mapping.py
    python enrich_team_mapping.py --dry-run   # affiche les ajouts sans modifier le CSV
    python enrich_team_mapping.py --output path/to/output.csv
"""

import argparse
from pathlib import Path

import polars as pl
from loguru import logger

from datetime import date


# ── Chemins par défaut ────────────────────────────────────────────────────────

ROOT_DIR  = Path(__file__).resolve().parent.parent
SEEDS_DIR = ROOT_DIR / "dbt_project" / "seeds"

DEFAULT_TM_CSV      = SEEDS_DIR / "transfermarkt_clubs.csv"
DEFAULT_MAPPING_CSV = SEEDS_DIR / "team_mapping.csv"

# ── Logs ──────────────────────────────────────────────────────────────────────

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/enrich_team_mapping.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="2 MB",
    retention=5,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)


# ── Fonctions ─────────────────────────────────────────────────────────────────

def load_transfermarkt_clubs(csv_path: Path) -> list[str]:
    """
    Lit le CSV Transfermarkt et retourne les club_name uniques triés.
    """
    logger.info(f"Lecture Transfermarkt : {csv_path}")
    df = pl.read_csv(csv_path, infer_schema_length=1000)

    if "club_name" not in df.columns:
        raise ValueError(
            f"Colonne 'club_name' absente. Colonnes trouvées : {df.columns}"
        )

    clubs = df["club_name"].drop_nulls().unique().sort().to_list()
    logger.info(f"  {len(clubs)} club_name uniques dans Transfermarkt")
    return clubs


def load_team_mapping(csv_path: Path) -> pl.DataFrame:
    """
    Lit le CSV team_mapping.
    Colonnes attendues : alias, club_name
    """
    logger.info(f"Lecture team_mapping : {csv_path}")
    df = pl.read_csv(csv_path, infer_schema_length=1000)

    if "alias" not in df.columns or "club_name" not in df.columns:
        raise ValueError(
            f"Colonnes 'alias' et 'club_name' requises. "
            f"Colonnes trouvées : {df.columns}"
        )

    logger.info(f"  {len(df)} entrées existantes dans team_mapping")
    return df


def compute_new_clubs(
    tm_clubs: list[str],
    existing_aliases: set[str],
) -> list[str]:
    """
    Retourne les club_name Transfermarkt absents de team_mapping.alias
    (comparaison exacte).
    """
    new_clubs = [c for c in tm_clubs if c not in existing_aliases]
    already   = len(tm_clubs) - len(new_clubs)
    logger.info(f"  {already} déjà couverts | {len(new_clubs)} nouveaux à ajouter")
    return new_clubs

def initialize_team_ids(mapping_df: pl.DataFrame, reset: bool = False) -> pl.DataFrame:
    today = date.today().strftime("%Y%m%d")

    if "team_id" not in mapping_df.columns:
        mapping_df = mapping_df.with_columns(
            pl.lit(None).cast(pl.Int64).alias("team_id")
        )

    if reset:
        mapping_df = mapping_df.with_columns(
            pl.lit(None).cast(pl.Int64).alias("team_id")
        )

    clubs_sans_id = (
        mapping_df
        .filter(pl.col("team_id").is_null())
        ["club_name"]
        .drop_nulls()
        .unique()
        .sort()
        .to_list()
    )

    generated = {club: int(f"{today}{str(i+1).zfill(4)}")
                 for i, club in enumerate(clubs_sans_id)}

    return mapping_df.with_columns(
        pl.struct(["club_name", "team_id"]).map_elements(
            lambda row: generated.get(row["club_name"]) if row["team_id"] is None else row["team_id"],
            return_dtype=pl.Int64,
        ).alias("team_id")
    )

def enrich_mapping(
    mapping_df: pl.DataFrame,
    new_clubs: list[str],
    dry_run: bool,
    output_path: Path,
) -> None:
    """
    Ajoute les nouveaux clubs dans le DataFrame team_mapping
    avec club_name = 'NEW', puis écrit le CSV enrichi.
    """
    if not new_clubs:
        logger.success("Aucun nouveau club — team_mapping déjà à jour ✅")
        if not dry_run:
            mapping_df.write_csv(output_path)
            logger.success(f"  CSV écrit → {output_path} ✅")
        return

    if dry_run:
        logger.info(f"[DRY-RUN] {len(new_clubs)} lignes qui seraient ajoutées :")
        for club in new_clubs:
            logger.info(f"  club_name='NEW'  alias={club!r}")
        logger.info(f"[DRY-RUN] Fichier cible : {output_path} (non modifié)")
        return

    # Génération des team_id pour les nouveaux clubs
    today = date.today().strftime("%Y%m%d")
    if "team_id" not in mapping_df.columns:
        mapping_df = mapping_df.with_columns(
            pl.lit(None).cast(pl.Int64).alias("team_id")
        )
    new_rows = pl.DataFrame({
        "alias":     new_clubs,
        "club_name": ["NEW"] * len(new_clubs),
        "team_id":   [int(f"{today}{str(i+1).zfill(4)}") 
                      for i in range(len(new_clubs))],
    }).with_columns(pl.col("team_id").cast(pl.Int64))

    enriched = pl.concat([mapping_df, new_rows], how="vertical")

    enriched = enriched.unique(subset="alias", keep="first", maintain_order=True)  # éviter les doublons au cas où

    enriched.write_csv(output_path)
    logger.success(
        f"  {len(new_clubs)} lignes ajoutées → {output_path} "
        f"({len(enriched)} entrées au total) ✅"
    )
    logger.info(
        "  → Pour voir les clubs à mapper : "
        "filtrer team_mapping.csv sur alias == 'NEW'"
    )


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main(
    tm_csv: Path,
    mapping_csv: Path,
    output_csv: Path,
    dry_run: bool,
    reset_ids: bool,
) -> None:
    logger.info("=== Démarrage enrich_team_mapping ===")

    for path in [tm_csv, mapping_csv]:
        if not path.exists():
            logger.error(f"Fichier introuvable : {path}")
            raise FileNotFoundError(path)

    # 1. Charger les deux CSV
    tm_clubs   = load_transfermarkt_clubs(tm_csv)
    mapping_df = load_team_mapping(mapping_csv)

    mapping_df = initialize_team_ids(mapping_df, reset=reset_ids)

    # 2. Extraire les alias existants
    existing_aliases = set(mapping_df["alias"].drop_nulls().to_list())

    # 3. Calculer les nouveaux clubs
    new_clubs = compute_new_clubs(tm_clubs, existing_aliases)


    # 4. Enrichir et écrire
    enrich_mapping(mapping_df, new_clubs, dry_run, output_csv)

    logger.success("=== enrich_team_mapping terminé ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrichit la seed team_mapping depuis le CSV Transfermarkt"
    )
    parser.add_argument(
        "--tm-csv",
        type=Path,
        default=DEFAULT_TM_CSV,
        help=f"CSV Transfermarkt (défaut : {DEFAULT_TM_CSV})",
    )
    parser.add_argument(
        "--mapping-csv",
        type=Path,
        default=DEFAULT_MAPPING_CSV,
        help=f"Seed team_mapping à enrichir (défaut : {DEFAULT_MAPPING_CSV})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Fichier de sortie (défaut : écrase --mapping-csv en place)",
    )
    parser.add_argument(
        "--reset-ids",
        action="store_true",
        help="Régénère tous les team_id from scratch (écrase les existants)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche les ajouts sans modifier le CSV",
    )
    args = parser.parse_args()

    # Par défaut : écriture en place sur le fichier mapping source
    output = args.output if args.output else args.mapping_csv

    main(
        tm_csv=args.tm_csv,
        mapping_csv=args.mapping_csv,
        output_csv=output,
        dry_run=args.dry_run,
        reset_ids=args.reset_ids,
    )