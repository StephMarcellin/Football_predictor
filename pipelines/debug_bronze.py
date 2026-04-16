import polars as pl
from pathlib import Path

RAW_DIR = Path("data/raw")
prq_root = RAW_DIR / "fbref" / "parquet" / "schedule"

df = pl.concat(
    [pl.read_parquet(f) for f in sorted(prq_root.glob("*.parquet"))],
    how="diagonal"
)

print(f"Total lignes brutes : {len(df):,}")
print(f"Colonnes : {df.columns}")

# Utiliser 'comp' au lieu de 'league_source'
result = (
    df.filter(
        pl.col("comp").is_in([
            "Premier League", "Ligue 1", "Serie A", "La Liga", "Bundesliga",
            # Essayer aussi les noms FBref originaux si différents
            "Fußball-Bundesliga", "La Liga", "Ligue 1"
        ])
    )
    .group_by(["comp", "season", "team"])
    .agg(pl.len().alias("nb_matchs"))
    .group_by(["comp", "season"])
    .agg(pl.col("team").n_unique().alias("nb_teams"))
    .sort(["comp", "season"])
)

print(result)

# Voir les valeurs uniques de comp pour identifier les noms exacts
print("\nValeurs uniques de comp :")
print(df["comp"].unique().sort())