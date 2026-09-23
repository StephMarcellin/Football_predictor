from pathlib import Path
import pandas as pd

# 1. Chemins des fichiers
PATH_REFERENCE = Path(
    r"C:\dev\Projet_3etoiles\dbt_project\seeds\transfermarkt_clubs.csv"
)
PATH_MAPPING = Path(
    r"C:\dev\Projet_3etoiles\data\team_mapping_fixed_20260915_v1.csv"
)
OUTPUT_PATH = Path(
    r"C:\dev\Projet_3etoiles\data\team_mapping_fixed_20260915_resultat_X.csv"
)

# 2. Chargement des fichiers
df_ref = pd.read_csv(PATH_REFERENCE)
df_map = pd.read_csv(PATH_MAPPING)

# 3. Dédoublonnage du référentiel sur club_name (conservation de la 1ère occurrence)
ref_unique = (
    df_ref[["club_name", "club_tm_id"]]
    .drop_duplicates(subset=["club_name"], keep="first")
    .rename(columns={"club_tm_id": "new_team_id"})
)

# 4. Jointure et mise à jour de team_id
df_merged = pd.merge(df_map, ref_unique, on="club_name", how="left")
df_merged["team_id"] = (
    df_merged["new_team_id"]
    .combine_first(df_merged["team_id"])
    .astype("Int64")
)

# 5. Ajout du flag d'existence dans le référentiel et nettoyage
df_merged["valid"] = df_merged["club_name"].isin(ref_unique["club_name"])
df_result = df_merged.drop(columns=["new_team_id"])

# 6. Sauvegarde
df_result.to_csv(OUTPUT_PATH, index=False)
print(f"Mise à jour terminée ! Fichier sauvegardé sous : {OUTPUT_PATH}")