import pandas as pd

# 1. Chemins des fichiers
CHEMIN_REFERENCE = "C:\\dev\\Projet_3etoiles\\dbt_project\\seeds\\transfermarkt_clubs.csv"
CHEMIN_A_COMPARER = "C:\\dev\\Projet_3etoiles\\data\\team_mapping_fixed_20260915_v1.csv"
CHEMIN_SORTIE = "C:\\dev\\Projet_3etoiles\\data\\team_mapping_fixed_20260915_resultat.csv"

# 2. Chargement du fichier de référence
# On extrait uniquement la colonne 'club_name' sous forme d'ensemble (set) pour une recherche instantanée
df_ref = pd.read_csv(CHEMIN_REFERENCE, usecols=["club_name"])
clubs_references = set(df_ref["club_name"].dropna().unique())

# 3. Chargement du fichier à comparer
df_cible = pd.read_csv(CHEMIN_A_COMPARER)

# 4. Comparaison vectorisée : création de la colonne 'valid'
df_cible["valid"] = df_cible["club_name"].isin(clubs_references)

# 5. Sauvegarde du résultat
df_cible.to_csv(CHEMIN_SORTIE, index=False)

print("Traitement terminé et fichier sauvegardé !")