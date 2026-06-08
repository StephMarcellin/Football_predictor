"""
data_loader.py — Couche d'accès aux données pour le dashboard Streamlit.
Responsabilité unique : lire DuckDB et retourner des DataFrames pandas.
Ce fichier ne connaît pas Streamlit.
"""

import duckdb
import pandas as pd
from pathlib import Path

# ── Chemin vers la base DuckDB ────────────────────────────────────────────────
# __file__ = app/data_loader.py  →  .parent = app/  →  .parent = racine projet
ROOT_DIR = Path(__file__).parent.parent
DB_PATH  = ROOT_DIR / "db" / "football.duckdb"


def _connect() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB en lecture seule — jamais d'écriture depuis le dashboard."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def get_feature_tables() -> list[str]:
    """
    Retourne les noms des tables gold.features_* disponibles.
    Interroge information_schema dynamiquement — aucune table codée en dur.
    Exclut les tables de backup (suffixe __dbt_backup).
    """
    with _connect() as con:
        rows = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'gold'
              AND table_name LIKE 'features_%'
              AND table_name NOT LIKE '%__dbt_backup%'
            ORDER BY table_name
        """).fetchall()
    return [row[0] for row in rows]


def get_filter_values(table: str) -> tuple[list[str], list[str]]:
    """
    Retourne (ligues, saisons) disponibles pour une table donnée.
    Vérifie d'abord si les colonnes existent — features_draw n'a pas les mêmes
    colonnes que features_final.
    """
    with _connect() as con:
        # Colonnes disponibles dans cette table
        cols = {
            row[0] for row in con.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'gold' AND table_name = '{table}'
            """).fetchall()
        }

        leagues = []
        seasons = []

        if "league_source" in cols:
            leagues = [
                row[0] for row in con.execute(f"""
                    SELECT DISTINCT league_source
                    FROM gold.{table}
                    WHERE league_source IS NOT NULL
                    ORDER BY 1
                """).fetchall()
            ]

        if "season" in cols:
            seasons = [
                row[0] for row in con.execute(f"""
                    SELECT DISTINCT season
                    FROM gold.{table}
                    WHERE season IS NOT NULL
                    ORDER BY 1
                """).fetchall()
            ]

    return leagues, seasons


def get_null_stats(
    table: str,
    leagues: list[str],
    seasons: list[str],
) -> pd.DataFrame:
    """
    Calcule le % NULL pour chaque colonne numérique de la table.

    Paramètres
    ----------
    table   : nom de la table (ex: 'features_final')
    leagues : liste de ligues sélectionnées (vide = toutes)
    seasons : liste de saisons sélectionnées (vide = toutes)

    Retourne
    --------
    DataFrame : feature_name | null_count | total_count | null_pct
    """
    with _connect() as con:
        # 1. Récupérer les colonnes de la table pour savoir lesquelles filtrer
        all_cols = {
            row[0]: row[1] for row in con.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'gold' AND table_name = '{table}'
                ORDER BY ordinal_position
            """).fetchall()
        }

        # 2. Construire le WHERE dynamiquement
        conditions = []
        if leagues and "league_source" in all_cols:
            # On formate la liste Python en liste SQL : ['A','B'] → ('A','B')
            leagues_sql = ", ".join(f"'{l}'" for l in leagues)
            conditions.append(f"league_source IN ({leagues_sql})")
        if seasons and "season" in all_cols:
            seasons_sql = ", ".join(f"'{s}'" for s in seasons)
            conditions.append(f"season IN ({seasons_sql})")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # 3. Colonnes numériques uniquement (pas les identifiants VARCHAR)
        numeric_types = {"DOUBLE", "FLOAT", "INTEGER", "BIGINT", "HUGEINT"}
        numeric_cols = [
            col for col, dtype in all_cols.items()
            if dtype in numeric_types
        ]

        if not numeric_cols:
            return pd.DataFrame(columns=["feature_name", "null_count", "total_count", "null_pct"])

        # 4. Une seule requête avec tous les agrégats — évite N aller-retours DuckDB
        null_exprs = [
            f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}"'
            for col in numeric_cols
        ]
        query = f"""
            SELECT COUNT(*) AS total_count, {', '.join(null_exprs)}
            FROM gold.{table}
            {where}
        """
        row = con.execute(query).fetchone()

    total = row[0]
    records = [
        {
            "feature_name": col,
            "null_count":   row[i + 1],
            "total_count":  total,
            "null_pct":     round(row[i + 1] / total * 100, 1) if total > 0 else 0.0,
        }
        for i, col in enumerate(numeric_cols)
    ]
    return pd.DataFrame(records)

def get_coverage_by_season(
    table: str,
    leagues: list[str],
) -> pd.DataFrame:
    """
    Calcule le % de remplissage (100 - % NULL) par colonne et par saison.

    Retourne
    --------
    DataFrame en format long : feature_name | season | fill_pct
    """
    with _connect() as con:
        # Colonnes disponibles dans cette table
        all_cols = {
            row[0]: row[1] for row in con.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'gold' AND table_name = '{table}'
                ORDER BY ordinal_position
            """).fetchall()
        }

        if "season" not in all_cols:
            return pd.DataFrame(columns=["feature_name", "season", "fill_pct"])

        # Filtre championnat
        conditions = []
        if leagues and "league_source" in all_cols:
            leagues_sql = ", ".join(f"'{l}'" for l in leagues)
            conditions.append(f"league_source IN ({leagues_sql})")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Colonnes numériques uniquement
        numeric_types = {"DOUBLE", "FLOAT", "INTEGER", "BIGINT", "HUGEINT"}
        numeric_cols = [
            col for col, dtype in all_cols.items()
            if dtype in numeric_types
        ]

        if not numeric_cols:
            return pd.DataFrame(columns=["feature_name", "season", "fill_pct"])

        # Une requête par saison serait trop lente — on groupe directement en SQL
        # Pour chaque colonne : AVG(CASE WHEN col IS NULL THEN 0.0 ELSE 1.0 END) * 100
        # = % de lignes non-NULL = fill_pct
        fill_exprs = [
            f'AVG(CASE WHEN "{col}" IS NULL THEN 0.0 ELSE 1.0 END) * 100 AS "{col}"'
            for col in numeric_cols
        ]
        query = f"""
            SELECT season, {', '.join(fill_exprs)}
            FROM gold.{table}
            {where}
            GROUP BY season
            ORDER BY season
        """
        df_wide = con.execute(query).df()

    # Convertir du format wide (une colonne par feature) au format long
    # Format long : une ligne par (feature, saison) — requis par Plotly heatmap
    df_long = df_wide.melt(
        id_vars="season",
        value_vars=numeric_cols,
        var_name="feature_name",
        value_name="fill_pct",
    )
    df_long["fill_pct"] = df_long["fill_pct"].round(1)
    return df_long

def get_feature_importance() -> pd.DataFrame:
    """
    Extrait les feature importances des modèles LightGBM depuis le joblib.

    Retourne
    --------
    DataFrame : feature_name | importance | model
    Trois blocs : lgbm_home, lgbm_away, et moyenne des deux (combined).
    """
    import joblib

    joblib_path = ROOT_DIR / "models" / "football_stacking_v1.joblib"
    if not joblib_path.exists():
        return pd.DataFrame(columns=["feature_name", "importance", "model"])

    bundle = joblib.load(str(joblib_path))
    models        = bundle["models"]
    feature_names = bundle["feature_names"]

    records = []

    # lgbm_home et lgbm_away ont chacun leurs propres features
    for model_key, fname_key in [("lgbm_home", "home"), ("lgbm_away", "away")]:
        model  = models[model_key]
        fnames = feature_names[fname_key]

        importances = model.feature_importances_  # array numpy, une valeur par feature
        # gain : gain moyen en qualité de séparation apporté par chaque split
        # booster_ est l'objet LightGBM natif sous-jacent au wrapper sklearn
        gains  = model.booster_.feature_importance(importance_type="gain")
        for fname, sp, ga in zip(fnames, importances, gains):
            records.append({
                "feature_name": fname,
                "split":        float(sp),
                "gain":         float(ga),
                "model":        model_key,
            })

    df = pd.DataFrame(records)

    # Moyenne calculée séparément pour split et gain
    for metric in ["split", "gain"]:
        df_home = df[df["model"] == "lgbm_home"][["feature_name", metric]]
        df_away = df[df["model"] == "lgbm_away"][["feature_name", metric]]
        df_avg_metric = (
            pd.merge(df_home, df_away, on="feature_name", suffixes=("_h", "_a"))
            .assign(**{
                metric: lambda x, m=metric: (x[f"{m}_h"] + x[f"{m}_a"]) / 2,
                "model": "moyenne",
            })[["feature_name", metric, "model"]]
        )
        # On fusionne la colonne moyenne dans df_avg
        if metric == "split":
            df_avg = df_avg_metric.rename(columns={metric: metric})
        else:
            df_avg = df_avg.merge(df_avg_metric[["feature_name", metric]], on="feature_name")

    return pd.concat([df, df_avg], ignore_index=True)

def get_correlations(
    table: str,
    leagues: list[str],
    seasons: list[str],
    feature_subset: list[str],
) -> pd.DataFrame:
    """
    Calcule la matrice de corrélation de Pearson pour un sous-ensemble de features.

    Paramètres
    ----------
    table          : nom de la table (ex: 'features_final')
    leagues        : liste de ligues sélectionnées (vide = toutes)
    seasons        : liste de saisons sélectionnées (vide = toutes)
    feature_subset : liste des colonnes à inclure dans la corrélation

    Retourne
    --------
    DataFrame carré : index = colonnes = feature_subset, valeurs = corrélation Pearson
    """
    with _connect() as con:
        all_cols = {
            row[0]: row[1] for row in con.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'gold' AND table_name = '{table}'
                ORDER BY ordinal_position
            """).fetchall()
        }

        # Filtre WHERE
        conditions = []
        if leagues and "league_source" in all_cols:
            leagues_sql = ", ".join(f"'{l}'" for l in leagues)
            conditions.append(f"league_source IN ({leagues_sql})")
        if seasons and "season" in all_cols:
            seasons_sql = ", ".join(f"'{s}'" for s in seasons)
            conditions.append(f"season IN ({seasons_sql})")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # On ne sélectionne que les colonnes demandées
        # Les guillemets protègent les noms avec des caractères spéciaux
        cols_sql = ", ".join(f'"{c}"' for c in feature_subset)
        query = f"""
            SELECT {cols_sql}
            FROM gold.{table}
            {where}
        """
        df = con.execute(query).df()

    # dropna : on retire les lignes avec au moins un NULL
    # pour que pandas calcule la corrélation sur des données complètes
    return df.dropna().corr(method="pearson")