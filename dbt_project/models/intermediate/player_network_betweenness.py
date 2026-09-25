import networkx as nx
import pandas as pd


def model(dbt, session):

    dbt.config(
        materialized="incremental",
        unique_key=["str_match_id", "str_team_id", "str_player_id"],
        on_schema_change="append_new_columns",
        schema="intermediate",
        alias="player_network_betweenness",
    )

    # ══════════════════════════════════════════════════════════════════════════
    # LECTURE DE LA SOURCE
    # dbt.ref() retourne une Relation DuckDB → .df() la convertit en DataFrame
    # On ne garde que les colonnes nécessaires au calcul du graphe.
    # ══════════════════════════════════════════════════════════════════════════
    # Refonte nommage : player_network_passes expose des noms préfixés par type
    # (str_, int_…) ; on les remappe vers les noms de travail du calcul ci-dessous.
    passes = dbt.ref("player_network_passes").df()
    df = passes[
        ["str_match_id", "str_team_id", "str_passer_id", "str_receiver_id",
         "int_n_passes", "str_season", "str_league_source"]
    ].rename(columns={
        "str_match_id": "match_id", "str_team_id": "team_id",
        "str_passer_id": "passer_id", "str_receiver_id": "receiver_id",
        "int_n_passes": "n_passes", "str_season": "season",
        "str_league_source": "league_source",
    })

    # ══════════════════════════════════════════════════════════════════════════
    # FILTRE INCRÉMENTAL
    # On exclut les match_id déjà présents dans la table cible.
    # dbt.is_incremental() retourne True si la table existe déjà.
    # ══════════════════════════════════════════════════════════════════════════
    if dbt.is_incremental:
        already_done = session.sql(
            f"SELECT DISTINCT str_match_id AS match_id FROM {dbt.this}"
        ).df()
        df = df[~df["match_id"].isin(already_done["match_id"])]

    # ══════════════════════════════════════════════════════════════════════════
    # CALCUL DE LA BETWEENNESS PAR (match_id, team_id)
    # Pour chaque groupe :
    #   1. On construit un graphe dirigé pondéré (DiGraph)
    #      — chaque arête passer→receiver a pour poids n_passes
    #   2. NetworkX calcule la betweenness centrality exacte
    #      — normalized=True : score entre 0 et 1 quelle que soit
    #        la taille du graphe (divisé par (n-1)*(n-2))
    #      — weight='weight' : les chemins privilégient les arêtes
    #        à fort volume de passes
    #   3. On aplatit le dictionnaire {player_id: score} en lignes DataFrame
    #      et on réattache match_id, team_id, season, league_source
    # ══════════════════════════════════════════════════════════════════════════
    results = []

    for (match_id, team_id), group in df.groupby(["match_id", "team_id"]):

        # Récupère season et league_source depuis la première ligne du groupe
        season       = group["season"].iloc[0]
        league_source = group["league_source"].iloc[0]

        # Construction du graphe dirigé pondéré
        G = nx.DiGraph()
        for _, row in group.iterrows():
            G.add_edge(
                row["passer_id"],
                row["receiver_id"],
                weight=row["n_passes"]
            )

        # Calcul betweenness exacte
        betweenness = nx.betweenness_centrality(
            G,
            weight="weight",
            normalized=True
        )

        # Aplatissement : dictionnaire → lignes DataFrame
        for player_id, score in betweenness.items():
            # Sortie aux noms refondus (préfixe de type en tête, ids en texte)
            results.append({
                "str_match_id":          match_id,
                "str_team_id":           str(team_id),
                "str_player_id":         str(player_id),
                "str_season":            season,
                "str_league_source":     league_source,
                "dec_betweenness_exact": round(score, 6),
            })

    # ══════════════════════════════════════════════════════════════════════════
    # RETOUR
    # dbt matérialise ce DataFrame comme une table DuckDB.
    # Si results est vide (run incrémental sans nouveaux matchs),
    # on retourne un DataFrame vide avec le bon schéma pour éviter
    # une erreur de matérialisation.
    # ══════════════════════════════════════════════════════════════════════════
    if not results:
        # DataFrame vide TYPÉ explicitement.
        # pd.DataFrame(columns=[...]) ne déclare que des noms : sans données,
        # pandas ne peut pas inférer les dtypes et en invente. Combiné à
        # on_schema_change='sync_all_columns', dbt émet alors un ALTER TABLE
        # sur la cible, qui casse sur les match_id SHA1 déjà stockés.
        # pd.Series(dtype=...) crée une série vide mais typée : le schéma est
        # déclaré même sans une seule ligne.
        return pd.DataFrame({
            "str_match_id":          pd.Series(dtype="object"),   # SHA1 → VARCHAR
            "str_team_id":           pd.Series(dtype="object"),   # → VARCHAR
            "str_player_id":         pd.Series(dtype="object"),   # → VARCHAR
            "str_season":            pd.Series(dtype="object"),   # → VARCHAR
            "str_league_source":     pd.Series(dtype="object"),   # → VARCHAR
            "dec_betweenness_exact": pd.Series(dtype="float64"),  # → DOUBLE
        })

    return pd.DataFrame(results)