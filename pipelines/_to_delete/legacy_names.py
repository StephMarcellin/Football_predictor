"""
legacy_names.py — Couche de compatibilité Python ↔ nommage refondu dbt.

Depuis la refonte du nommage (préfixe de type en tête : str_, int_, dec_, dt_,
bool_), toutes les tables dbt exposent de nouveaux noms. La correspondance
complète (ancien nom → nouveau nom + types) est dans
docs/proposition_nommage_v2.csv, générée depuis les modèles dbt eux-mêmes.

Les scripts Python historiques (serving, imputation KNN, embeddings, xT, xGOT,
export Spark) raisonnent avec les anciens noms. Plutôt que de réécrire leur
logique, on leur donne des VUES TEMPORAIRES « legacy_<table> » qui remappent les
colonnes refondues vers les anciens noms ET les anciens types (ex. str_team_id
→ team_id BIGINT). Même principe que les CTE in_<modèle> côté dbt.

    import legacy_names as ln
    ln.install(con, "gold.joueur_saison", "intermediate.int_whoscored_lineup")
    con.execute("select player_id, scorer_xg_per90_lag from legacy_joueur_saison")

Les vues sont TEMPORAIRES (propres à la connexion) : elles fonctionnent aussi sur
une connexion read_only et ne modifient jamais la base.
"""
from pathlib import Path
import csv

_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
_CSV = _ROOT / "docs" / "proposition_nommage_v2.csv"
_MAP = None


def mapping():
    """{table: [(ancien_nom, ancien_type, nouveau_nom), ...]} dans l'ordre du modèle."""
    global _MAP
    if _MAP is None:
        _MAP = {}
        with open(_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f, delimiter=";"):
                _MAP.setdefault(r["nom_table"], []).append(
                    (r["nom_variable_actuel"], r["type_actuel"], r["new_nom_variable"]))
    return _MAP


def legacy_select(qualified_table, existing=None):
    """SELECT qui relit `schema.table` refondue sous ses anciens noms et types.

    `existing` = noms de colonnes présents physiquement dans la table. Pendant la
    transition, une table peut ne pas encore avoir été reconstruite par dbt
    (anciens noms) : chaque colonne est alors lue sous le nom qui existe
    réellement (nouveau d'abord, sinon ancien), sans conversion."""
    table = qualified_table.split(".")[-1]
    cols = []
    for old, old_type, new in mapping()[table]:
        if existing is not None and new not in existing:
            if old in existing:
                cols.append(f'"{old}"')        # table pas encore refondue : lecture telle quelle
            continue                           # colonne absente : on ne l'invente pas
        expr = new
        # retour au type d'origine uniquement quand la refonte l'a changé
        if new.startswith("str_") and old_type != "VARCHAR":
            expr = f"CAST({new} AS {old_type})"
        elif new.startswith("int_") and old_type in ("DOUBLE", "FLOAT", "HUGEINT"):
            expr = f"CAST({new} AS {old_type})"
        elif new.startswith("dt_") and old_type == "VARCHAR":
            expr = f"CAST({new} AS VARCHAR)"
        cols.append(f'{expr} AS "{old}"')
    return f"SELECT {', '.join(cols)} FROM {qualified_table}"


def install(con, *qualified_tables):
    """Crée (ou remplace) les vues temporaires legacy_<table> sur `con`."""
    names = {}
    for qt in qualified_tables:
        name = "legacy_" + qt.split(".")[-1]
        existing = {r[0] for r in con.execute(f"DESCRIBE {qt}").fetchall()}
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS {legacy_select(qt, existing)}")
        names[qt] = name
    return names


def to_new(table, old_names):
    """Traduit une liste/dict de noms anciens → noms refondus pour `table`
    (les noms inconnus sont laissés tels quels)."""
    m = {old: new for old, _, new in mapping()[table]}
    if isinstance(old_names, dict):
        return {m.get(k, k): v for k, v in old_names.items()}
    return [m.get(k, k) for k in old_names]
