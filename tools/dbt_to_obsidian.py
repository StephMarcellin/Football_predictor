"""
dbt_to_obsidian.py
==================
Génère une note Obsidian (.md) pour chaque modèle dbt,
à partir de target/manifest.json et target/catalog.json.

!!!! note "Pré-requis"
# D'abord, générer la doc dbt
dbt docs generate
!!! 

Usage (PowerShell, depuis la racine du projet dbt) :
    python tools/dbt_to_obsidian.py
    python tools/dbt_to_obsidian.py --dbt-target target --vault "C:\\Obsidian\\3_étoiles_obsidian\\dbt"
    python tools/dbt_to_obsidian.py --dry-run   # affiche sans écrire

Structure générée dans le vault :
    dbt/
        _INDEX.md               ← vue d'ensemble de tous les modèles
        staging/
            stg_fbref_shots.md
            stg_whoscored_events.md
        marts/
            fct_matches.md
            dim_teams.md
        ...
"""

import json
import argparse
from datetime import datetime
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────────────
# Configuration par défaut
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_TARGET_DIR = Path("dbt_project\\target")
DEFAULT_VAULT_DIR  = Path(r"C:\\Obsidian\\3_étoiles_obsidian\\03 - dbt")
ROOT_DIR         = Path(__file__).parent.parent  # racine du projet dbt


# ──────────────────────────────────────────────────────────────────────────────
# Chargement des artefacts dbt
# ──────────────────────────────────────────────────────────────────────────────

def load_manifest(target_dir: Path) -> dict:
    path = target_dir / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(
            f"manifest.json introuvable dans '{target_dir}'.\n"
            "Lance d'abord : dbt docs generate"
        )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_catalog(target_dir: Path) -> dict:
    path = target_dir / "catalog.json"
    if not path.exists():
        print("  ⚠️  catalog.json absent — les types de colonnes ne seront pas inclus.")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ──────────────────────────────────────────────────────────────────────────────
# Extraction des données par modèle
# ──────────────────────────────────────────────────────────────────────────────

def extract_models(manifest: dict) -> list[dict]:
    """
    Retourne une liste de dicts enrichis pour chaque nœud modèle/source/seed/snapshot.
    """
    nodes     = manifest.get("nodes", {})
    sources   = manifest.get("sources", {})
    parent_map = manifest.get("parent_map", {})
    child_map  = manifest.get("child_map", {})

    models = []

    for unique_id, node in nodes.items():
        if node.get("resource_type") not in ("model", "seed", "snapshot"):
            continue

        name        = node.get("name", "")
        description = node.get("description", "").strip()
        schema      = node.get("schema", "")
        database    = node.get("database", "")
        config      = node.get("config", {})
        materialized = config.get("materialized", "")
        path        = node.get("path", "")          # ex: "staging/stg_fbref.sql"
        tags        = node.get("tags", [])
        columns     = node.get("columns", {})       # colonnes documentées dans .yml
        tests       = _extract_tests(unique_id, nodes)

        # Lineage : parents et enfants (uniquement les modèles, pas les tests)
        raw_parents = parent_map.get(unique_id, [])
        raw_children = child_map.get(unique_id, [])

        parents  = _filter_model_ids(raw_parents, nodes, sources)
        children = _filter_model_ids(raw_children, nodes, sources)

        # Dossier de destination dans le vault (reprend le dossier dbt)
        # ex: "staging/stg_fbref.sql" → "staging"
        folder = Path(path).parent.as_posix() if path else "."
        if folder == ".":
            folder = "other"

        models.append({
            "unique_id":    unique_id,
            "name":         name,
            "description":  description,
            "schema":       schema,
            "database":     database,
            "materialized": materialized,
            "folder":       folder,
            "tags":         tags,
            "columns":      columns,
            "tests":        tests,
            "parents":      parents,
            "children":     children,
            "resource_type": node.get("resource_type"),
        })

    return models


def _filter_model_ids(
    id_list: list[str],
    nodes: dict,
    sources: dict,
) -> list[str]:
    """Garde uniquement les noms de modèles/sources réels (pas les tests)."""
    names = []
    for uid in id_list:
        if uid in nodes:
            rtype = nodes[uid].get("resource_type")
            if rtype in ("model", "seed", "snapshot", "source"):
                names.append(nodes[uid]["name"])
        elif uid in sources:
            names.append(sources[uid]["name"])
    return sorted(set(names))


def _extract_tests(unique_id: str, nodes: dict) -> list[str]:
    """Retourne les noms des tests attachés à ce modèle."""
    tests = []
    for node in nodes.values():
        if node.get("resource_type") != "test":
            continue
        depends = node.get("depends_on", {}).get("nodes", [])
        if unique_id in depends:
            test_name = node.get("name", "")
            # Nettoie le nom : "not_null_stg_fbref_match_id" → "not_null (match_id)"
            tests.append(_clean_test_name(test_name, node))
    return sorted(set(tests))


def _clean_test_name(test_name: str, node: dict) -> str:
    """Rend le nom du test lisible."""
    # dbt nomme les tests : "not_null_model_name_column_name"
    test_meta = node.get("test_metadata", {})
    kind      = test_meta.get("name", "")
    kwargs    = test_meta.get("kwargs", {})
    col       = kwargs.get("column_name", "")

    if kind and col:
        return f"`{kind}` sur `{col}`"
    return f"`{test_name}`"


def enrich_with_catalog(models: list[dict], catalog: dict) -> None:
    """Ajoute les types de colonnes issus du catalog.json (in-place)."""
    if not catalog:
        return
    catalog_nodes = catalog.get("nodes", {})
    catalog_sources = catalog.get("sources", {})
    all_catalog = {**catalog_nodes, **catalog_sources}

    for model in models:
        uid = model["unique_id"]
        cat_node = all_catalog.get(uid, {})
        cat_cols = cat_node.get("columns", {})

        # Fusionne : les colonnes dbt-docs (descriptions) + catalog (types)
        for col_name, col_data in model["columns"].items():
            col_lower = col_name.lower()
            if col_lower in cat_cols:
                col_data["type"] = cat_cols[col_lower].get("type", "")

        # Colonnes présentes dans catalog mais pas documentées dans .yml
        for col_name, cat_col in cat_cols.items():
            if col_name not in model["columns"]:
                model["columns"][col_name] = {
                    "name":        col_name,
                    "description": "",
                    "type":        cat_col.get("type", ""),
                }


# ──────────────────────────────────────────────────────────────────────────────
# Génération des notes Markdown
# ──────────────────────────────────────────────────────────────────────────────

def model_to_markdown(model: dict, generated_at: str) -> str:
    """Génère le contenu Markdown complet d'une note Obsidian pour un modèle."""

    lines = []

    # ── Frontmatter YAML ──────────────────────────────────────────────────────
    lines.append("---")
    lines.append(f"dbt_name: {model['name']}")
    lines.append(f"schema: {model['schema']}")
    lines.append(f"materialized: {model['materialized']}")
    lines.append(f"resource_type: {model['resource_type']}")
    if model["tags"]:
        tags_str = ", ".join(f'"{t}"' for t in model["tags"])
        lines.append(f"tags: [{tags_str}, dbt]")
    else:
        lines.append("tags: [dbt]")
    lines.append(f"generated_at: {generated_at}")
    lines.append("---")
    lines.append("")

    # ── Titre + description ───────────────────────────────────────────────────
    lines.append(f"# {model['name']}")
    lines.append("")

    if model["description"]:
        lines.append(f"> {model['description']}")
    else:
        lines.append("> *(pas de description — à remplir dans le fichier .yml dbt)*")
    lines.append("")

    # ── Infos rapides ─────────────────────────────────────────────────────────
    lines.append("## Infos")
    lines.append("")
    lines.append(f"| Propriété      | Valeur |")
    lines.append(f"|----------------|--------|")
    lines.append(f"| **Schema**     | `{model['schema']}` |")
    if model["database"]:
        lines.append(f"| **Database**   | `{model['database']}` |")
    lines.append(f"| **Matérialisation** | `{model['materialized']}` |")
    lines.append(f"| **Type**       | `{model['resource_type']}` |")
    lines.append("")

    # ── Lineage ───────────────────────────────────────────────────────────────
    lines.append("## Lineage")
    lines.append("")

    if model["parents"]:
        parents_links = " · ".join(f"[[{p}]]" for p in model["parents"])
        lines.append(f"**⬆ Parents :** {parents_links}")
    else:
        lines.append("**⬆ Parents :** *(source brute — pas de modèle parent)*")

    lines.append("")
    lines.append(f"**➡ Ce modèle : `{model['name']}`**")
    lines.append("")

    if model["children"]:
        children_links = " · ".join(f"[[{c}]]" for c in model["children"])
        lines.append(f"**⬇ Enfants :** {children_links}")
    else:
        lines.append("**⬇ Enfants :** *(modèle final — pas de dépendants)*")

    lines.append("")

    # ── Colonnes ──────────────────────────────────────────────────────────────
    lines.append("## Colonnes")
    lines.append("")

    cols = model["columns"]
    if cols:
        lines.append("| Colonne | Type | Description |")
        lines.append("|---------|------|-------------|")
        for col_name, col_data in sorted(cols.items()):
            col_type = col_data.get("type", "")
            col_desc = col_data.get("description", "").replace("|", "\\|").replace("\n", " ")
            lines.append(f"| `{col_name}` | `{col_type}` | {col_desc} |")
    else:
        lines.append("*(colonnes non documentées dans les fichiers .yml dbt)*")

    lines.append("")

    # ── Tests ─────────────────────────────────────────────────────────────────
    lines.append("## Tests dbt")
    lines.append("")

    if model["tests"]:
        for test in model["tests"]:
            lines.append(f"- {test}")
    else:
        lines.append("*(aucun test défini pour ce modèle)*")

    lines.append("")

    # ── Footer ────────────────────────────────────────────────────────────────
    lines.append("---")
    lines.append(f"*Note générée automatiquement par `dbt_to_obsidian.py` le {generated_at}*")
    lines.append("*Relance `python tools/dbt_to_obsidian.py` pour mettre à jour.*")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Génération de la note INDEX
# ──────────────────────────────────────────────────────────────────────────────

def generate_index(models: list[dict], generated_at: str) -> str:
    """Génère _INDEX.md : tableau récapitulatif de tous les modèles."""

    lines = []
    lines.append("---")
    lines.append("tags: [dbt, index]")
    lines.append(f"generated_at: {generated_at}")
    lines.append("---")
    lines.append("")
    lines.append("# 📦 Index dbt — Tous les modèles")
    lines.append("")
    lines.append(f"*Mis à jour le {generated_at} — {len(models)} modèle(s)*")
    lines.append("")

    # Groupe par dossier
    from itertools import groupby
    sorted_models = sorted(models, key=lambda m: (m["folder"], m["name"]))

    for folder, group in groupby(sorted_models, key=lambda m: m["folder"]):
        group_list = list(group)
        lines.append(f"## 📁 {folder}")
        lines.append("")
        lines.append("| Modèle | Matérialisation | Description |")
        lines.append("|--------|-----------------|-------------|")
        for m in group_list:
            desc = (m["description"][:80] + "…") if len(m["description"]) > 80 else m["description"]
            desc = desc.replace("|", "\\|") or "*(non documenté)*"
            lines.append(f"| [[{m['name']}]] | `{m['materialized']}` | {desc} |")
        lines.append("")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Écriture dans le vault
# ──────────────────────────────────────────────────────────────────────────────

def write_vault(
    models: list[dict],
    vault_dir: Path,
    generated_at: str,
    dry_run: bool = False,
) -> None:
    """Écrit toutes les notes dans le vault Obsidian."""

    if not dry_run:
        vault_dir.mkdir(parents=True, exist_ok=True)

    written  = 0
    skipped  = 0

    for model in models:
        content    = model_to_markdown(model, generated_at)
        note_dir   = vault_dir / model["folder"]
        note_path  = note_dir / f"{model['name']}.md"

        if dry_run:
            print(f"  [DRY-RUN] {note_path}")
            skipped += 1
            continue

        note_dir.mkdir(parents=True, exist_ok=True)
        note_path.write_text(content, encoding="utf-8")
        written += 1

    # Index
    index_content = generate_index(models, generated_at)
    index_path    = vault_dir / "_INDEX.md"

    if not dry_run:
        index_path.write_text(index_content, encoding="utf-8")
        print(f"  ✅ {written} note(s) écrite(s) dans : {vault_dir}")
        print(f"  📋 Index généré : {index_path}")
    else:
        print(f"  [DRY-RUN] Index : {index_path}")
        print(f"  [DRY-RUN] {skipped} note(s) seraient écrites dans : {vault_dir}")


# ──────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Synchronise la doc dbt vers un vault Obsidian."
    )
    parser.add_argument(
        "--dbt-target",
        type=Path,
        default=DEFAULT_TARGET_DIR,
        help=f"Dossier target/ de dbt (défaut : {DEFAULT_TARGET_DIR})",
    )
    parser.add_argument(
        "--vault",
        type=Path,
        default=DEFAULT_VAULT_DIR,
        help=f"Dossier de destination dans le vault Obsidian (défaut : {DEFAULT_VAULT_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche les fichiers qui seraient créés, sans écrire.",
    )
    args = parser.parse_args()

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    SOURCE = ROOT_DIR / DEFAULT_TARGET_DIR
    print(f"\n🔄 dbt → Obsidian")
    print(f"   Source  : {SOURCE}")
    print(f"   Vault   : {args.vault}")
    print(f"   Dry-run : {args.dry_run}")
    print()

    # 1. Charger les artefacts
    manifest = load_manifest(SOURCE)
    catalog  = load_catalog(SOURCE)

    # 2. Extraire et enrichir les modèles
    models = extract_models(manifest)
    enrich_with_catalog(models, catalog)

    print(f"  📦 {len(models)} modèle(s) trouvé(s) dans le manifest")

    if not models:
        print("  ⚠️  Aucun modèle trouvé. Vérifie que dbt docs generate a bien tourné.")
        return

    # 3. Écrire dans le vault
    write_vault(models, args.vault, generated_at, dry_run=args.dry_run)

    print("\n✅ Synchronisation terminée !\n")


if __name__ == "__main__":
    main()