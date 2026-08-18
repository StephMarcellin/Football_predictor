#!/usr/bin/env python3
"""
Génère un vault Obsidian documentant + profilant les tables du projet dbt.
Une note markdown par modèle : rôle (schema.yml) + lineage ([[wikilinks]] depuis
ref()/source()) + profiling par feature (DuckDB). Voir SKILL.md pour les règles.
"""
import os, re, glob, argparse, yaml, duckdb

# Racine du projet = 3 niveaux au-dessus de skills/data-map/scripts/
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MODELS = os.path.join(ROOT, "dbt_project", "models")
DB = os.path.join(ROOT, "db", "football.duckdb")


def _unique_key_from_tests(tests):
    """Extrait la clé d'un test unique_combination_of_columns (ancienne ou nouvelle syntaxe)."""
    for t in tests or []:
        if isinstance(t, dict):
            for k, v in t.items():
                if "unique_combination_of_columns" in k and isinstance(v, dict):
                    return v.get("arguments", v).get("combination_of_columns")
    return None


def load_descriptions():
    """Descriptions table + colonnes + clé unique déclarée, depuis tous les schema.yml."""
    descs = {}
    for yml in glob.glob(f"{MODELS}/**/*.yml", recursive=True):
        try:
            doc = yaml.safe_load(open(yml, encoding="utf-8"))
        except Exception:
            continue
        if not doc or "models" not in doc:
            continue
        for m in doc["models"]:
            cols = {c["name"]: (c.get("description", "") or "") for c in m.get("columns", [])}
            descs[m["name"]] = {
                "desc": m.get("description", "") or "",
                "cols": cols,
                "key": _unique_key_from_tests(m.get("tests")),
            }
    return descs


def load_lineage():
    """deps[model]={parents}, children[model]={enfants} (via ref()/source()),
    et sql_key[model]=clé unique déclarée en config unique_key du .sql."""
    deps, sqlpath, sql_key = {}, {}, {}
    for sql in glob.glob(f"{MODELS}/**/*.sql", recursive=True):
        name = os.path.splitext(os.path.basename(sql))[0]
        sqlpath[name] = sql
        txt = open(sql, encoding="utf-8").read()
        refs = set(re.findall(r"ref\(\s*'([^']+)'\s*\)", txt))
        srcs = set(f"{s}.{t}" for s, t in re.findall(r"source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", txt))
        deps[name] = refs | srcs
        mk = re.search(r"unique_key\s*=\s*\[([^\]]+)\]", txt)
        if mk:
            sql_key[name] = re.findall(r"'([^']+)'", mk.group(1))
    children = {}
    for m, ps in deps.items():
        for p in ps:
            children.setdefault(p, set()).add(m)
    return deps, children, sqlpath, sql_key


def schema_of(name, sqlpath):
    p = sqlpath.get(name, "")
    for s in ("gold", "intermediate", "machine_learning", "silver"):
        if f"/{s}/" in p.replace("\\", "/"):
            return s
    return None


NUM = ("INT", "DOUBLE", "DECIMAL", "FLOAT", "HUGEINT", "BIGINT")


def profile(con, schema, table, extended):
    cols = con.sql(
        f"select column_name, data_type from information_schema.columns "
        f"where table_schema='{schema}' and table_name='{table}' order by ordinal_position"
    ).fetchall()
    if not cols:
        return None, None
    n = con.sql(f"select count(*) from {schema}.{table}").fetchone()[0]
    rows = []
    for c, dt in cols:
        q = f'"{c}"'
        nonnull, distinct = con.sql(f"select count({q}), count(distinct {q}) from {schema}.{table}").fetchone()
        comp = round(100.0 * nonnull / n, 1) if n else 0.0
        nulls = n - nonnull
        is_id = c.endswith("_id") or c in ("match_id",)
        stat = ""
        if any(k in dt.upper() for k in NUM) and not is_id:
            try:
                mn, md, mnx, mxx = con.sql(
                    f"select round(avg({q}::DOUBLE),3), round(median({q}::DOUBLE),3), "
                    f"round(min({q}::DOUBLE),3), round(max({q}::DOUBLE),3) from {schema}.{table}"
                ).fetchone()
                stat = f"moy {mn} · méd {md} · min/max {mnx}/{mxx}"
                if extended:
                    p10, p90 = con.sql(
                        f"select round(quantile_cont({q}::DOUBLE,0.1),3), round(quantile_cont({q}::DOUBLE,0.9),3) "
                        f"from {schema}.{table}"
                    ).fetchone()
                    stat += f" · p10/p90 {p10}/{p90}"
            except Exception:
                stat = ""
        # Top-3 valeurs pour les colonnes catégorielles (peu de distinct)
        elif extended and not is_id and distinct is not None and 0 < distinct <= 15:
            try:
                top = con.sql(
                    f"select {q}, count(*) c from {schema}.{table} where {q} is not null "
                    f"group by 1 order by c desc limit 3"
                ).fetchall()
                stat = "top: " + ", ".join(f"{v} ({c})" for v, c in top)
            except Exception:
                stat = ""
        rows.append((c, dt, comp, nulls, distinct, stat))
    return n, rows


def check_duplicates(con, schema, table, key):
    """Nb de lignes en trop par rapport au grain déclaré (0 = pas de doublon)."""
    try:
        cols = ", ".join(f'"{c}"' for c in key)
        total = con.sql(f"select count(*) from {schema}.{table}").fetchone()[0]
        uniq = con.sql(f"select count(*) from (select distinct {cols} from {schema}.{table})").fetchone()[0]
        return total - uniq
    except Exception:
        return None


def make_note(name, descs, deps, children, sqlpath, sql_key, con, extended):
    sch = schema_of(name, sqlpath)
    d = descs.get(name, {"desc": "", "cols": {}, "key": None})
    n, rows = profile(con, sch, name, extended) if sch else (None, None)
    key = d.get("key") or sql_key.get(name)
    dups = check_duplicates(con, sch, name, key) if (key and sch) else None
    # Tag famille depuis la DESCRIPTION DE TABLE uniquement (évite les faux tags)
    fam = sorted(set(re.findall(r"[Ff]amille(?:s)?(?:\s+CDC)?\s+(\d+)", d["desc"])), key=int)
    tags = " ".join([f"#{sch}"] + [f"#famille{f}" for f in fam]) if sch else ""
    parents = sorted(deps.get(name, []))
    kids = sorted(children.get(name, []))
    # Intégrité (doublons sur le grain déclaré)
    if key:
        icon = "✅ aucun doublon" if dups == 0 else (f"⚠️ **{dups} doublons**" if dups else "?")
        integ = f"**Clé déclarée :** ({', '.join(key)}) — {icon}"
    else:
        integ = "**Clé déclarée :** — (pas de test d'unicité)"
    L = ["---", f"schema: {sch}", f"rows: {n}", "---", f"# {name}", "", tags, "",
         d["desc"] or "_(pas de description schema.yml)_", "",
         "## Intégrité", integ, "",
         "## Lineage",
         "**Sources :** " + (", ".join(f"[[{p}]]" for p in parents) if parents else "—"),
         "**Alimente :** " + (", ".join(f"[[{k}]]" for k in kids) if kids else "—"),
         "", f"## Features & profiling  ({n if n is not None else '?'} lignes)", ""]
    if rows:
        L.append("| Feature | Type | Complétion | Null | Distinct | Stats | Description |")
        L.append("|---|---|---|---|---|---|---|")
        for c, dt, comp, nulls, distinct, stat in rows:
            desc = d["cols"].get(c, "").replace("\n", " ").replace("|", "/")
            L.append(f"| `{c}` | {dt} | {comp}% | {nulls} | {distinct} | {stat} | {desc} |")
    else:
        L.append("_Profiling indisponible (table non matérialisée ?)_")
    struct = {
        "name": name, "schema": sch, "rows": n, "key": key, "duplicates": dups,
        "sources": parents, "children": kids,
        "columns": [
            {"name": c, "type": dt, "completion_pct": comp, "nulls": nulls, "distinct": distinct, "stats": stat}
            for (c, dt, comp, nulls, distinct, stat) in (rows or [])
        ],
    }
    return "\n".join(L), sch, struct


def make_index(generated, out):
    by_schema = {}
    for name, sch in generated:
        by_schema.setdefault(sch or "autre", []).append(name)
    L = ["---", "type: index", "---", "# 🗺️ Data Map", "",
         "Carte des tables du projet. Ouvre la vue graphe d'Obsidian pour la map cliquable.", ""]
    for sch in ("silver", "intermediate", "gold", "machine_learning", "autre"):
        if sch in by_schema:
            L.append(f"## {sch}")
            for name in sorted(by_schema[sch]):
                L.append(f"- [[{name}]]")
            L.append("")
    open(os.path.join(out, "_Data Map.md"), "w", encoding="utf-8").write("\n".join(L))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", help="liste séparée par des virgules (défaut : toutes)")
    ap.add_argument("--out", default=os.path.join(ROOT, "docs", "data_map"))
    ap.add_argument("--basic", action="store_true", help="profiling basique (sans quantiles ni top-valeurs)")
    args = ap.parse_args()
    extended = not args.basic

    os.makedirs(args.out, exist_ok=True)
    descs = load_descriptions()
    deps, children, sqlpath, sql_key = load_lineage()
    # Dossier temp explicite : en read_only, DuckDB n'a pas de temp valide et
    # plante ("Failed to create directory \\.tmp") quand un count(distinct) sur
    # une grosse table doit déborder sur disque. On lui en donne un + plus de RAM.
    tmp = os.path.join(ROOT, ".duckdb_tmp")
    os.makedirs(tmp, exist_ok=True)
    con = duckdb.connect(DB, read_only=True, config={
        "threads": "4", "memory_limit": "8GB", "temp_directory": tmp,
    })

    targets = args.models.split(",") if args.models else [
        n for n in sqlpath if schema_of(n, sqlpath) in ("gold", "intermediate", "machine_learning")
    ]
    import json
    # Fusion incrémentale : on repart de l'existant pour que des lots successifs
    # s'accumulent en une carte complète (utile car profiler 25 M lignes est long).
    json_path = os.path.join(args.out, "_data_map.json")
    merged = {}
    if os.path.exists(json_path):
        try:
            for s in json.load(open(json_path, encoding="utf-8")):
                merged[s["name"]] = s
        except Exception:
            pass

    for name in sorted(targets):
        note, sch, struct = make_note(name, descs, deps, children, sqlpath, sql_key, con, extended)
        open(os.path.join(args.out, f"{name}.md"), "w", encoding="utf-8").write(note)
        merged[name] = struct
        flag = "" if struct["duplicates"] in (0, None) else f"  ⚠️ {struct['duplicates']} doublons"
        print(f"  {name}.md{flag}")

    allstructs = sorted(merged.values(), key=lambda s: ((s.get("schema") or "zzz"), s["name"]))
    make_index([(s["name"], s.get("schema")) for s in allstructs], args.out)
    json.dump(allstructs, open(json_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n{len(targets)} notes générées · {len(allstructs)} au total → {args.out}")


if __name__ == "__main__":
    main()
