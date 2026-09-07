# `ingest/` — Acquisition & couche Silver

Transforme les données **brutes** (scraping, HTML, CSV JSON) en tables
`silver.*` propres, normalisées et **joignables à 100 %**, prêtes pour la couche
dbt (gold / marts).

> Convention : tous les scripts se lancent depuis la **racine du repo**
> (`python pipelines/ingest/<script>.py`). Le `ROOT_DIR` est ancré sur
> `config.yaml`, donc l'emplacement du script n'a pas d'importance.

---

## 1. Amont — à faire AVANT `ingest` (données brutes)

Rien de tout ceci n'écrit dans `silver` ; on ne fait que **produire la matière brute**.

| Source | Comment obtenir la donnée brute | Destination |
|---|---|---|
| WhoScored events/compos | `python pipelines/ingest/run_scrapping.py --step scrape_raw` (ou `--serve` pour le flow nocturne Prefect) | JSON dans `data/raw/whoscored/` |
| FBref / Understat / WhoScored team-season | scrapers `pipelines/scrapping/team_stats/*` **ou** dépôt manuel des HTML/CSV | `data/raw/{fbref,understat,whoscored}/` |
| Cotes | CSV football-data.co.uk | `data/raw/bets/` |

Le scraping WhoScored est **borné en temps** et **reprend sur disque** d'une nuit
à l'autre — aucune écriture DuckDB pendant le scrape (DBeaver reste libre).

---

## 2. Dans `ingest` — dans l'ordre

Tout est orchestré par **`run_ingest.py`** (ou lance les étapes une par une) :

```bash
python pipelines/ingest/run_ingest.py                 # tout, dans l'ordre
python pipelines/ingest/run_ingest.py --step odds     # une seule étape
python pipelines/ingest/run_ingest.py --dry-run       # liste sans exécuter
```

| # | Script | Rôle | Écrit |
|---|---|---|---|
| 1 | `run_scrapping.py --step load_archive` | Charge les JSON WhoScored archivés dans DuckDB | `silver.stg_whoscored_events`, `stg_whoscored_formations` (compos), `stg_whoscored_match_index`, `stg_whoscored_urls` |
| 2 | `01_ingest.py [--source fbref\|understat\|whoscored]` | HTML/CSV bruts → Parquet Bronze | `data/raw/{source}/parquet/` |
| 3 | `01b_odds.py` | CSV cotes → probas implicites + normalisation équipes | `silver.odds` |
| 4 | `process_events.py` | Normalise l'index WhoScored → enregistre les matchs | `match_registry` + silver (events/compos) |
| 5 | `process_team_stats.py` | Parquet FBref/Understat/WhoScored → silver | `silver.fbref_*`, `silver.understat_*`, `silver.whoscored_team_season` |

> Le couple **`process_team_stats.py` + `process_events.py`** (socle
> `process_common.py`) est la **référence**. L'ancien monolithe `02_process.py`
> a été retiré dans `legacy/` — `run_pipeline` et `run_ingest` pointent tous deux
> sur le refactor.

Contrôle qualité optionnel : `python pipelines/audits/audit_silver.py`
(à lancer **après** `process_team_stats` + `process_events`).

---

## 3. Aval — après `ingest`

Propager `silver → gold → marts` via dbt (+ KNN + xGOT) :

```bash
python pipelines/run_pipeline.py --tables      # bloc transform seul
# ou le flux quotidien complet (transform + prédiction) :
python pipelines/run_pipeline.py --flow daily
```

---

## Cas d'usage fréquent — « j'ai scrapé des matchs, les mettre en silver »

```bash
python pipelines/ingest/run_scrapping.py --step load_archive   # JSON → staging silver
python pipelines/ingest/process_events.py                      # normalise l'index → match_registry
python pipelines/run_pipeline.py --tables                      # silver → gold → marts
```

Tout est **idempotent** (DELETE + INSERT) — relancer un match remplace proprement
ses données, aucune duplication.
