# Passation — Session du 22 juillet 2026

> **À coller en premier message d'un nouveau Cowork pour reprendre le contexte.**
> Thème de la session : refonte du scraping WhoScored (archivage + découplage),
> orchestration Prefect nocturne, et extraction des données joueurs / formations /
> équipe / méta dans les couches silver et intermediate.

---

## 1. État git

- **Branche courante** : `feature/whoscored-match-facts` (2 commits non poussés → `git push` à faire).
- **Branche sœur** : `feature/player_reference` (tables de référence — déjà commitée dessus).
- La branche `feature/whoscored-match-facts` **descend de** `feature/player_reference` : elle contient donc dims + faits + modèles intermediate.

Commits clés de la session (du plus ancien au plus récent) :

```
82cb8d3 feat: archive brute gzip + events enrichis + mode raw-only/loader WhoScored
11d0373 feat: orchestrateur Prefect scraping nocturne (run_scrapping raw-only + load)
ee63049 feat: orchestrateur Prefect scraping nocturne + timezone + fix pickling loguru
875874d feat: tables de référence WhoScored (players_ref, formations_ref)
b2406d7 feat: tables de fait WhoScored (player_match, formations, team_match, match_meta)
1855eb6 docs: déclarer les 6 tables sources WhoScored (dims + faits)
c7e058c feat: int_whoscored_player_match + sources WhoScored (dims + faits)
8889d0b feat: int_whoscored_formations, team_match, players (dim transferts)
```

**À faire** : `git push` de `feature/whoscored-match-facts`, puis décider de la stratégie de merge vers `main` (player_reference d'abord, puis facts, ou merge direct de facts qui contient tout).

---

## 2. Ce qui a été réalisé

### 2.1 Refonte du scraping — découplage fetch / parse

Avant : `scrape_whoscored_details.py` écrivait directement les events en base, **aucune sauvegarde du JSON brut**. Re-récupérer un champ oublié imposait de re-scraper.

Maintenant :
- Le scraper **archive chaque match en JSON gzip** (`data/raw/whoscored/{saison}/{ligue}/{ws_id}.json.gz`) — ratio de compression ~22x. L'objet archivé est le `matchCentreData` complet (events + stats équipe + joueurs + formations + méta).
- Fonction `save_raw_json`, appelée **avant** le parsing (le brut est gardé même si le parsing plante).
- Chemin d'archive piloté par `config.yaml` → `paths.whoscored_raw` (mettre un chemin absolu type `E:/...` pour un disque externe).

### 2.2 Enrichissement des events

9 colonnes ajoutées à `silver.stg_whoscored_events` (avec migration auto `migrate_events_table`) :
`is_goal`, `is_own_goal`, `related_event_id`, `related_player_id`, `card_type`,
`goal_mouth_y`, `goal_mouth_z`, `blocked_x`, `blocked_y`.
Correction au passage : priorité d'extraction `matchCentreData` (objet complet) avant `allStats`.

### 2.3 Mode `--raw-only` + loader découplé

- `scrape_whoscored_details.py --raw-only` : n'écrit **que** les archives gzip, **aucune écriture DuckDB** (DBeaver reste libre pendant le scraping). Reprise sur disque (saute les `.json.gz` déjà présents + marqueurs `_skipped/`). Flag `--max-runtime N` pour borner la fenêtre nocturne.
- `pipelines/scrapping/load_whoscored_archive.py` : relit les archives et remplit DuckDB (events + index + 6 nouvelles tables). À lancer **DBeaver fermé**.
- `pipelines/scrapping/whoscored_entities.py` : schémas + parse + upsert des 6 nouvelles tables.
- Optimisation vitesse : `block_images=True` sur le driver (sûr, pas de risque de ban).

### 2.4 Orchestrateur Prefect nocturne

- `pipelines/run_scrapping.py` : flow `scrape_raw` (raw-only, borné) → `load_archive`, calqué sur `run_pipeline.py`.
- Config dans `config.yaml` → section `scraping` (`cron`, `timezone: Europe/Paris`, `max_runtime_min`, `deployment_name`).
- Backend = **Prefect Cloud** (pas de serveur local). Lancement : `python pipelines/run_scrapping.py --serve` (process bloquant, PC allumé + veille désactivée).

### 2.5 Nouvelles tables silver (6)

| Table | Grain | Contenu |
|---|---|---|
| `stg_whoscored_players_ref` | joueur | player_id → nom (dimension) |
| `stg_whoscored_formations_ref` | formation | formation_id → nom (ex: 2→442) |
| `stg_whoscored_player_match` | joueur × match | note WhoScored + `stats_json` brut |
| `stg_whoscored_formations` | formation × équipe × match | timeline tactique |
| `stg_whoscored_team_match` | équipe × match | 35 stats officielles (`stats_json`) |
| `stg_whoscored_match_meta` | match | arbitre, stade, affluence, scores |

Toutes déclarées dans `dbt_project/models/sources.yml`.

### 2.6 Nouveaux modèles intermediate (4)

Tous normalisent le `team_id` WhoScored → `team_id` canonique et rattachent le `match_id` unifié via `int_whoscored_match_index` (même patron que `int_whoscored_events`).

| Modèle | Rôle | Lignes (backfill partiel) |
|---|---|---|
| `int_whoscored_player_match` | note + stats par joueur/match | 52 474 |
| `int_whoscored_formations` | timeline tactique | 11 675 |
| `int_whoscored_team_match` | stats équipe (`stats_json` conservé) | 2 774 |
| `int_whoscored_players` | dimension (player_id, saison, team_id) — gère les transferts | 3 924 |

Validés : 0 % de NULL `match_id`, 0 doublon de clé, transferts capturés (ex : Federico Ricci = 3 équipes en 2017-2018).

---

## 3. Décisions & apprentissages clés

- **Découplage fetch/parse** : le scrape produit du JSON brut archivé, le loader le transforme. On peut ré-enrichir les tables à tout moment sans re-scraper.
- **Champ `age` = âge au SCRAPE, pas au match** (WhoScored renvoie l'âge courant 2026 pour des matchs 2017). Donc **pas d'âge « par saison »** dans la dimension joueur. height/weight = profil courant (~stable), team_id/saison = historique correct.
- **Normalisation du `team_id`** : l'id WhoScored (ex: 167) ≠ ton `team_id` canonique (ex: 202606100248). La seed `team_mapping` porte l'id canonique. On convertit via le pont `int_whoscored_match_index`.
- **Ordre du pipeline critique** : après le `load_archive`, il faut **`02_process` PUIS dbt**. `02_process` normalise les noms d'équipes de `stg_whoscored_match_index` (le loader écrit les noms bruts courts « PSG », « Monaco »… qui sont dans la colonne `alias`, pas `club_name`). Sans ça, le pont échoue et `match_id` = NULL. `run_pipeline.py` respecte cet ordre ; en standalone, y penser.
- **Bug Prefect `.serve()` + loguru** : le flow planifié doit être **au niveau module** (pas dans `main`) ET le sink fichier loguru ne doit être ajouté qu'à l'exécution (`_ensure_file_log`), sinon cloudpickle échoue (`Cannot pickle files ... : a`) car il embarque le fichier de log ouvert.
- **Fuseau cron** : sans `timezone` explicite, Prefect interprète le cron en UTC. On a épinglé `Europe/Paris`.

---

## 4. Workflow opérationnel

**Backfill (en cours, ~7 nuits)** — pour relancer / réouvrir des matchs :
```sql
-- Rouvrir les matchs à (re)scraper (garde paywall/no_data exclus)
UPDATE silver.stg_whoscored_urls SET is_scraped = FALSE WHERE skip_reason IS NULL;
```

**Chaîne complète après un scrape** (DBeaver fermé) :
```powershell
python pipelines/scrapping/load_whoscored_archive.py   # archives → silver
python pipelines/02_process.py                          # normalise noms + registry
dbt run  --select +int_whoscored_player_match int_whoscored_formations int_whoscored_team_match int_whoscored_players
dbt test --select int_whoscored_player_match int_whoscored_formations int_whoscored_team_match int_whoscored_players
```

**Nocturne automatique** :
```powershell
python pipelines/run_scrapping.py --serve   # à laisser tourner, PC allumé
```

**Points de vigilance** :
- DBeaver verrouille la base : le fermer avant tout `load_archive` / `02_process` / `dbt run`.
- Config prod : `scraping.cron = "0 23 * * *"`, `max_runtime_min = 480` (vérifier qu'on n'est pas resté sur des valeurs de test).
- Le PC doit rester allumé + veille désactivée sur la plage 23h-8h.

---

## 5. Reste à faire (roadmap)

Par priorité :

1. **Exploiter les nouveaux champs events** dans `int_event_enriched` / `event_values` :
   `goal_mouth_*` + `blocked_*` (proxy xG / analyse gardien), `related_player_id` (passeurs réels), `card_type`.
2. **Extraire les totaux depuis `stats_json`** (team_match puis player_match) en gold — **vérifier d'abord** si les séries par minute sont cumulatives ou incrémentales.
3. **Brancher la note joueur WhoScored → squad features** d'équipe en gold (feature ML forte).
4. Laisser tourner le **backfill nocturne** jusqu'à couverture complète des 8 saisons.
5. `git push` + stratégie de merge des branches WhoScored vers `main`.

Abandonné volontairement : `int_whoscored_match_meta` (scores = leakage, arbitre/affluence = valeur trop faible pour l'instant).

---

## 6. Fichiers touchés cette session

- `config.yaml` (paths.whoscored_raw, section scraping)
- `pipelines/scrapping/scrape_whoscored_details.py` (archive, events enrichis, raw-only, max-runtime, fix upsert_match_index)
- `pipelines/scrapping/load_whoscored_archive.py` (loader + wiring dims/faits)
- `pipelines/scrapping/whoscored_entities.py` (nouveau — 6 tables)
- `pipelines/run_scrapping.py` (nouveau — orchestrateur Prefect)
- `dbt_project/models/sources.yml` (6 sources WhoScored)
- `dbt_project/models/intermediate/schema.yml` (descriptions + 4 modèles int_whoscored_*)
- `dbt_project/models/intermediate/int_whoscored_player_match.sql` (nouveau)
- `dbt_project/models/intermediate/int_whoscored_formations.sql` (nouveau)
- `dbt_project/models/intermediate/int_whoscored_team_match.sql` (nouveau)
- `dbt_project/models/intermediate/int_whoscored_players.sql` (nouveau)
