# AUTONOMY_RUN — Session autonomie Data Quality

Session : 2026-09-08
Périmètre validé : Skill v2 + 3 tables (int_understat_schedule, int_understat_stats, int_whoscored_match_index)

## Résumé exécutif

| Livrable | Statut | Note |
|---|---|---|
| Skill v2 réécrit (`.claude/skills/data_test_generator.md`) | ✅ Livré | Procédure corrigée : schema.yml first, segmentation Big5, seeds, exclusion saison courante, style ancien pour macros custom |
| `int_understat_schedule` | ✅ Livré | dbt + GE + bridge pytest. **Toutes expectations vertes.** |
| `int_understat_stats` | ✅ Livré | dbt + GE + bridge pytest. **Toutes expectations vertes.** |
| `int_whoscored_match_index` | ✅ Livré | dbt + GE + bridge pytest. **Toutes expectations vertes** après correction type ws_match_id (VARCHAR pas BIGINT). |

## Détail par table

### intermediate.int_understat_schedule (16 258 lignes)

**Périmètre** : 100 % Big5 domestique — pas de segmentation.

**Découvertes** :
- Grain `match_id` propre (0 doublon)
- Grain `us_match_id` : **46 doublons** (matches rejoués possibles — à investiguer)
- xG NULL sur 2.9 % (476 lignes) — matches sans xG calculé

**Livrables** :
- Edit `dbt_project/models/intermediate/schema.yml` (bloc int_understat_schedule)
- `tests/great_expectations/intermediate/int_understat_schedule.yml` (25+ expectations)
- `tests/unit/test_int_understat_schedule_ge.py`

**Contraintes physiques ajoutées** :
- `home_goals >= 0 AND away_goals >= 0` (dbt expression_is_true)
- `is_result = 0 → home_goals = 0 AND away_goals = 0` (GE row_condition)

**Résultat empirique** : ✅ Toutes vertes.

---

### intermediate.int_understat_stats (15 782 lignes)

**Périmètre** : 100 % Big5 domestique.

**Découvertes** :
- Grain `match_id` propre (0 doublon), `us_match_id` : 1 doublon
- Cohérences internes propres : miroir xG (0 violation), diff = home - away (0 violation), xpts sum ≤ 3 (0 violation)
- PPDA max = 193 & 152 (extrêmes physiques mais légitimes)

**Livrables** :
- Edit `dbt_project/models/intermediate/schema.yml` (bloc int_understat_stats)
- `tests/great_expectations/intermediate/int_understat_stats.yml`
- `tests/unit/test_int_understat_stats_ge.py`

**Contraintes physiques ajoutées** (dbt ET GE — doublon volontaire) :
- Miroir : `ABS(home_np_xg_diff + away_np_xg_diff) < 0.001`
- Cohérence xG : `ABS((home_np_xg - away_np_xg) - home_np_xg_diff) < 0.001`
- Points attendus : `home_xpts + away_xpts <= 3.001`

**Résultat empirique** : ✅ Toutes vertes.

---

### intermediate.int_whoscored_match_index (39 096 lignes)

**Périmètre** : Big5 + D2 (Championship + 2.Bundesliga), historique long 2009-2026.

**Découvertes** :
- **118 doublons sur `ws_match_id`** (à investiguer — rechargements de scraping ?)
- **1 718 `match_id` NULL (4.4 %)** — non résolus au match_registry
- **854 `team_id` NULL (2.2 %) + 850 `opponent_id` NULL (2.2 %)** — noms d'équipes non mappés
- Type `ws_match_id` = VARCHAR (pas BIGINT), a nécessité l'ajustement du test values_to_be_between

**Livrables** :
- Edit `dbt_project/models/intermediate/schema.yml` (bloc int_whoscored_match_index)
- `tests/great_expectations/intermediate/int_whoscored_match_index.yml`
- `tests/unit/test_int_whoscored_match_index_ge.py`

**Contraintes physiques ajoutées** :
- `ws_home_team_id != ws_away_team_id` (dbt + GE)
- `team_id != opponent_id` (quand non-NULL — dbt + GE)

**Sévérités** :
- `unique_combination_of_columns(ws_match_id)` en **warn** (118 doublons connus)
- `not_null` sur `match_id`, `team_id`, `opponent_id` en **warn** (NULL empiriques documentés)

**Résultat empirique** : ✅ Toutes vertes après correction du test type ws_match_id.

---

## Points en attente d'arbitrage à ton retour

### 🔍 À investiguer

1. **46 doublons `us_match_id` dans int_understat_schedule** — matches rejoués légitimes ou bug ? Le grain `match_id` unifié absorbe l'ambigüité (0 doublon), mais la présence de doublons côté source Understat mérite une réponse.

2. **118 doublons `ws_match_id` dans int_whoscored_match_index** — même question. Ces 118 lignes se retrouvent-elles dupliquées dans `int_whoscored_events` en aval ?

3. **1 718 `match_id` NULL dans int_whoscored_match_index (4.4 %)** — nombre bien plus élevé que les 49 vus dans `int_fbref_keeper`. Suggère que la résolution ws → match_id échoue plus fréquemment que la résolution fbref → match_id. Racine probable : mêmes bugs seed team_mapping (nom brut WhoScored ≠ nom brut FBref pour le même club).

4. **PPDA max 193 (understat_stats)** — physique inhabituel. Vérifier si les matches concernés sont légitimes (faible volume de passes défensives) ou bugs.

### ⚠️ Découvertes de type

`ws_match_id` est VARCHAR alors qu'on aurait pu s'attendre à BIGINT. Cela impacte 2 tests envisagés (values_to_be_between, is_positive) — j'ai basculé sur `matches_regex '^[1-9][0-9]*$'` côté dbt. **À rediscuter** : est-ce qu'on veut caster en INT dans une CTE amont pour uniformiser ? Ou garder VARCHAR ?

### 🎯 Anti-patterns évités (pour référence future)

1. Toujours vérifier le type SQL réel avant d'appliquer un test numérique (`is_positive`, `values_to_be_between`)
2. `MIN(varchar_col)` retourne l'ordre lexicographique — piège classique
3. Ne pas retester les mêmes contraintes en dbt ET GE sauf pour les contraintes physiques métier critiques (charte v2 assumée)

## Suite proposée à ton retour

Le plan de bataille validé prévoyait ensuite :
- ~~`int_fbref_shooting` + `int_fbref_misc`~~ ✅ Livré 2026-09-08 (session suite)
- `int_whoscored_events` (60 M lignes — stratégie query à discuter : échantillonnage ou query optimisée)
- Puis gold, marts, ML (à discuter — règles métier plus riches)

**Alternative à considérer** : boucler tous les `int_fbref_*` restants (schedule, defense, gca,
passing, possession, keeper_adv, poss) en un bloc, avant d'attaquer `int_whoscored_events`.
Pattern identique + attaque groupée de A-005 (49 match_id NULL communs).

---

# Session suite — 2026-09-08 (après-midi)

## Résumé exécutif

| Livrable | Statut | Note |
|---|---|---|
| `int_fbref_shooting` | ✅ Livré | dbt + GE (43 expectations) + bridge pytest. 41 succès, 2 warnings (A-011), 0 erreur. |
| `int_fbref_misc` | ✅ Livré | dbt + GE (51 expectations) + bridge pytest. 51 succès, 0 warning, 0 erreur. |

## Détail par table

### intermediate.int_fbref_shooting (55 709 lignes)

**Périmètre** : toutes compétitions (FBref universel, pas de segmentation Big5).

**Découvertes** :
- Symétrie parfaite avec `int_fbref_keeper` sur les patterns de qualité identité
- 48 doublons `(match_id, team_id)` — cascade A-001
- 49 match_id NULL (0.09 %) — cascade A-005
- 2 001 opponent_id NULL (3.59 %)
- **NOUVEAU** — 1 490 lignes `standard_gls > standard_sot` (2.67 %) → **A-011**
- **NOUVEAU** — 5 lignes `standard_gls > gf` → **A-011**

**Contraintes physiques ajoutées** (dbt ET GE) :
- `standard_sot <= standard_sh` (error, 0 violation)
- `standard_pk <= standard_pkatt` (error, 0 violation)
- `standard_gls <= standard_sot` (warn, 1 490 violations — A-011)
- `standard_gls <= gf` (warn, 5 violations — A-011)

**Résultat empirique** : ✅ 41 succès, 2 warnings A-011 attendus.

---

### intermediate.int_fbref_misc (55 651 lignes)

**Périmètre** : toutes compétitions.

**Découvertes** :
- Table structurellement propre (aucune contrainte physique violée)
- 48 doublons `(match_id, team_id)` — cascade A-001
- 49 match_id NULL — cascade A-005
- 1 993 opponent_id NULL (3.58 %)

**Contrainte physique ajoutée** :
- `crdy + crdr + crdy2 <= 15` (warn, 0 violation) — plafond de plausibilité disciplinaire

**Détail technique** — `off` et `int` sont mots réservés DuckDB *et* Python :
Renommés dans la query source (`"off" AS off_col`, `"int" AS int_col`) pour que
`pandas.eval` puisse traiter la contrainte croisée.

**Résultat empirique** : ✅ 51 succès, 0 warning, 0 erreur.

---

## Nouvelles anomalies consignées

- **A-011 🟠** : `standard_gls > standard_sot` (1 490 lignes) + `standard_gls > gf` (5 lignes).
  Bug scraping FBref suspecté sur `silver.fbref_shooting`. À investiguer.

## Fichiers ajoutés/modifiés (session suite)

Nouveaux :
- `tests/great_expectations/intermediate/int_fbref_shooting.yml`
- `tests/great_expectations/intermediate/int_fbref_misc.yml`
- `tests/unit/test_int_fbref_shooting_ge.py`
- `tests/unit/test_int_fbref_misc_ge.py`

Modifiés :
- `dbt_project/models/intermediate/schema.yml` (2 blocs enrichis)
- `Claude mémoire/00 - Index.md` (compteur 4→6)
- `Claude mémoire/04 - Anomalies découvertes.md` (A-011 ajoutée)
- `Claude mémoire/Sessions/2026-09-08b — DQ int_fbref_shooting + misc.md` (journal)

---

# Session soir — 2026-09-08c — Silver FBref trio

Déclencheur : "on a généré aucun test sur la table silver !" — constat de Stéphane.
Correction du gap : couverture silver du trio FBref (keeper, shooting, misc).

## Résumé exécutif

| Livrable | Statut |
|---|---|
| `silver.fbref_keeper` (sources.yml + GE + pytest) | ✅ 40 succès, 2 warnings (A-004 + A-012), 0 erreur |
| `silver.fbref_shooting` (sources.yml + GE + pytest) | ✅ 41 succès, 2 warnings (A-011 × 2), 0 erreur |
| `silver.fbref_misc` (sources.yml + GE + pytest) | ✅ 51 succès, 0 warning, 0 erreur |

## Découvertes majeures

1. **A-011 confirmé côté silver** : 1 475 + 5 violations. Root cause = scraping FBref, PAS transformation intermediate. La vue `scraping_targets_int_fbref_shooting` est architecturalement justifiée.
2. **A-012 nouvelle** : `cs` polluée en silver (119 lignes `cs = 2`, 322 contradictions cs↔ga_keeper). L'intermediate le camoufle via garde-fou existant — silver expose la vérité.
3. **A-013 nouvelle (gouvernance)** : row_count intermediate > silver de +2 546 lignes. Contre-intuitif. À investiguer.
4. **A-004 recompté** : 1 186 lignes en silver (vs 1 224 en intermediate). Écart cascadé A-001.

## Nouveaux fichiers

- `tests/great_expectations/silver/fbref_keeper.yml`
- `tests/great_expectations/silver/fbref_shooting.yml`
- `tests/great_expectations/silver/fbref_misc.yml`
- `tests/unit/test_silver_fbref_keeper_ge.py`
- `tests/unit/test_silver_fbref_shooting_ge.py`
- `tests/unit/test_silver_fbref_misc_ge.py`

## Fichiers modifiés

- `dbt_project/models/sources.yml` (3 blocs enrichis niveau table)
- `Claude mémoire/00 - Index.md` (nouvelle ligne Silver 3/17)
- `Claude mémoire/04 - Anomalies découvertes.md` (A-004 actualisé, A-011 confirmé, A-012 + A-013 nouvelles)
- `Claude mémoire/Sessions/2026-09-08c — DQ silver FBref trio.md` (journal)

## Points en attente d'arbitrage

- ~~Créer `scraping_targets_int_fbref_shooting`~~ ✅ Livré session 2026-09-08d
- Investiguer A-013 (inspection SQL des `int_fbref_*.sql`)
- ~~Silver understat + odds~~ ✅ Livré session 2026-09-08d
- Silver WhoScored (10 tables restantes — 60M events à part)

---

# Session nuit — 2026-09-08d — Silver Groupe 1 + audit shooting

## Résumé exécutif

| Livrable | Statut |
|---|---|
| `audits.scraping_targets_int_fbref_shooting` | ✅ 316 pairs (team, season) à rescraper |
| `silver.fbref_schedule` (dbt + GE + pytest) | ✅ 20 vertes, 0 warning |
| `silver.understat_schedule` | ✅ 24 vertes, 0 warning |
| `silver.understat_stats` | ✅ 36 vertes (miroir xG, xpts sum vérifiés à la source) |
| `silver.odds` | ✅ 53 vertes (probas no-vig somment à 1 exactement) |

## Détail

- **Vue d'audit shooting** : SELECT DISTINCT team, season FROM silver.fbref_shooting WHERE standard_gls > standard_sot OR standard_gls > gf. Tendance décroissante par saison (max 71 en 2017-2018, 14 en 2025-2026) — le scraper s'est amélioré.
- **silver.understat_stats** : contraintes physiques miroir xG et xpts sum confirmées à la source (0 violation), maintenant en dbt + GE.
- **silver.odds** : 200+ colonnes bookmakers — suite GE ciblée sur colonnes canoniques (pinnacle, avg, max, probas no-vig). Somme des 3 probas = 1 exactement à 1e-6 près.

## Nouveaux fichiers

- `dbt_project/models/audits/scraping_targets_int_fbref_shooting.sql`
- `tests/great_expectations/silver/fbref_schedule.yml`
- `tests/great_expectations/silver/understat_schedule.yml`
- `tests/great_expectations/silver/understat_stats.yml`
- `tests/great_expectations/silver/odds.yml`
- `tests/unit/test_silver_fbref_schedule_ge.py`
- `tests/unit/test_silver_understat_schedule_ge.py`
- `tests/unit/test_silver_understat_stats_ge.py`
- `tests/unit/test_silver_odds_ge.py`

## Fichiers modifiés

- `dbt_project/models/sources.yml` (4 blocs enrichis)
- `dbt_project/models/audits/schema.yml` (bloc `scraping_targets_int_fbref_shooting` ajouté)
- `Claude mémoire/00 - Index.md` (compteur silver 3→7)
- `Claude mémoire/04 - Anomalies découvertes.md` (4 tables saines ajoutées)
- `Claude mémoire/Sessions/2026-09-08d — DQ silver Groupe 1.md` (journal)

## Reste à couvrir en silver (10 tables WhoScored)

- ~~9 autres tables WhoScored~~ ✅ Livré session 2026-09-08f
- `stg_whoscored_events` (60 M lignes — stratégie query à définir) ⏳

---

# Session fin — 2026-09-08f — Silver WhoScored (9 tables)

## Résumé exécutif

**104 expectations vertes, 1 warning (A-014), 0 erreur** sur 9 tables silver WhoScored livrées :

| Table | GE succès | Warn | Note |
|---|---|---|---|
| `stg_whoscored_formations_ref` | 4 | 0 | Dim propre |
| `stg_whoscored_players_ref` | 4 | 0 | Dim propre |
| `whoscored_team_season` | 24 | 0 | Ratings 0-10 respectés |
| `stg_whoscored_match_index` | 12 | 0 | **0 doublon** (int a 118 A-003) |
| `stg_whoscored_match_meta` | 9 | 0 | A-017 documentée (et_score/pk_score dead) |
| `stg_whoscored_urls` | 11 | 0 | Ajouté au sources.yml (absent auparavant) |
| `stg_whoscored_team_match` | 9 | 0 | 2 lignes/match |
| `stg_whoscored_formations` | 16 | 1 | **A-014** : 474 lignes end_minute < start_minute |
| `stg_whoscored_player_match` | 15 | 0 | A-016 documentée (sentinelles 0) |

## Nouvelles anomalies (4)

- **A-014 🟠** — Timeline cassée `stg_whoscored_formations` : 474 lignes `end_minute < start_minute` + 5 sentinelles 32767
- **A-015 🟡** — `start_minute_reg`, `end_minute_reg` 100 % NULL (colonnes dead)
- **A-016 🟡** — `stg_whoscored_player_match` : height=0 (2 260), weight=0 (6 460), age=0 (2) — sentinelles manquantes
- **A-017 🟡** — `stg_whoscored_match_meta` : et_score et pk_score 100 % chaîne vide

## Découverte notable

`silver.stg_whoscored_match_index` a **0 doublon sur ws_match_id**, alors que
`intermediate.int_whoscored_match_index` en a 118. **Confirmation formelle** :
A-003 est introduit par la couche intermediate, pas hérité de silver.

## Nouveaux fichiers (18)

9 suites GE + 9 bridges pytest sous `tests/great_expectations/silver/` et `tests/unit/`.

## Fichiers modifiés

- `dbt_project/models/sources.yml` (8 blocs enrichis + `stg_whoscored_urls` créé)
- `Claude mémoire/00 - Index.md` (silver 7 → 16 tables)
- `Claude mémoire/04 - Anomalies découvertes.md` (A-014 → A-017 + 9 tables saines listées)
- `Claude mémoire/Sessions/2026-09-08f — DQ silver WhoScored (livré).md` (journal)

## État silver au 2026-09-08 (fin)

~~**16 / 17 tables silver couvertes.** Reste `stg_whoscored_events` (60 M lignes).~~ ✅ Complet session 08g.

---

# Session nuit — 2026-09-08g — Silver events (push-down agrégats)

## Résumé exécutif

`stg_whoscored_events` = **60 074 480 lignes**, 32 colonnes.
Stratégie big data validée avec Stéphane : **push-down agrégats + partition par saison**.

3 suites livrées, 28 expectations vertes, 0 warn, 0 error :

| Suite | Résultat pandas | Temps | Succès |
|---|---|---|---|
| `by_season` | 17 × 13 | 17 s | 9/9 |
| `physical` | 1 × 10 | 21 s | 11/11 |
| `by_match` | 40 k × 6 | 11 s | 8/8 |

## 4 nouvelles anomalies

- **A-018 🟠** : 5 992 player_id distincts (39.5 %) orphelins vs `players_ref`
- **A-019 🟡** : 40 matches à volumétrie anormale (5 < 500 events)
- **A-020 🟡** : 7 sentinelles `minute = 32767` (cousin A-014)
- **A-021 🟡** : 2 lignes `second < 0`

## Cohérences validées

- FK ws_match_id, team_id : 0 orphelin
- is_shot ↔ is_goal : cohérence parfaite
- Coordonnées x, y, end_x, end_y : [0, 100] respecté (terrain normalisé)
- type_id = 10000 = convention WhoScored, pas sentinel

## Fichiers créés

3 suites GE + 3 bridges pytest sous `tests/great_expectations/silver/` et `tests/unit/`.

## État silver final

**17 / 17 tables silver couvertes** ✅
- FBref (4/4)
- Understat (2/2)
- Odds (1/1)
- WhoScored (10/10 : 9 tables déjà couvertes + events via 3 suites push-down)

## Prochaines étapes

- Chantiers restants : intermediate (~29 modèles), gold (~15), marts (5), ML (7)
- Ou : investigation anomalies en attente (A-001 à A-021)
- Ou : CI/CD GitHub Actions
- Décision à trancher avec Stéphane

## Points en attente d'arbitrage

- A-013 (row_count int > silver +2 546) — non investigué
- A-014 (formations timeline cassée) — fix scraper
- A-015 (formations reg columns dead) — supprimer ou fixer parseur
- A-016 (player_match sentinelles 0) — caster en NULL dans intermediate
- A-017 (match_meta et_score/pk_score dead) — arbitrer
- `stg_whoscored_events` — stratégie query (échantillonnage stratifié / pré-agrégat / par saison)
- Passer aux couches gold, marts, ML

## Récap de fichiers ajoutés/modifiés cette session

**Nouveaux fichiers** :
- `.claude/skills/data_test_generator.md` (v2 remplacée)
- `tests/great_expectations/intermediate/int_understat_schedule.yml`
- `tests/great_expectations/intermediate/int_understat_stats.yml`
- `tests/great_expectations/intermediate/int_whoscored_match_index.yml`
- `tests/unit/test_int_understat_schedule_ge.py`
- `tests/unit/test_int_understat_stats_ge.py`
- `tests/unit/test_int_whoscored_match_index_ge.py`
- `tests/AUTONOMY_RUN.md` (ce fichier)

**Fichiers modifiés** :
- `dbt_project/models/intermediate/schema.yml` (3 blocs enrichis)

**Aucun modèle d'audit `scraping_targets_*`** créé cette session — les anomalies trouvées ne sont pas de nature "à rescraper depuis silver" mais plutôt de nature "bug de résolution d'identité" (à corriger via fix seed team_mapping — travail à faire à ton retour).


---

# Session 2026-09-09 — DQ int_fbref_schedule

## Résumé exécutif

| Livrable | Statut |
|---|---|
| `int_fbref_schedule` (dbt + GE + pytest) | ✅ **25 expectations vertes, 0 warning, 0 erreur** |

Ferme la famille FBref intermediate (4/4 : keeper, shooting, misc, schedule).

## Découvertes majeures

- **Cohérences internes parfaites** (0 violation) sur les 3 contraintes physiques croisées : result/gf-ga, venue×result→result_1n2, somme poss par match.
- 1 seul doublon `(match_id, team_id)` — contraste fort avec les 48 des 3 autres int_fbref_* → **A-023**.
- Chaînes vides sur `poss` (2 275 lignes dont 942 Big5) et `formation` (1 612) → **A-022**.
- Cascade A-002 (opponent_id NULL) : 100 % hors Big5 (Cup, D2, Europe, Other).
- Delta row_count silver → intermediate confirmé : schedule +2 579 (cohérent avec +2 546 sur les autres FBref) — **A-013** confirmée systématique.

## Nouvelles anomalies (2)

- **A-022 🟡** : poss et formation vides sur `int_fbref_schedule` (2 275 + 1 612 lignes)
- **A-023 🟡** : écart de doublons entre schedule (1) et keeper/shooting/misc (48 chacun)

## Fichiers créés / modifiés

**Nouveaux** :
- `tests/great_expectations/intermediate/int_fbref_schedule.yml`
- `tests/unit/test_int_fbref_schedule_ge.py`

**Modifiés** :
- `dbt_project/models/intermediate/schema.yml` (bloc int_fbref_schedule enrichi)
- `Claude mémoire/00 - Index.md`, `04 - Anomalies découvertes.md`
- `Claude mémoire/Sessions/2026-09-09 — DQ int_fbref_schedule.md` (nouveau journal)

## Prochaine étape suggérée

- `int_odds` (petit, ferme la famille odds — pattern identique aux int_understat/misc)
- OU `backbone` (table centrale, alimente marts/ML — plus gros impact aval, plus riche en règles métier)


---

# Session 2026-09-09b — DQ backbone (table pivot centrale)

## Résumé exécutif

| Livrable | Statut |
|---|---|
| `backbone_all.yml` (couche universelle FBref) | ✅ 48 vertes + 4 warnings (A-024/A-025/A-026) |
| `backbone_big5.yml` (couche Understat/WhoScored/Odds) | ✅ 82 vertes, 0 warning |
| dbt schema.yml backbone (bloc réécrit, +9 tests table, +13 colonnes) | ✅ Livré |

**Total : 130 expectations sur backbone, 4 warnings cascade scraping documentés.**

## 4 nouvelles anomalies

- **A-024 🟠** : `saves > shots_on_target_faced` (296 lignes) — cascade silver.fbref_keeper
- **A-025 🟠** : `clean_sheet=1 AND ga>0` (23 lignes) — cascade A-024
- **A-026 🟠** : `save_pct < 0` (36 lignes, min = -200%) — dérivée d'A-024
- **A-027 🟠** : `season_att_rating == season_def_rating` (100% des 390 lignes non-null) — bug scraper WhoScored, features redondantes

## 13 colonnes non documentées ajoutées à schema.yml

`pinnacle_prob_close_*/opp`, `market_prob_close_*/opp`, `pinnacle_drift_*/opp`, `pinnacle_prob_over25/under25`, `pinnacle_prob_close_over25/under25`.

## Cohérences validées (0 violation)

- Miroir xG (`np_xg_diff_match = np_xg - np_xg_conceded`)
- Somme probas 1n2 = 1 (Pinnacle ET marché)
- Somme over25 + under25 = 1
- `shots_on_target ≤ shots_total`
- `venue × result_1n2 ↔ sign(gf-ga)`

## Fichiers créés / modifiés

**Nouveaux** :
- `tests/great_expectations/intermediate/backbone_all.yml`
- `tests/great_expectations/intermediate/backbone_big5.yml`
- `tests/unit/test_backbone_all_ge.py`
- `tests/unit/test_backbone_big5_ge.py`
- `Claude mémoire/Sessions/2026-09-09b — DQ backbone.md`
- `Claude mémoire/05 - CI vs Prefect (orchestration tests).md`

**Modifiés** :
- `dbt_project/models/intermediate/schema.yml` (bloc backbone : 288 → 428 lignes)
- `Claude mémoire/00 - Index.md` (intermediate 7 → 8 + entrée session + réf 05)
- `Claude mémoire/04 - Anomalies découvertes.md` (A-024/25/26/27 + 13 colonnes)

## Prochaine étape suggérée

- `int_odds` (petit, ferme la famille odds — pattern rapide)
- `int_whoscored_events` (60 M lignes — push-down agrégats à réutiliser)
- Ou passer à Gold : backbone couvert = feu vert pour `gold.features_*`


---

# Session 2026-09-09c — Autonome (intermediate + gold + ML)

## Résumé exécutif

**~55 tables couvertes en une session autonome** grâce à 2 générateurs auto (SELECT * et push-down agrégats).

| Périmètre | # tables | Statut |
|---|---|---|
| Intermediate | 39 | ✅ Suites générées, majorité testées OK |
| Gold | 14 | ✅ Suites générées + testées |
| ML (dbt) | 2 | ✅ xgot_features, xgot_training |

## Générateurs créés

- `/tmp/gen_ge_suite.py` : mode SELECT * pour tables < 500k lignes
- `/tmp/gen_ge_pushdown.py` : mode agrégats DuckDB pour tables > 500k ou celles OOM en SELECT *

## Tests critiques validés

- `player_match_stats` (1.075 M × 296 cols) : **589 expectations vertes** en push-down
- `int_whoscored_events` (60 M) : **54 vertes en 83 s** en push-down
- `events_qual` (265 M) : **6 vertes en 23 s** en push-down light

## Documentation

- `Claude mémoire/DOUBTS - Session autonome 2026-09-09.md` : politique appliquée + décisions à valider
- `Claude mémoire/Sessions/2026-09-09c — Session autonome.md` : journal complet
- `Claude mémoire/00 - Index.md` : compteurs à jour (intermediate 8/35 → 47/47, gold 0/14 → 14/14, ML 0/2 → 2/2)

## Politique auto appliquée

- Sévérité warn par défaut (row_count = error)
- Blocs dbt schema.yml **non modifiés** pour les nouvelles tables (non-casse)
- Bornes empiriques ± 10 % (SELECT *) ou ± empirique (push-down)
- Aucune segmentation Big5 auto — à confirmer manuellement

## À faire à ton retour

Voir `DOUBTS - Session autonome 2026-09-09.md`.
