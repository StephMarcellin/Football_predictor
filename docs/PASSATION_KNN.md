# Passation — Famille 7 finie → Famille 11 (KNN) à réaliser

## État du pipeline Gold

Tout le Gold est construit et **vert**, sauf la famille 11. Les deux derniers builds validés :
- `dbt build --select int_player_zone_season+ --full-refresh` (features 37, 41, 55 + densité déf.)
- `dbt build --select equipe_confrontation_match --full-refresh` (feature 56 + xgbuildup)

Familles 1→10 : terminées. **Seule la famille 11 (KNN + clusters, features 76-79) reste à faire.**

---

## ⟪ MISE À JOUR — Session 16 août 2026 : diagnostic données FAIT ⟫

> Détail complet + tables de résultats : `docs/Concepts_Session_2026-08-16_KNN.md`.
> Script reproductible (read-only) : `pipelines/diagnostics/knn_diagnostic.py`.

**Correctif appliqué** : les 2 features supposées « à construire »
(`scorer_xgot_overperformance_lag`, `def_threat_conceded_per90_lag`) étaient
**déjà codées** dans `joueur_saison.sql` (PASSE 2) mais absentes de la table
`incremental`. Résolu par `dbt run --select joueur_saison --full-refresh`. Les 2
colonnes sont peuplées (98,2 % / 82,3 % non-null). **Les 7 espaces sont complets.**

**Paramètres FIGÉS (les points « à trancher » plus bas sont résolus) :**
- **Seuil d'imputation = 10 observations** (fiabilité split-half r≈0,70 au coude
  k≈8-10). Optuna teste {10, 15, 20}.
- **Clusters = 3 par côté** (offensif + défensif), KMeans standardisé sur
  profil + position (`gv`, `width=|gh−5|`), gardiens exclus (`gv≤0,3`). Style =
  continuum (silhouette ~0,3), k=3 = compromis séparation / taille de vivier.
- **K voisins** : plage Optuna {10, 15, 20}.
- **Position (cluster 76)** : `int_whoscored_lineup.grid_horizontal/grid_vertical`,
  100 % non-null → confirmée.
- **Dimensions faibles** à pondérer/exclure au build : `def_errors_per90_lag`
  (60 % zéros), `def_duel_win_rate_by_zone_lag` (38,5 % non-null, méd. 1 duel).
- **À recaler (secondaire)** : `profile_confidence_flag` → `high`≥20, `medium`≥10,
  `low`<10 (le `medium`≥5 actuel ↔ r≈0,50, trop permissif).

**Prochaine étape = CODER 76 → 77 → 78** dans `pipelines/03_knn_impute.py`
(scikit-learn, étape Prefect, table écrite en `source` pour dbt/ML ; tuning
Optuna/MLflow).

---

## À RÉALISER — Famille 11 : imputation par voisins (KNN) + clusters de style

**Principe (acté)** : profil individuel quand les données suffisent ; imputation par K-NN sur joueurs similaires SEULEMENT quand elles manquent. Ne jamais clusteriser par défaut (ça lisserait les joueurs et détruirait la spécificité recherchée). Le cluster sert UNIQUEMENT à imputer, jamais comme entrée directe.

**Décision structurante** : la similarité n'est PAS unique. Plusieurs espaces de similarité selon l'action modélisée (« finition », « création par le centre », « défense de couloir »…), pas des stats globales type buts+passes.

**Features à produire :**
- **76 `player_style_cluster_offensive`** — cluster de style offensif (position, zones de touche, type d'actions créatrices). Base d'imputation des profils offensifs manquants (familles 5, 10). Dimensions 5×5.
- **77 `player_style_cluster_defensive`** — cluster de style défensif (zones d'action déf., type de duels, aérien). Base d'imputation des profils défensifs (famille 6).
- **78 `knn_imputed_profile`** — MÉCANISME, pas une colonne : valeur imputée = moyenne des K voisins les plus similaires dans l'espace pertinent, pour une case/feature où le joueur manque de données.
- **79 `profile_confidence_flag`** — DÉJÀ CONSTRUIT dans `joueur_saison` et `joueur_zone_saison` (chaque profil sait sur combien d'observations il repose : none/low/medium/high).

**Méthode du seuil d'imputation (CDC, en 4 étapes — ne pas deviner) :**
1. Repousser la décision : les profils sont déjà construits avec leur compteur (`profile_confidence_flag`). Le seuil n'intervient qu'au moment « j'impute ou pas ».
2. Regarder la distribution : tracer le nb d'observations par joueur-zone → masse de joueurs fournis + traîne de joueurs pauvres.
3. Mesurer la stabilité (vraie méthode) : pour les joueurs riches en données, calculer le profil sur la 1ʳᵉ moitié vs la 2ᵉ moitié de leurs matchs ; le seuil = le point où les deux moitiés convergent (stat reproductible, mesurable).
4. Laisser le modèle valider : le seuil est un hyperparamètre → tester 10/20/30 via Optuna/MLflow, garder le meilleur en validation.

**Décisions à trancher AVEC Stéphane (calibration, ne pas faire en autonomie) :**
- Liste exacte des dimensions de clustering + nombre de clusters, par espace (offensif / défensif).
- Espaces de similarité à créer (finition, création centrale, défense de couloir…).
- Seuil de déclenchement de l'imputation (via la méthode ci-dessus).
- Choix de K.

---

## RÉSERVES & NOTES (à connaître pour la suite)

**À valider à l'entraînement (SHAP/importance MLflow) :**
- **Sens des ratios** `matchup_high_press_vs_buildup` (56) et `matchup_central_control` (55) : sémantique subtile. Les composantes brutes crossées sont exposées à côté du ratio → si le ratio est mal orienté, le modèle récupère le signal dans les colonnes brutes. Rien à construire, se lève au 1ᵉʳ train.

**Versions réduites (composantes CDC absentes) :**
- **53 `matchup_cross_threat`** : composante aérienne (`def_aerial_win_rate`, `def_cross_conceded_by_flank`, famille 6) NON incluse → TODO. Livré = cross_strength × (1 − def_solidity).
- **55 `matchup_central_control`** : `def_threat_conceded_by_zone` IMPOSSIBLE (`threat_conceded` sans coordonnées). Option a retenue = densité défensive = actions défensives seules.

**Défauts de source à corriger en amont (non bloquants) :**
- `equipe_match` : 2 lignes à `match_id`/`team_id` NULL (garde `WHERE match_id IS NOT NULL` manquant dans le modèle producteur). Exclues dans `equipe_confrontation_match`.
- `h2h_history` : off-by-one sur ~3% des lignes, 14.8% NULL.
- `backbone` : `opponent_id`/`team_id` NULL sur D2/coupes (hors scope modélisation).
- `np_xg` : Understat re-scrapé → Big5 0% NULL 2017-2025. NULL restants = coupes/Europe/D2/saison en cours ; inoffensif car rolling partitionné par `league_source`.

**Chantiers ouverts (hors Gold) :**
- `config/data_rules.yml` créé ; `check_data_rules(con, rules_path)` fourni → reste à câbler dans `run_quality_check(con)` de `pipelines/process_common.py`.
- Bug chemin `process_common.py` : `DB_PATH`/`RAW_DIR` (lignes ~22-23) relatifs → DB parasite si lancé depuis `pipelines/`. Fix : préfixer `ROOT_DIR`.
- `.env` : `GCS_BUCKET_NAME` commenté → upload GCS sauté (non bloquant).
- `team_mapping` : IDs assignés à la main. Orthographe source parfois absente des alias (ex. Ulm). Idée validée : scraper Transfermarkt → seed → boucle de réconciliation fuzzy auto (`thefuzz` déjà importé dans `01_ingest.py`).
- Skill `data-map` installé (cartographie Obsidian des tables).

---

## Nouvelles colonnes/tables de cette session (pour référence)

- `int_player_zone_season` : `+total_progressive`, `+total_crosses`, `+total_def_actions`
- `joueur_zone_saison` : `+off_progressive_actions_by_zone_lag` (41), `+off_cross_volume_by_zone_lag` (37, centre GÉOMÉTRIQUE — pas `is_creative`), `+def_actions_by_zone_lag`, `+off_danger_by_zone_lag`
- `team_corridor_profile` : `+off_cross_strength`, `+off_dribble_strength`, `+off_central_progression`, `+off_central_touch`, `+def_central_density`
- `zone_confrontation_match` : `+matchup_cross_threat` (53), `+matchup_dribble_threat` (54), `+matchup_central_control` (55, ligne axe seulement) + composantes brutes
- `equipe_confrontation_match` : **NOUVELLE TABLE** — `matchup_high_press_vs_buildup_rolling_{3,5,10}` (56) + `opp_press_ppda`, `self_buildup_resistance`, `self_team_xgbuildup_lag`, `opp_team_xgbuildup_lag`

**Convention centre géométrique** (feature 37) : un centre = passe partant du dernier tiers sur un flanc (`x≥66 ET (y<20 OU y>80)`) vers la surface centrale (`end_x≥83 ET end_y∈[21,79]`), comptée dans la cellule d'origine. `is_creative` écarté (piquait au centre c3, pas sur les flancs).
