# Concepts — Session du 16 août 2026 (diagnostic KNN)

> Thème : **diagnostic données préalable au chantier KNN** (famille 11, features
> 76 cluster offensif, 77 cluster défensif, 78 imputation KNN). Objectif : figer
> sur données réelles (`db/football.duckdb`, 65 Go, ouvert en **read-only**) le
> **seuil d'imputation**, le **nombre de clusters** et l'**inventaire des
> dimensions** avant d'écrire la moindre ligne de code KNN.
> Script reproductible : `pipelines/diagnostics/knn_diagnostic.py`.

---

## 0. Retournement clé — les 2 features « manquantes » existaient déjà

La passation supposait qu'il fallait **construire** `scorer_xgot_overperformance_lag`
(espace 1) et `def_threat_conceded_per90_lag` (espaces 6 et 7). Vérification dans
le code réel : **les deux colonnes sont déjà entièrement codées** dans
`dbt_project/models/gold/joueur_saison.sql` (bloc PASSE 2, lignes 159 et 162-163),
et **toutes** leurs dépendances existent (`machine_learning.xgot_predictions`
= 137 523 lignes, `intermediate.threat_conceded`, `int_shot_placement`, etc.).

Cause de leur absence dans la table matérialisée : `joueur_saison` est
`materialized='incremental'`. Le bloc PASSE 2 a été ajouté **après** la dernière
matérialisation, et un run incrémental ne retraite que `date > MAX(date)` — or la
table est déjà à jour au 2025-05-25, donc **zéro ligne recalculée**.

**Correctif appliqué** (pas de nouveau code) :

```powershell
cd dbt_project
dbt run --select joueur_saison --full-refresh
```

Après full-refresh, les 2 colonnes sont peuplées :
`def_threat_conceded_per90_lag` **98,2 %** non-null (méd. 0,46), 
`scorer_xgot_overperformance_lag` **82,3 %** non-null (méd. −0,06, NULL quand aucun
tir cadré sur la fenêtre). **Les 7 espaces de similarité sont désormais complets.**

> Leçon : sur une table `incremental`, ajouter une colonne au modèle ne la
> rétro-remplit pas. Toujours `--full-refresh` après un ajout de colonne
> historique.

---

## 1. Schéma confirmé (grain + anti-leakage)

| Table | Lignes | Grain | Décalage passé |
|---|---|---|---|
| `gold.joueur_saison` | 492 759 | `(match_id, team_id, player_id, date)` | fenêtre as-of 38 apparitions **antérieures** (match courant exclu) |
| `gold.joueur_zone_saison` | 639 525 | `(player_id, zone_5x5, season)` | profil porté par la saison N-1 |

- `zone_5x5` = **25 cellules** `z{1..5}_c{1..5}` (z = profondeur, c = couloir ;
  c3 = axe, c1/c5 = flancs, z4/z5 = zones offensives). Cohérent avec les
  références de cellules de la passation.
- **Position (source du cluster 76)** : `intermediate.int_whoscored_lineup`,
  `grid_horizontal` ∈ [1,9], `grid_vertical` ∈ [0,9], **100 % non-null** sur
  1,76 M lignes → agrégeable en position moyenne par joueur (gardiens = `gv≈0`).

---

## 2. Complétion — dimensions faibles repérées

Colonnes `joueur_saison` denses (~98 % non-null) sauf `off_xg_per_shot_lag`
(89,7 %, indéfini si 0 tir). Colonnes zonales ~60 % non-null (normal : un joueur
n'agit pas dans toutes les zones). **Deux dimensions faibles à surveiller** :

- `def_errors_per90_lag` : 98 % non-null mais **60 % de zéros** (peu de joueurs
  font des erreurs) → axe de similarité peu discriminant pour l'espace 7.
- `def_duel_win_rate_by_zone_lag` : **38,5 %** non-null seulement, médiane de
  **1 duel** par joueur-zone → peu fiable telle quelle pour l'espace 5.

`profile_confidence_flag = 'none'` couvre **40 %** des lignes zonales : ce sont
les cibles principales de l'imputation.

---

## 3. Seuil d'imputation — fiabilité split-half du profil

Méthode CDC : pour k matchs, corréler le profil des k premiers matchs d'un joueur
vs les k suivants (corrélation inter-joueurs = reproductibilité). Coude = seuil.

| k matchs | n joueurs | r moyen | fiabilité 2k (Spearman-Brown) |
|---:|---:|---:|---:|
| 3 | 6 927 | 0,34 | 0,50 |
| 5 | 6 398 | 0,50 | 0,67 |
| **8** | 5 732 | **0,64** | 0,78 |
| **10** | 5 387 | **0,70** | 0,82 |
| 12 | 5 047 | 0,73 | 0,84 |
| 15 | 4 562 | 0,78 | 0,88 |
| 20 | 3 905 | 0,82 | 0,90 |
| 30 | 2 978 | 0,86 | 0,93 |

**Lecture** : coude net à **k≈8-10**. Sous 8 matchs → profil non reproductible
(r<0,65). À 10 matchs → seuil de fiabilité acceptable (r≈0,70). Au-delà de 20,
gains marginaux. Feature la plus lente : duels aériens (r=0,57 à k=10).

**Décision — seuil = 10 observations** (au-dessus = fiable, en-dessous = imputer),
puis Optuna/MLflow teste {10, 15, 20} autour.

> Le `profile_confidence_flag` actuel (`high`≥20, `medium`≥5) est mal calibré :
> `medium`=5 ↔ r≈0,50 (trop permissif). Recalibrage recommandé (secondaire) :
> `high`≥20, `medium`≥10, `low`<10.

---

## 4. Nombre de clusters de style (76/77)

KMeans standardisé sur le profil courant par joueur (≥10 apps, gardiens exclus),
silhouette k=2..8. Silhouette plafonne à ~0,31-0,33 → **le style est un
continuum**, pas des groupes nets ; on choisit k pour l'utilité fonctionnelle
(pré-restreindre le vivier de voisins), pas pour un pic statistique.

| côté | k=2 | k=3 | k=4 | décision |
|---|---:|---:|---:|---|
| Offensif (profil) | 0,31 | 0,29 | 0,23 | **k=3** |
| Offensif (profil+pos) | 0,30 | 0,26 | 0,22 | position gardée (design 76) |
| Défensif (profil) | 0,32 | 0,30 | 0,26 | **k=3** |
| Défensif (profil+pos) | 0,33 | 0,31 | 0,28 | position améliore la séparation |

**Décision — 3 clusters par côté**, features = profil + position. k=2 trop
grossier ; k≥4 fait chuter la silhouette et réduit trop les viviers de repli.
À k=3, clusters ≈ 2000 joueurs → repli fiable. Position légèrement contre-
productive côté offensif (0,29→0,26) mais gardée car c'est la source désignée du
cluster 76 et elle a du sens fonctionnel.

---

## 5. Paramètres figés pour coder 76 → 77 → 78

- **Seuil de confiance profil** : **10 observations** (recalibrer le flag ensuite).
- **K voisins** : hyperparamètre Optuna/MLflow, plage **{10, 15, 20}**.
- **Clusters** : **3 offensifs + 3 défensifs**, KMeans standardisé sur
  profil + position (`gv`, `width = |gh−5|`), gardiens exclus (`gv ≤ 0,3`).
- **Espaces** : les **7 validés**, tous peuplés après full-refresh.
- **Rôle des clusters** (rappel conception) : (a) pré-restreindre le vivier au
  même cluster de style, (b) repli quand l'espace action-spécifique manque de
  voisins. **Jamais en entrée directe du modèle.**
- **Dimensions à traiter avec prudence** : `def_errors_per90_lag` (60 % zéros) et
  `def_duel_win_rate_by_zone_lag` (38,5 % non-null) → pondération réduite ou
  exclusion à tester pendant le build.

---

## 6. Prochaine étape

Coder dans l'ordre **76 (cluster offensif) → 77 (cluster défensif) → 78 (KNN)**,
en script pipeline autonome `pipelines/03_knn_impute.py` (scikit-learn), inséré
comme étape Prefect, écrivant une table consommée en `source` par dbt/ML. Tuning
K + seuil via Optuna/MLflow (cohérent avec `04_train.py`).
