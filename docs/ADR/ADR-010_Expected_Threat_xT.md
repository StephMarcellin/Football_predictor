# ADR-010 — Expected Threat (xT) : grille Markov estimée en Python, actions de possession centralisées dans `int_xt_actions`

## Statut

Adopté

## Contexte

On veut mesurer la valeur qu'un joueur ajoute en **faisant progresser le ballon** vers des zones dangereuses — combien chaque passe ou conduite augmente la probabilité de marquer bientôt. C'est un signal orthogonal au xG (qui ne récompense que le tir final) : il crédite le meneur qui casse une ligne, pas seulement le buteur.

Le découpage du terrain en grille et le calcul par état sont le modèle **Expected Threat (xT)** de Karun Singh. La donnée disponible : les events WhoScored/Opta, déjà nettoyés en `int_event_enriched`, et les actions atomiques déjà affectées à une grille fine dans `int_xt_actions`.

### Options considérées

| Option | Profil |
|---|---|
| **xT — grille Markov (itération de valeur)** | Étalon global par case, valeur récursive « tirer ou progresser », interprétable |
| Poids de zones manuels | Grille de valeurs choisies à la main — arbitraire, pas de justification data |
| Modèle supervisé (prédire le but à N actions) | Puissant mais opaque, coûteux, difficile à attribuer par action |
| Comptage de passes progressives | Simple, mais binaire et sans notion de valeur (une passe à 30 m vaut autant qu'une passe dans la surface) |

---

## Décision

On adopte le **modèle xT par itération de valeur sur une chaîne de Markov**, avec l'architecture suivante :

1. **`int_xt_actions`** est la **source unique de toutes les actions de possession** — `action_kind` ∈ {`move`, `shot`, `turnover`}. L'affectation `(x,y) → case` (grille 16×12, 192 cases) y est définie **une seule fois** ; les pièces en aval lisent `col/row`, ne recalculent jamais.
2. **`xt_grid.py`** (Python/numpy) estime la grille par itération de valeur et écrit **`machine_learning.xt_grid`** (192 lignes). La table est exposée à dbt comme **source**.
3. **`int_xt_contributions`** (modèle dbt, **matérialisé en table**) joint chaque déplacement à la grille : `xt_added = xT(arrivée) − xT(départ)`, attribué au joueur/équipe.

L'équation résolue par case `c` :

```
xT(c) = shoot%(c)·goalProb(c) + move%(c)·Σ T(c→c')·xT(c')
        avec  shoot% + move% < 1   (le complément = perte de balle, valeur 0)
```

---

## Justification

### Pourquoi une chaîne de Markov (itération de valeur)

La valeur d'une case, c'est ce qu'on peut espérer marquer **bientôt** depuis là. Deux façons de « marquer » : tirer tout de suite (récompense immédiate `shoot%·goalProb`), ou déplacer le ballon vers une meilleure case et récolter *sa* valeur. Comme la valeur d'une case dépend de celle des autres, on résout le système par **itération** : on part de zéro partout, et à chaque passe la valeur des zones dangereuses « remonte » vers les cases qui y mènent, jusqu'à convergence (`max|Δ| < 1e-9`, atteint en ~51 passes).

C'est un **étalon global stable** : la grille est estimée une fois sur toute la donnée (8 saisons, 5 ligues), pas par match. La différence entre joueurs sort dans les **contributions**, pas dans la grille.

### Le terme de perte de balle est essentiel (l'apprentissage clé de la session)

La formulation naïve pose `move% = 1 − shoot%` : à chaque case, soit je tire, soit je déplace le ballon — **mais je ne le perds jamais**. Conséquence : le ballon circule à l'infini jusqu'à un tir, donc depuis n'importe où (même sa propre cage) on finit par remonter et tirer → la valeur devient **quasi identique partout** (~0.106). Grille plate, inutile.

La vraie vie a une **troisième issue** à chaque action : perdre le ballon (passe ratée, dribble raté, contrôle manqué, dispossession), valeur 0. C'est ce terme qui fait **décroître la valeur quand on recule** : depuis sa propre cage 89 % des ballons sont perdus avant d'arriver au but, contre 18 % devant la surface adverse. On l'obtient en mettant au **dénominateur toutes les actions de possession** (tirs + déplacements réussis + pertes), d'où `shoot% + move% < 1`.

> [!warning] Symptôme diagnostique
> Une grille xT **plate** (valeur ≈ constante partout, cage à ~0.10 au lieu de ~0) est le signe qu'on a oublié la perte de balle. Le contrôle : le profil `xT moyen par colonne` doit **croître de la cage (≈0) vers le but adverse**. Avant/après le terme de perte :
> ```
> col  0 (sa cage) :  plat 0.1075   →  corrigé 0.0015
> col 15 (but adv) :  plat 0.1475   →  corrigé 0.0659   (pic, xT max/case 0.26)
> ```

### Pourquoi estimer la grille en Python (et pas en dbt)

L'itération de valeur est **itérative** : on répète `v = r + move% ⊙ (T·v)` jusqu'à convergence. SQL/dbt est **déclaratif** — pas de boucle jusqu'à un critère de convergence. En numpy, c'est une multiplication matrice-vecteur (192×192) répétée ~50 fois, trivial. La grille estimée est ensuite persistée dans `machine_learning.xt_grid` et **ré-exposée à dbt comme source** (même patron que `match_registry`, écrit par `02_process.py`). dbt reprend la main pour les contributions.

### `int_xt_actions` comme source unique des actions de possession

Le terme de perte de balle a d'abord été calculé en retournant dans `int_event_enriched` depuis `xt_grid.py` — ce qui **dupliquait la formule d'affectation `(x,y)→case`**, entorse au principe « défini une seule fois ». Corrigé : on a ajouté un `action_kind = 'turnover'` à `int_xt_actions`. La table contient désormais toute la matière du modèle (tir, déplacement, perte), l'affectation est faite une fois, et `xt_grid.py` ne lit plus que cette table.

### `int_xt_contributions` en table

`xt_added` dépend des **valeurs de la grille**. Si la grille est ré-estimée, toutes les contributions changent. Matérialiser en **table** (rebuild complet à chaque run) garantit que toutes les lignes reflètent la grille courante — pas de piège de grille périmée. Le coût est nul : deux jointures de hachage sur une table de 192 lignes.

---

## Conséquences

### Positives

- Signal de **création** interprétable et attribuable par action (validation : top contributeurs = Kimmich, Alexander-Arnold, De Bruyne, Groß, Çalhanoğlu — les profils attendus).
- Grille = étalon global stable, réutilisable pour d'autres analyses (danger de zone, valeur de possession).
- Architecture propre : `int_xt_actions` source unique, grille en source dbt, contributions en modèle dbt testé.

### Négatives et limites

- **Dépendance d'orchestration** : `int_xt_contributions` lit une table écrite par un step Python. L'ordre imposé est `dbt (int_xt_actions) → xt_grid.py → dbt (int_xt_contributions)` — les deux modèles dbt ne peuvent pas être dans le même `dbt run`. À câbler dans `run_pipeline.py`.
- **Grille figée entre deux runs** de `xt_grid.py` : si la donnée amont change beaucoup, la grille peut dériver tant qu'on ne relance pas l'estimation.
- **Granularité fixe 16×12** : compromis résolution/bruit choisi a priori, pas optimisé.
- **Attribution binaire au porteur** : le xT ajouté va au passeur/dribbleur, pas de partage passeur↔receveur.

---

## Alternatives rejetées et pourquoi

**Poids de zones manuels** : aucune justification data, non défendable en entretien.

**Modèle supervisé** : boîte noire coûteuse, attribution par action difficile. L'xT donne la même intuition avec un modèle transparent (deux probabilités + une matrice de transition).

**Comptage de passes progressives** : ignore la valeur — une passe latérale dans la surface vaut plus qu'une passe verticale au milieu, ce que le comptage ne voit pas.

---

## Questions d'entretien anticipées

**« C'est quoi l'Expected Threat ? »**
Une valeur attribuée à chaque zone du terrain = la probabilité de marquer bientôt si on a le ballon là. On la calcule en résolvant, par itération de point fixe, une équation récursive : valeur d'une zone = (proba de tirer × proba de marquer le tir) + (proba de déplacer × valeur moyenne des zones où on envoie le ballon). Une action gagne `xT(arrivée) − xT(départ)`.

**« Pourquoi une chaîne de Markov, et pourquoi ça converge ? »**
Parce que la valeur d'une zone dépend de la valeur des zones voisines : c'est un système récursif qu'on résout par itération de valeur (comme un MDP). Ça converge parce qu'à chaque action une part de la possession est **perdue** (proba de tir + proba de déplacement < 1) : la chaîne finit toujours par s'absorber (tir ou perte de balle), donc la valeur reste bornée et l'itération est une contraction.

**« Quel piège avez-vous rencontré ? »**
Sans le terme de **perte de balle**, le modèle suppose qu'on garde le ballon indéfiniment : la valeur devient constante partout (grille plate). Il faut mettre au dénominateur toutes les actions de possession — tirs + déplacements réussis + pertes — pour que `shoot% + move% < 1`. C'est ce complément (la perte) qui fait décroître la valeur vers sa propre cage et crée le gradient.

**« Pourquoi ne pas tout faire en SQL/dbt ? »**
L'itération de valeur est une boucle jusqu'à convergence : SQL est déclaratif et ne boucle pas. On estime donc la grille en numpy (matmul 192×192 répétée), on la stocke dans une table, et on la ré-expose à dbt comme source pour que le reste (les contributions) reste en SQL testé.

**« Comment avez-vous validé le modèle ? »**
Deux contrôles. (1) La **grille** doit croître de sa cage (≈0) vers le but adverse et piquer devant la surface — vérifié (pic col 15/row 6, xT 0.26). (2) Les **meilleurs contributeurs** doivent être des créateurs — vérifié : Kimmich, Alexander-Arnold, De Bruyne, Groß, en tête sur 8 saisons.
