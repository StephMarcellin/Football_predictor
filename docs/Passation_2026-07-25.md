# Passation — Session du 25 juillet 2026

> **À coller en premier message d'un nouveau Cowork pour reprendre le contexte.**
> Thème : extension des features **intermediate** depuis les events WhoScored
> (partie B du plan) — 6 nouveaux modèles + correction d'un bug de parsing amont.
> Voir aussi `docs/Concepts_Session_2026-07-25.md` et `docs/ADR/ADR-009`.

---

## 1. État git

- **Branche courante** : `feature/whoscored-match-facts`.
- **Non commité** à cette heure (à committer, voir §6) :
  - Nouveaux : `int_penalties.sql`, `int_fouls_drawn.sql`, `int_defensive_blocks.sql`,
    `int_shot_creating_actions.sql`, `int_progressive_carries.sql`, `int_substitutions.sql`
  - Modifiés : `int_shot_placement.sql` (→ incrémental), `intermediate/schema.yml`,
    `pipelines/scrapping/scrape_whoscored_details.py` (fix parsing)
  - Docs : `Passation_2026-07-25.md`, `Concepts_Session_2026-07-25.md`,
    `ADR-009_Parsing_qualifiers_WhoScored.md` (+ `Passation_2026-07-22.md` encore untracked)

---

## 2. Ce qui a été réalisé

### 6 nouveaux modèles intermediate (tous run + test verts)

| Modèle | Grain | Idée |
|---|---|---|
| `int_penalties` | un penalty tiré | tireur, résultat (marqué/arrêté/poteau/raté), placement, gardien, obtenteur, fautif |
| `int_fouls_drawn` | une faute subie | qui obtient / concède (fautif via 233), tiers offensif, `leads_to_penalty` |
| `int_defensive_blocks` | un contre de passe | bloqueur nommé (type 74) + passe bloquée (contres de TIR hors périmètre : pas de bloqueur nommé chez Opta) |
| `int_shot_creating_actions` | crédit SCA (tir × rang 1/2) | 2 actions offensives avant le tir ; `is_gca` si but |
| `int_progressive_carries` | un dribble réussi | conduite via `TakeOn`, progression en mètres, `is_progressive` (≥5 m) |
| `int_substitutions` | un remplacement | sortie ↔ entrée par rang, `sub_number`, timing |

### Correction bug parsing (amont)

`goal_mouth_y/z` et `blocked_x/y` étaient lus comme des champs d'event alors que ce
sont des **qualifiers** → colonne remplie à 26 % seulement. Corrigé dans
`parse_events` (helper `_qualifier_value` + COALESCE qualifier/top-level). Détail et
décision : **ADR-009**.

### Divers

- `int_shot_placement` converti en **incrémental**.

---

## 3. Décisions & apprentissages clés (détails dans Concepts + ADR-009)

- **qual 233 « OppositeRelatedEvent »** = appariement déterministe des events miroirs.
- **`event_id` scopé par (match, équipe)** (66,8 % en collision) → tout join via 233
  doit contraindre l'équipe adverse. Bug corrigé dans `int_fouls_drawn` (0,6 %),
  garde ajoutée dans `int_penalties`.
- **Appariement par rang** (pas de fenêtre de temps) : penalties (tir↔faute),
  substitutions (sortie↔entrée). Robuste aux événements simultanés.
- **Pas d'event carry** dans WhoScored → conduites via `TakeOn` réussis, progression
  vers la touche suivante bornée à 5 s.
- **Parsing** : source de vérité = `qualifiers_json` ; fix à la racine + re-scrape,
  pas de backfill SQL (ADR-009).

---

## 4. Points de vigilance / opérationnel

- **Couverture `goal_mouth`** : correcte pour les matchs (re)scrapés avec le parser
  corrigé ; **NULL pour les matchs non ré-archivés** jusqu'à ce que le re-scrape les
  couvre. Requête de contrôle : colonne remplie vs qualifier présent (`LIKE
  '%GoalMouthY%'`), à relancer après chaque vague de scrape.
- **Rebuild aval après re-scrape** : certains modèles incrémentaux filtrent sur
  `match_id NOT IN (this)` → ils **ignorent un match déjà présent même re-scrapé**.
  Après un re-scrape de matchs existants, faire `dbt build --full-refresh`.
- DBeaver fermé avant tout `dbt run` / `load_archive` / UPDATE silver.

---

## 5. Reste à faire (roadmap)

1. **`int_keeper_psxg`** — bloqué : nécessite d'abord un modèle **xGOT** (post-shot
   xG entraîné, features déjà prêtes dans `int_shot_placement`). Puis agrégat
   gardien×match (gardien identifié via `int_whoscored_lineup` slot 1). Chantier ML.
2. **`int_xt_grid`** — expected threat (grille Karun Singh), estimation à part. Chantier.
3. **Brancher les 6 nouveaux modèles en gold** : SCA/GCA par joueur, conduites
   progressives par joueur/équipe, fautes subies, contres, penaltys, changements →
   `features_players` / `features_final`. C'est l'étape qui rend ces atomes utiles au ML.
4. Laisser le backfill nocturne compléter la couverture `goal_mouth`.

---

## 6. Plan de commits (à faire)

Sur `feature/whoscored-match-facts` (ou sous-branches dédiées) :

```
fix: parser goal_mouth/blocked lus depuis les qualifiers (COALESCE top-level)
     → pipelines/scrapping/scrape_whoscored_details.py

feat: int_penalties + int_fouls_drawn + int_defensive_blocks (events WhoScored)
     → 3 .sql + schema.yml

feat: int_shot_creating_actions + int_progressive_carries (chaînes de possession)
     → 2 .sql + schema.yml

feat: int_substitutions (changements appariés par rang)
     → 1 .sql + schema.yml

refactor: int_shot_placement en incrémental

docs: concepts + ADR-009 (parsing qualifiers) + passation 25 juillet
```

Puis décider de la stratégie de merge des branches WhoScored vers `main`
(question restée ouverte depuis la passation du 22 juillet).
