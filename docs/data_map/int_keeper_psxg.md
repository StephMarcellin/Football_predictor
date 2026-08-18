---
schema: intermediate
rows: 1886
---
# int_keeper_psxg

#intermediate

Post-Shot Expected Goals +/- (PSxG+/-) par gardien = mesure de shot-stopping. Grain : gardien × saison × championnat. Source : int_keeper_shots. psxg_plus_minus = Σ xGOT subi − buts encaissés ; > 0 = le gardien arrête plus que la qualité des tirs ne le prédit (surperformance). Garde-fou symétrique : uniquement les tirs des matchs où def_gk_available (pas d'arrêt sans but possible). Hors CSC/penalty par construction amont. Matérialisé en table.


## Intégrité
**Clé déclarée :** (keeper_id, season, league_source) — ✅ aucun doublon

## Lineage
**Sources :** [[int_keeper_shots]]
**Alimente :** [[gardien_saison]]

## Features & profiling  (1886 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `keeper_id` | BIGINT | 100.0% | 0 | 635 |  | Gardien (player_id WhoScored). |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (289), 2021-2022 (247), 2020-2021 (241) | Saison (ex. 2023-2024). |
| `league_source` | VARCHAR | 100.0% | 0 | 7 | top: La Liga (350), Premier League (316), Serie A (311) | Championnat / source des données. |
| `shots_faced` | BIGINT | 100.0% | 0 | 204 | moy 71.25 · méd 63.0 · min/max 1.0/226.0 · p10/p90 4.0/150.5 | Nombre de tirs cadrés subis (hors CSC/penalty, matchs couverts). |
| `goals_conceded` | HUGEINT | 100.0% | 0 | 72 | moy 21.283 · méd 19.0 · min/max 0.0/84.0 · p10/p90 1.0/45.0 | Buts encaissés (hors CSC/penalty). |
| `saves` | HUGEINT | 100.0% | 0 | 152 | moy 49.967 · méd 43.0 · min/max 0.0/162.0 · p10/p90 3.0/106.0 | Arrêts = shots_faced − goals_conceded. |
| `psxg_faced` | DOUBLE | 100.0% | 0 | 1821 | moy 21.117 · méd 19.154 · min/max 0.014/79.396 · p10/p90 1.313/44.534 | Σ xGOT des tirs subis = qualité cumulée des tirs affrontés. |
| `psxg_plus_minus` | DOUBLE | 100.0% | 0 | 1707 | moy -0.166 · méd -0.174 · min/max -17.492/15.848 · p10/p90 -3.956/4.045 | psxg_faced − goals_conceded. Positif = surperformance (bon shot-stopper). |
| `save_pct` | DOUBLE | 100.0% | 0 | 311 | moy 0.685 · méd 0.698 · min/max 0.0/1.0 · p10/p90 0.545/0.8 | Taux d'arrêt brut = saves / shots_faced (ne tient pas compte de la difficulté). |