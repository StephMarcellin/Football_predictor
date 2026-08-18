---
schema: intermediate
rows: 15782
---
# int_understat_stats

#intermediate

Stats Understat par match (npxG, npxG concédés, PPDA) rattachées au match_id unifié via int_understat_schedule. Alimente les colonnes xG et pressing de backbone.


## Intégrité
**Clé déclarée :** — (pas de test d'unicité)

## Lineage
**Sources :** [[int_understat_schedule]], [[silver.understat_stats]]
**Alimente :** [[backbone]]

## Features & profiling  (15782 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `home_xpts` | DOUBLE | 100.0% | 0 | 13441 | moy 1.577 · méd 1.599 · min/max 0.0/2.999 · p10/p90 0.397/2.712 | Expected points domicile — points attendus selon les xG du match |
| `away_xpts` | DOUBLE | 100.0% | 0 | 13212 | moy 1.195 · méd 1.082 · min/max 0.0/2.999 · p10/p90 0.186/2.424 | Expected points extérieur |
| `home_np_xg` | DOUBLE | 100.0% | 0 | 15500 | moy 1.449 · méd 1.292 · min/max 0.0/6.882 · p10/p90 0.485/2.611 | Expected goals sans pénalties — équipe domicile |
| `away_np_xg` | DOUBLE | 100.0% | 0 | 15527 | moy 1.167 · méd 1.01 · min/max 0.0/7.11 · p10/p90 0.33/2.206 | Expected goals sans pénalties — équipe extérieur |
| `home_np_xg_diff` | DOUBLE | 100.0% | 0 | 15748 | moy 0.282 · méd 0.266 · min/max -6.271/5.831 · p10/p90 -1.281/1.89 | Différentiel npxG domicile (home_np_xg - away_np_xg) |
| `away_np_xg_diff` | DOUBLE | 100.0% | 0 | 15748 | moy -0.282 · méd -0.266 · min/max -5.831/6.271 · p10/p90 -1.89/1.281 | Différentiel npxG extérieur (away_np_xg - home_np_xg) |
| `home_ppda` | DOUBLE | 100.0% | 1 | 5478 | moy 11.809 · méd 10.432 · min/max 2.114/193.0 · p10/p90 5.92/19.176 | PPDA domicile — passes adverses autorisées par action défensive (mesure l'intensité du pressing) |
| `away_ppda` | DOUBLE | 100.0% | 1 | 5507 | moy 13.108 · méd 11.37 · min/max 2.083/152.0 · p10/p90 6.257/21.571 | PPDA extérieur |
| `home_deep` | BIGINT | 100.0% | 0 | 37 | moy 6.83 · méd 6.0 · min/max 0.0/42.0 · p10/p90 2.0/13.0 | Passes complétées dans les 18 derniers mètres adverses — domicile (mesure la pression offensive) |
| `away_deep` | BIGINT | 100.0% | 0 | 30 | moy 5.641 · méd 5.0 · min/max 0.0/30.0 · p10/p90 1.0/11.0 | Passes complétées dans les 18 derniers mètres adverses — extérieur |
| `season` | VARCHAR | 100.0% | 0 | 9 | top: 2022-2023 (1827), 2020-2021 (1826), 2021-2022 (1826) | Saison au format YYYY-YYYY |
| `league_source` | VARCHAR | 100.0% | 0 | 5 | top: Serie A (3351), Premier League (3349), La Liga (3340) | Nom de la compétition source |
| `source` | VARCHAR | 100.0% | 0 | 1 | top: understat (15782) | Source des données (Understat). |
| `scraped_at` | VARCHAR | 100.0% | 0 | 39 |  | Horodatage ISO du scrape. |
| `comp_category` | VARCHAR | 100.0% | 0 | 1 | top: Big5 (15782) | Catégorie de compétition : Big5, Cup, Europe |
| `us_match_id` | BIGINT | 100.0% | 0 | 15781 |  | Identifiant du match côté Understat (source). |
| `match_id` | VARCHAR | 100.0% | 0 | 15782 |  | Identifiant unifié du match (SHA1), rattaché via int_understat_schedule (us_match_id -> match_id). |