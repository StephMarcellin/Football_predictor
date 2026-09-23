# tests/great_expectations — Suites de signal statistique

## Rôle

Les suites de ce dossier valident le **SIGNAL** des tables du warehouse DuckDB :
taux max de NULL, taux max de zéros, bornes statistiques, dérive volumétrique.
Les tests de **STRUCTURE** (unicité, FK, valeurs autorisées) restent dans
`dbt_project/models/**/schema.yml`.

Répartition stricte : `dbt = structure`, `GE = signal`, `pytest = logique code`.

## Arborescence

```
tests/great_expectations/
├── runner.py                   # runner pandas-natif, headless, Windows-friendly
├── README.md                   # ce fichier
├── gold/
│   └── equipe_confrontation_zone.yml
├── intermediate/
│   └── (à venir)
├── silver/
│   └── (à venir)
└── marts/
    └── (à venir)
```

## Exécuter une suite

```powershell
# Directe (CLI)
python tests/great_expectations/runner.py tests/great_expectations/gold/equipe_confrontation_zone.yml

# Avec export JUnit XML (pour la CI)
python tests/great_expectations/runner.py tests/great_expectations/gold/equipe_confrontation_zone.yml `
    --junit reports/ge_equipe_confrontation_zone.xml

# Via pytest (bridge)
pytest tests/unit/test_equipe_confrontation_zone_ge.py -v
```

## Écrire une nouvelle suite

Suivre le skill `.claude/skills/data_test_generator.md`. Structure attendue :

```yaml
suite_name: <schema>.<table>
source:
  duckdb_path: db/football.duckdb
  query: |
    SELECT ...
    FROM <schema>.<table>
    WHERE season >= '2017-2018'   -- si colonnes zonales/WhoScored
expectations:
  - type: expect_column_values_to_not_be_null
    for_each_column: [col_a, col_b]
    kwargs: { mostly: 1.00 }
    severity: error
```

## Expectations disponibles

| Nom | Rôle |
|---|---|
| `expect_table_row_count_to_be_between` | Volumétrie totale |
| `expect_column_values_to_not_be_null` | Taux max de NULL (`mostly`) |
| `expect_column_value_zero_ratio_to_be_below` | Taux max de zéros (custom) |
| `expect_column_mean_to_be_between` | Bornes de moyenne |
| `expect_column_values_to_be_between` | Bornes de valeurs |
| `expect_partition_volume_stability` | Dérive volumétrique inter-saisons (custom) |

Ajouter une expectation → enregistrer une fonction dans `runner.py` via `@register("...")`.
