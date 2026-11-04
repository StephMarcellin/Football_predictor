{#
  season_match_ids — les match_id d'UNE saison, quand `only_season` est fourni.

  POURQUOI : un modèle incrémental par match peut traiter un sous-ensemble de matchs.
  Sur 18 saisons d'un coup (59 M lignes) DuckDB sature la mémoire et le temps explose
  (shot_creating_actions : 1 h 57) ; sur UNE saison (3,7 M lignes) il prend 2-3 s.
  L'orchestrateur boucle donc sur les saisons :
      dbt run --select tag:by_season --vars '{only_season: 2023-2024}'

  Le lien match -> saison vient de intermediate_whoscored_match_bridge, construit en
  tout début de flux, et dont la clé est un match_id : toutes les CTE sources en ont
  un, contrairement à la colonne `season` que certaines n'exposent pas.
  Le nom est qualifié par la base de `this` : aucune dépendance `ref()` ajoutée.

  Usage dans new_matches (voir les modèles tagués by_season) :
      WHERE match_id IN ({{ season_match_ids(this) }})
#}
{% macro season_match_ids(relation) -%}
SELECT str_match_id FROM {{ relation.database }}.intermediate.intermediate_whoscored_match_bridge WHERE str_season = '{{ var("only_season") }}'
{%- endmacro %}
