{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_xt_contributions'
    )
}}

-- Contributions Expected Threat (xT) : xT ajouté par déplacement.
-- Grain : 1 ligne par déplacement (int_xt_actions action_kind='move' AVEC destination).
--
-- xT_ajouté = xT(case d'arrivée) − xT(case de départ), attribué au joueur/équipe.
--   positif = le déplacement rapproche du but adverse (progression)
--   négatif = recul.
--
-- La grille machine_learning.xt_grid est l'étalon global (écrit par xt_grid.py).
-- L'affectation (x,y)→case vient de int_xt_actions (source unique) — jamais recalculée
-- ici : on lit col_from/row_from/col_to/row_to et on joint la grille.
--
-- Matérialisé en TABLE : xt_added dépend de la grille, un rebuild complet garantit que
-- toutes les lignes reflètent la grille courante. À relancer si la grille change.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_xt_actions lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_xt_actions AS (
    SELECT
        str_match_id                                                 AS "match_id",
        int_row_num                                                  AS "row_num",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_action_kind                                              AS "action_kind",
        bool_is_shot                                                 AS "is_shot",
        bool_is_goal                                                 AS "is_goal",
        int_col_from                                                 AS "col_from",
        int_row_from                                                 AS "row_from",
        int_col_to                                                   AS "col_to",
        int_row_to                                                   AS "row_to"
    FROM {{ ref('int_xt_actions') }}
),

mdl_body AS (
SELECT
    a.match_id,
    a.row_num,
    a.season,
    a.league_source,
    a.team_id,
    a.player_id,

    a.col_from, a.row_from,
    a.col_to,   a.row_to,

    gf.dec_xt              AS xt_from,
    gt.dec_xt              AS xt_to,
    gt.dec_xt - gf.dec_xt  AS xt_added

FROM in_int_xt_actions a
JOIN {{ source('machine_learning', 'xt_grid') }} gf
     ON gf.int_col = a.col_from AND gf.int_row = a.row_from
JOIN {{ source('machine_learning', 'xt_grid') }} gt
     ON gt.int_col = a.col_to   AND gt.int_row = a.row_to
WHERE a.action_kind = 'move'
  AND a.col_to IS NOT NULL
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "col_from"                                                   AS int_col_from,
        "row_from"                                                   AS int_row_from,
        "col_to"                                                     AS int_col_to,
        "row_to"                                                     AS int_row_to,
        "xt_from"                                                    AS dec_xt_from,
        "xt_to"                                                      AS dec_xt_to,
        "xt_added"                                                   AS dec_xt_added
    FROM mdl_body
)

SELECT * FROM mdl_out
