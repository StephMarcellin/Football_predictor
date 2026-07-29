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

SELECT
    a.match_id,
    a.row_num,
    a.season,
    a.league_source,
    a.team_id,
    a.player_id,

    a.col_from, a.row_from,
    a.col_to,   a.row_to,

    gf.xt              AS xt_from,
    gt.xt              AS xt_to,
    gt.xt - gf.xt      AS xt_added

FROM {{ ref('int_xt_actions') }} a
JOIN {{ source('machine_learning', 'xt_grid') }} gf
     ON gf."col" = a.col_from AND gf."row" = a.row_from
JOIN {{ source('machine_learning', 'xt_grid') }} gt
     ON gt."col" = a.col_to   AND gt."row" = a.row_to
WHERE a.action_kind = 'move'
  AND a.col_to IS NOT NULL
