{{ config(materialized='table', schema='intermediate', alias='intermediate_player_season_role_lag') }}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- intermediate_whoscored_lineup_period lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_lineup AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_formation_seq                                            AS "formation_seq",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        int_period                                                   AS "period",
        int_start_minute                                             AS "start_minute",
        int_end_minute                                               AS "end_minute",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        int_slot                                                     AS "slot",
        dec_grid_vertical                                            AS "grid_vertical",
        dec_grid_horizontal                                          AS "grid_horizontal",
        bool_is_captain                                              AS "is_captain"
    FROM {{ ref('intermediate_whoscored_lineup_period') }}
),

-- intermediate_whoscored_match_bridge lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_match_index AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_ws_match_id                                              AS "ws_match_id",
        dt_match_date                                                AS "match_date",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        CAST(str_ws_home_team_id AS INTEGER)                         AS "ws_home_team_id",
        CAST(str_ws_away_team_id AS INTEGER)                         AS "ws_away_team_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        str_comp_category                                            AS "comp_category"
    FROM {{ ref('intermediate_whoscored_match_bridge') }}
),

mdl_body AS (
with
-- Nouvelle logique dans intermediate_player_season_role_lag
-- ══ Refonte du 09/10/2026 — defaut 30 ══════════════════════════════════════
-- Trois problemes corriges d'un coup, parce que les patcher separement laissait
-- la chaine incoherente :
--
-- 1. PRODUIT CARTESIEN. season_agg joignait apparitions_classified a
--    in_int_whoscored_lineup sur `player_id` SEUL (la condition s'arretait sur un
--    `-- ...` jamais termine). Chaque titularisation etait donc appariee a toutes
--    les lignes de composition du joueur, toutes saisons confondues. Mesure sur la
--    table livree : apps_starter_lag moyenne 7 204 et max 53 713, pour un maximum
--    reel de 46 titularisations ; minutes_titu_lag jusqu'a 1 169 520 minutes, soit
--    812 jours de football en une saison. Les coordonnees moyennes etaient donc des
--    moyennes de carriere, pas de saison. La jointure est supprimee : tout est lu
--    depuis apparitions_classified.
--
-- 2. CLASSIFIEUR MORT. dominant_role (role cumulant le plus de minutes) etait
--    calcule puis jamais reference ; `lagged` partait de season_agg et le SELECT
--    final reclassifiait depuis gv_avg/gh_avg. Le commentaire de season_agg disait
--    pourtant « plus utilisees pour classer ». La methode annoncee est desormais
--    celle qui s'applique : role_fin_lag vient de dominant_role.
--    Au passage, classer sur une moyenne de coordonnees est faux en soi : un joueur
--    alternant lateral gauche (gh 1) et lateral droit (gh 9) moyenne a gh 5, donc
--    central. gv_avg/gh_avg restent exposees, en information seule.
--
-- 3. GRAIN. apparitions_classified prenait toute periode de formation de duree > 0,
--    remplacants compris, et comptait les PERIODES et non les matchs. Le grain est
--    desormais (joueur, saison, match ou il a DEMARRE), avec les minutes cumulees
--    sur l'ensemble de ses periodes du match. apps_starter_lag compte donc des
--    titularisations, ce que son nom affirme.
--
-- Invisible sur dev : une seule saison en base, donc role_fin_lag ne rencontre
-- aucun match et vaut NULL partout apres le decalage. Sur prod, player_match_role
-- fait COALESCE(role_fin_lag, role_fin_current) — le lag a la PRIORITE sur le
-- creneau du match, donc cette valeur fausse ecrasait la bonne.

-- Une ligne = (joueur, saison, match ou il etait titulaire).
-- minutes_match additionne toutes ses periodes du match, y compris apres un
-- changement de formation ; gv_depart / gh_depart sont celles de son creneau de
-- depart, seul creneau qui decrit la position voulue par l'entraineur.
titularisations as (
    select
        l.player_id,
        idx.season                                                    as source_season,
        l.match_id,
        sum(l.end_minute - l.start_minute)                            as minutes_match,
        any_value(l.grid_vertical)   filter (where l.start_minute = 0) as gv_depart,
        any_value(l.grid_horizontal) filter (where l.start_minute = 0) as gh_depart
    from in_int_whoscored_lineup l
    join in_int_whoscored_match_index idx using (match_id)
    where (l.end_minute - l.start_minute) > 0
    group by 1, 2, 3
    -- Pas de creneau a la minute 0 = entre en jeu, pas titulaire.
    having count(*) filter (where l.start_minute = 0) > 0
),

-- Role du creneau de depart. Regle identique a celle d'intermediate_player_match_role,
-- plafond WM a 6.0 compris (defaut 27) et tolerance CB a 2.0 — une defense a trois
-- place ses excentres a gh 3 et 7, soit un ecart de 2.
apparitions_classified as (
    select
        player_id,
        source_season,
        match_id,
        minutes_match as minutes_titu,
        gv_depart     as grid_vertical,
        gh_depart     as grid_horizontal,
        case
            when gv_depart is null                               then null
            when gv_depart <= 0.5                                then 'GK'
            when gv_depart <= 3.0 and abs(gh_depart - 5) <= 2.0  then 'CB'
            when gv_depart <= 3.0                                then 'FB'
            when gv_depart <  5.0 and abs(gh_depart - 5) <= 1.5  then 'DM'
            when gv_depart <= 6.0 and abs(gh_depart - 5) <= 1.5  then 'CM'
            when gv_depart <= 6.0                                then 'WM'
            when gv_depart <= 7.5 and abs(gh_depart - 5) <= 1.5  then 'AM'
            when abs(gh_depart - 5) <= 1.5                       then 'ST'
            else                                                      'W'
        end as role_this_app
    from titularisations
),

role_minutes_by_player_season as (
    -- Cumul minutes par (joueur, saison, role)
    select player_id, source_season, role_this_app as role,
           sum(minutes_titu) as minutes_in_role
    from apparitions_classified
    group by 1, 2, 3
),

dominant_role as (
    -- Role cumulant le plus de minutes sur la saison. C'est LUI qui classe.
    select player_id, source_season, role as role_fin_lag
    from role_minutes_by_player_season
    qualify row_number() over (
        partition by player_id, source_season
        order by minutes_in_role desc, role   -- tie-break stable par nom du role
    ) = 1
),

season_agg as (
    -- Compteurs + coordonnees moyennes. Information seule : ce n'est plus elles qui
    -- classent. Aucune jointure ici, donc aucun produit cartesien possible.
    select
        player_id,
        source_season,
        sum(minutes_titu)                                                 as minutes_titu_lag,
        count(*)                                                          as apps_starter_lag,
        sum(grid_vertical   * minutes_titu) / nullif(sum(minutes_titu), 0) as gv_avg,
        sum(grid_horizontal * minutes_titu) / nullif(sum(minutes_titu), 0) as gh_avg
    from apparitions_classified
    group by 1, 2
),

-- Puis lag +1 saison et select final

lagged as (
    select
        sa.player_id,
        -- printf est supporté nativement par DuckDB
        printf('%d-%d', 
            cast(substr(sa.source_season, 1, 4) as integer) + 1,
            cast(substr(sa.source_season, 6, 4) as integer) + 1
        ) as season,
        sa.gv_avg,
        sa.gh_avg,
        sa.minutes_titu_lag,
        sa.apps_starter_lag,
        dr.role_fin_lag
    from season_agg sa
    left join dominant_role dr
        on  dr.player_id     = sa.player_id
        and dr.source_season = sa.source_season
)

select
    player_id,
    season,
    gv_avg,
    gh_avg,
    abs(gh_avg - 5) as gh_offaxis,
    minutes_titu_lag,
    apps_starter_lag,
    role_fin_lag
from lagged
),

mdl_out AS (
    SELECT
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "season"                                                     AS str_season,
        "gv_avg"                                                     AS dec_gv_avg,
        "gh_avg"                                                     AS dec_gh_avg,
        "gh_offaxis"                                                 AS dec_gh_offaxis,
        CAST(minutes_titu_lag AS BIGINT)                             AS int_minutes_titu_lag,
        "apps_starter_lag"                                           AS int_apps_starter_lag,
        "role_fin_lag"                                               AS str_role_fin_lag
    FROM mdl_body
)

SELECT * FROM mdl_out
