"""
serve_features.py — Chemin de service : recalcule les features de compo pour UN
match depuis la compo fournie (ids joueurs), à l'identique des modèles dbt.
Chaque fonction est vérifiée contre dbt (écart = 0) avant usage.

Nommage : lit et produit directement les noms refondus (préfixe de type en tête :
str_, int_, dec_, dt_, bool_), identiques à ceux de marts.mart_1n2. Les ids
(match, équipe, joueur, formation) sont des VARCHAR.
"""

import json, unicodedata
from pathlib import Path
import yaml


def _in_list(ids):
    """Liste SQL de littéraux texte : ('a','b',...). Les ids sont des VARCHAR."""
    return ",".join("'" + str(i).replace("'", "''") + "'" for i in ids)


def lineup_features(con, match_id, team_id, xi_ids):
    """Reproduit gold.equipe_lineup_match : agrège les profils SEASON-LAG des 11
    titulaires (joueur_saison) fournis dans la compo."""
    return con.execute(f"""
        select sum(dec_scorer_xg_per90_lag)     as dec_lineup_sum_xg_per90_lag,
               avg(dec_scorer_shots_per90_lag)  as dec_lineup_avg_shots_per90_lag,
               avg(dec_def_actions_per90_lag)   as dec_lineup_avg_def_actions_per90_lag,
               avg(dec_def_aerial_win_rate_lag) as dec_lineup_avg_aerial_win_rate_lag
        from gold.joueur_saison
        where str_match_id=? and str_team_id=? and str_player_id in ({_in_list(xi_ids)})""",
        [str(match_id), str(team_id)]).fetchdf().iloc[0].to_dict()


def keeper_features(con, match_id, team_id, gk_id, season):
    """Reproduit gold.equipe_gardien_match : profil SEASON-LAG du gardien titulaire
    (gardien_saison) désigné dans la compo."""
    cols = ["dec_keeper_psxg_plus_minus_lag", "dec_keeper_psxg_per_shot_lag",
            "dec_keeper_save_pct_lag", "int_keeper_shots_faced_lag"]
    r = con.execute(f"""select {", ".join(cols)} from gold.gardien_saison
        where str_keeper_id=? and str_season=?""", [str(gk_id), season]).fetchdf()
    return r.iloc[0].to_dict() if len(r) else {c: None for c in cols}


def _opp_name(col):
    """Nom de la feature adverse dans le mart : le contexte opp_ se place APRÈS le
    préfixe de type (dec_lineup_x → dec_opp_lineup_x)."""
    typ, rest = col.split("_", 1)
    return f"{typ}_opp_{rest}"


def match_compo_features(con, match_id, team_id, opp_id, compo, season):
    """Assemble pour team_id : son onze + gardien + l'onze ADVERSE (contexte opp_).
    `compo` = {team_id: {"xi": [ids...], "gk": id}, opp_id: {...}}."""
    own = lineup_features(con, match_id, team_id, compo[team_id]["xi"])
    opp = lineup_features(con, match_id, opp_id, compo[opp_id]["xi"])
    gk  = keeper_features(con, match_id, team_id, compo[team_id]["gk"], season)
    feats = dict(own)
    feats.update({_opp_name(k): v for k, v in opp.items()})
    feats.update(gk)
    return feats


def _values(match_id, xi_positions, with_vertical):
    """Lignes VALUES (ids en texte) pour la compo servie."""
    mid = str(match_id).replace("'", "''")
    if with_vertical:
        return ",".join(f"('{mid}','{t}','{p}',{gv},{gh})" for (t, p, gv, gh) in xi_positions)
    return ",".join(f"('{mid}','{t}','{p}',{gh})" for (t, p, _, gh) in xi_positions)


def zonal_features(con, match_id, xi_positions):
    """Reproduit la chaîne zonale (team_corridor_profile → zone_confrontation_match
    → equipe_confrontation_zone) : 16 features off_/def_ par couloir, pour les DEUX
    équipes. `xi_positions` = liste de (team_id, player_id, grid_vertical, grid_horizontal).

    NB : joint gold.joueur_zone_saison — ce sur quoi le modèle a été ENTRAÎNÉ.
    (Le .sql dbt pointe vers zonal_profiles_imputed mais n'est pas reconstruit ;
    on reste sur joueur_zone_saison pour éviter le train/serve skew.)
    """
    vals = _values(match_id, xi_positions, with_vertical=False)
    sql = f"""
    with serve_xi(str_match_id,str_team_id,str_player_id,dec_grid_horizontal) as (values {vals}),
    xi as (select distinct s.str_match_id,s.str_team_id,b.str_opponent_id,s.str_player_id,b.str_season,
        case when s.dec_grid_horizontal<4.5 then 'gauche' when s.dec_grid_horizontal<=5.5 then 'axe' else 'droit' end str_corridor
      from serve_xi s join intermediate.backbone b on b.str_match_id=s.str_match_id and b.str_team_id=s.str_team_id),
    prof as (select x.str_match_id,x.str_team_id,x.str_opponent_id,x.str_corridor,
        jz.dec_off_shot_volume_by_zone_lag dec_off_vol, jz.dec_off_cross_volume_by_zone_lag dec_off_cross,
        jz.dec_off_progressive_actions_by_zone_lag dec_off_prog, jz.dec_off_touch_share_by_zone_lag dec_off_touch,
        jz.dec_def_duel_win_rate_by_zone_lag dec_def_wr, jz.dec_def_actions_by_zone_lag dec_def_act, jz.int_n_duels_prev,
        cast(substr(jz.str_zone_5x5,2,1) as int) int_z, cast(substr(jz.str_zone_5x5,5,1) as int) int_c
      from xi x join gold.joueur_zone_saison jz on jz.str_player_id=x.str_player_id and jz.str_season=x.str_season),
    tcp as (select str_match_id,str_team_id,str_opponent_id,str_corridor,
        sum(case when int_z in(4,5) and ((str_corridor='gauche' and int_c in(1,2)) or (str_corridor='axe' and int_c=3) or (str_corridor='droit' and int_c in(4,5))) then dec_off_vol else 0 end) dec_off_strength,
        sum(case when int_z in(4,5) and ((str_corridor='gauche' and int_c in(1,2)) or (str_corridor='axe' and int_c=3) or (str_corridor='droit' and int_c in(4,5))) then dec_off_cross else 0 end) dec_off_cross_strength,
        sum(case when int_z in(4,5) and ((str_corridor='gauche' and int_c in(1,2)) or (str_corridor='axe' and int_c=3) or (str_corridor='droit' and int_c in(4,5))) then dec_off_prog else 0 end) dec_off_dribble_strength,
        sum(case when int_z in(3,4) and int_c=3 then dec_off_prog else 0 end) dec_off_central_progression,
        sum(case when int_z in(1,2,3) and int_c=3 then dec_def_act else 0 end) dec_def_central_density,
        sum(case when int_z in(1,2) and ((str_corridor='gauche' and int_c in(1,2)) or (str_corridor='axe' and int_c=3) or (str_corridor='droit' and int_c in(4,5))) then dec_def_wr*int_n_duels_prev else 0 end)
          / nullif(sum(case when int_z in(1,2) and ((str_corridor='gauche' and int_c in(1,2)) or (str_corridor='axe' and int_c=3) or (str_corridor='droit' and int_c in(4,5))) then int_n_duels_prev else 0 end),0) dec_def_solidity
      from prof group by 1,2,3,4),
    zc as (select a.str_match_id,a.str_team_id str_attacking_team_id,a.str_opponent_id str_defending_team_id,a.str_corridor str_attack_corridor,
        a.dec_off_strength*(1-b.dec_def_solidity) dec_matchup_danger_by_corridor,
        a.dec_off_cross_strength*(1-b.dec_def_solidity) dec_matchup_cross_threat,
        a.dec_off_dribble_strength*(1-b.dec_def_solidity) dec_matchup_dribble_threat,
        case when a.str_corridor='axe' then a.dec_off_central_progression/nullif(b.dec_def_central_density,0) end dec_matchup_central_control
      from tcp a join tcp b on b.str_match_id=a.str_match_id and b.str_team_id=a.str_opponent_id
        and b.str_corridor=case a.str_corridor when 'gauche' then 'droit' when 'droit' then 'gauche' else 'axe' end),
    off as (select str_match_id,str_attacking_team_id str_team_id,
      max(case when str_attack_corridor='gauche' then dec_matchup_danger_by_corridor end) dec_off_danger_gauche,
      max(case when str_attack_corridor='axe' then dec_matchup_danger_by_corridor end) dec_off_danger_axe,
      max(case when str_attack_corridor='droit' then dec_matchup_danger_by_corridor end) dec_off_danger_droit,
      max(case when str_attack_corridor='gauche' then dec_matchup_cross_threat end) dec_off_cross_gauche,
      max(case when str_attack_corridor='droit' then dec_matchup_cross_threat end) dec_off_cross_droit,
      max(case when str_attack_corridor='gauche' then dec_matchup_dribble_threat end) dec_off_dribble_gauche,
      max(case when str_attack_corridor='droit' then dec_matchup_dribble_threat end) dec_off_dribble_droit,
      max(case when str_attack_corridor='axe' then dec_matchup_central_control end) dec_off_central_axe from zc group by 1,2),
    def as (select str_match_id,str_defending_team_id str_team_id,
      max(case when str_attack_corridor='gauche' then dec_matchup_danger_by_corridor end) dec_def_danger_gauche,
      max(case when str_attack_corridor='axe' then dec_matchup_danger_by_corridor end) dec_def_danger_axe,
      max(case when str_attack_corridor='droit' then dec_matchup_danger_by_corridor end) dec_def_danger_droit,
      max(case when str_attack_corridor='gauche' then dec_matchup_cross_threat end) dec_def_cross_gauche,
      max(case when str_attack_corridor='droit' then dec_matchup_cross_threat end) dec_def_cross_droit,
      max(case when str_attack_corridor='gauche' then dec_matchup_dribble_threat end) dec_def_dribble_gauche,
      max(case when str_attack_corridor='droit' then dec_matchup_dribble_threat end) dec_def_dribble_droit,
      max(case when str_attack_corridor='axe' then dec_matchup_central_control end) dec_def_central_axe from zc group by 1,2)
    select o.str_team_id, o.* exclude(str_team_id,str_match_id), d.* exclude(str_team_id,str_match_id)
    from off o join def d using(str_match_id,str_team_id)
    """
    df = con.execute(sql).fetchdf()
    return {str(row["str_team_id"]): {c: row[c] for c in df.columns if c != "str_team_id"}
            for _, row in df.iterrows()}

def formation_features(con, match_id, xi_positions):
    """Reproduit int_lineup_formation + int_formation_matchup_match pour LES DEUX
    équipes. Retourne {team_id: {feature: value, ...}} contenant les features
    self, opp (miroir depuis l'adversaire) et les deltas matchup directionnels.
    `xi_positions` = [(team_id, player_id, grid_vertical, grid_horizontal), ...]

    IMPORTANT — TRAIN/SERVE : le CASE de rôle DOIT rester ALIGNÉ AVEC
    int_player_role_lag / int_player_role_match. Si les seuils changent en dbt,
    changer ici aussi (ceinture MANUELLE).
    """
    vals = _values(match_id, xi_positions, with_vertical=True)
    sql = f"""
    with serve_xi(str_match_id, str_team_id, str_player_id, dec_gv_start, dec_gh_start) as (values {vals}),
    -- Rôle courant (calculé sur la coord slot) + saison via backbone.
    xi_with_role as (
        select s.*, b.str_season,
            case
                when dec_gv_start <= 0.5                                        then 'GK'
                when dec_gv_start <= 3.0 and abs(dec_gh_start - 5) <= 2.0       then 'CB'
                when dec_gv_start <= 3.0                                        then 'FB'
                when dec_gv_start <  5.0 and abs(dec_gh_start - 5) <= 1.5       then 'DM'
                when dec_gv_start <= 6.0 and abs(dec_gh_start - 5) <= 1.5       then 'CM'
                when dec_gv_start <= 7.5 and abs(dec_gh_start - 5) <= 1.5       then 'AM'
                when dec_gv_start <= 5.5                                        then 'WM'
                when abs(dec_gh_start - 5) <= 1.5                               then 'ST'
                else                                                                 'W'
            end as str_role_current
        from serve_xi s
        left join intermediate.backbone b on b.str_match_id=s.str_match_id and b.str_team_id=s.str_team_id
    ),
    -- role_fin = COALESCE(SEASON-LAG, current).
    xi_resolved as (
        select x.*, coalesce(r.str_role_fin_lag, x.str_role_current) as str_role_fin
        from xi_with_role x
        left join intermediate.int_player_role_lag r
            on r.str_player_id=x.str_player_id and r.str_season=x.str_season
    ),
    -- int_lineup_formation par (match, team).
    form_by_team as (
        select str_match_id, str_team_id,
            count(*) filter (where str_role_fin='GK')                          as int_n_gk,
            count(*) filter (where str_role_fin in ('CB','FB'))                as int_n_def,
            count(*) filter (where str_role_fin in ('DM','CM','AM','WM'))      as int_n_mid,
            count(*) filter (where str_role_fin in ('W','ST'))                 as int_n_att,
            count(*) filter (where str_role_fin in ('W','WM'))                 as int_n_wingers,
            count(*) filter (where str_role_fin in ('ST','AM'))                as int_n_central_att,
            stddev_samp(dec_gh_start) filter (where str_role_fin != 'GK')      as dec_bloc_width,
            stddev_samp(dec_gv_start) filter (where str_role_fin != 'GK')      as dec_bloc_depth,
            avg(dec_gv_start) filter (where str_role_fin in ('CB','FB'))       as dec_line_defensive_avg,
            avg(dec_gv_start) filter (where str_role_fin in ('W','ST'))        as dec_line_offensive_avg,
            avg(case when abs(dec_gh_start - 5) <= 1.5 then 1.0 else 0.0 end)
                filter (where str_role_fin != 'GK')                            as dec_axiality_score
        from xi_resolved group by 1, 2
    ),
    form_labeled as (
        select f.*,
            case when int_n_def=3 then '3-back'
                 when int_n_def=4 then '4-back'
                 when int_n_def=5 then '5-back'
                 else                  'other'
            end as str_formation_family
        from form_by_team f
    )
    -- Self-join : chaque team reçoit ses features + celles de l'adversaire (opp)
    -- + les deltas matchup.
    select
        s.str_team_id,
        s.str_formation_family, s.int_n_gk, s.int_n_def, s.int_n_mid, s.int_n_att,
        s.int_n_wingers, s.int_n_central_att, s.dec_bloc_width, s.dec_bloc_depth,
        s.dec_line_defensive_avg, s.dec_line_offensive_avg, s.dec_axiality_score,
        o.str_formation_family     as str_opp_formation_family,
        o.int_n_gk                 as int_opp_n_gk,
        o.int_n_def                as int_opp_n_def,
        o.int_n_mid                as int_opp_n_mid,
        o.int_n_att                as int_opp_n_att,
        o.int_n_wingers            as int_opp_n_wingers,
        o.int_n_central_att        as int_opp_n_central_att,
        o.dec_bloc_width           as dec_opp_bloc_width,
        o.dec_bloc_depth           as dec_opp_bloc_depth,
        o.dec_line_defensive_avg   as dec_opp_line_defensive_avg,
        o.dec_line_offensive_avg   as dec_opp_line_offensive_avg,
        o.dec_axiality_score       as dec_opp_axiality_score,
        (s.int_n_att              - o.int_n_def)              as int_attack_overload,
        (s.int_n_mid              - o.int_n_mid)              as int_mid_control_delta,
        (s.dec_line_defensive_avg - o.dec_line_defensive_avg) as dec_back_depth_delta,
        (s.dec_bloc_width         - o.dec_bloc_width)         as dec_width_delta,
        (s.dec_bloc_depth         - o.dec_bloc_depth)         as dec_depth_delta,
        (s.dec_axiality_score     - o.dec_axiality_score)     as dec_axiality_delta,
        s.str_formation_family     as str_formation_family_self,
        o.str_formation_family     as str_formation_family_opp,
        concat(s.str_formation_family, '_vs_', o.str_formation_family) as str_matchup_family
    from form_labeled s
    join form_labeled o
        on o.str_match_id = s.str_match_id
       and o.str_team_id != s.str_team_id
    """
    df = con.execute(sql).fetchdf()
    return {str(row["str_team_id"]): {c: row[c] for c in df.columns if c != "str_team_id"}
            for _, row in df.iterrows()}

def serve_match(con, match_id, compo, xi_positions):
    """Assemble le vecteur complet du match pour les 2 équipes :
    lit la ligne mart existante, puis remplace les features de compo par le
    recompute depuis `compo`. Rend le DataFrame prêt pour prepare_x/predict.
      compo        = {team_id: {"xi": [ids...], "gk": id}, opp_id: {...}}
      xi_positions = [(team_id, player_id, grid_vertical, grid_horizontal), ...]
    """
    df = con.execute("select * from marts.mart_1n2 where str_match_id=? order by str_team_id",
                     [str(match_id)]).fetchdf()
    teams = list(df["str_team_id"])
    zonal     = zonal_features(con, match_id, xi_positions)
    formation = formation_features(con, match_id, xi_positions)
    for i, t in enumerate(teams):
        opp = teams[1 - i]
        season = df.loc[df.str_team_id == t, "str_season"].iloc[0]
        feats = match_compo_features(con, match_id, t, opp, compo, season)
        feats.update(zonal.get(t, {}))
        feats.update(formation.get(t, {}))
        for k, v in feats.items():
            df.loc[df.str_team_id == t, k] = v
    return df

def predict_served(con, match_id, compo, xi_positions, payload, spec):
    """serve_match → prepare_x → probas P(H/D/A). Refuse une sortie dégénérée."""
    import numpy as np
    import ml_common as mc
    df = serve_match(con, match_id, compo, xi_positions)
    X = mc.prepare_x(df, spec["target"], spec.get("exclude")).reindex(
        columns=payload["features"], fill_value=0)
    proba = payload["model"].predict_proba(X)
    if float(proba.std(axis=0).mean()) < 1e-6:
        raise RuntimeError("Probas uniformes : calibration dégénérée — vérifie la "
                           "version de scikit-learn vs celle d'entraînement.")
    inv = {v: k for k, v in payload["label_map"].items()}
    out = df[["str_match_id", "str_team_id"]].copy()
    for i, cls in enumerate(payload["model"].classes_):
        out[f"dec_prob_{inv[cls]}"] = proba[:, i]
    return out

def _norm(s):
    """Minuscule + sans accents, pour comparer les noms de façon robuste."""
    return "".join(c for c in unicodedata.normalize("NFD", s.lower().strip())
                   if unicodedata.category(c) != "Mn")


def build_template(con):
    """Gabarit formation_id → {slot: (grid_vertical, grid_horizontal)}, déterministe.
    Fournit la position (verticale + horizontale) de chaque joueur depuis son slot."""
    tmpl = {}
    for fid, pos in con.execute("""select str_formation_id, any_value(str_formation_positions)
        from intermediate.int_whoscored_formations group by str_formation_id""").fetchall():
        p = json.loads(pos)
        tmpl[fid] = {i + 1: (float(p[i]["vertical"]), float(p[i]["horizontal"]))
                     for i in range(min(11, len(p)))}
    return tmpl


def resolve_player(con, name, team_id, season):
    """Nom → player_id, restreint à (équipe, saison). Exact d'abord, puis 'contient'.
    Lève une erreur claire si 0 ou >1 correspondance (homonyme / graphie)."""
    rows = con.execute("""select str_player_id, str_player_name from intermediate.int_whoscored_players
        where str_team_id=? and str_season=?""", [str(team_id), season]).fetchall()
    n = _norm(name)
    hits = [pid for pid, pn in rows if _norm(pn) == n] or \
           [pid for pid, pn in rows if n in _norm(pn)]
    if len(hits) != 1:
        raise ValueError(f"'{name}' → {len(hits)} correspondance(s) pour team {team_id} / {season}")
    return hits[0]


def load_compo(con, path):
    """Lit le fichier compo YAML (home/away : formation_id + 11 noms), résout les
    noms en ids, déduit les positions via le gabarit. Retourne (match_id, compo,
    xi_positions) prêts pour serve_match / predict_served."""
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    mid = str(spec["match_id"])                      # clé du fichier compo (config), pas une colonne
    venue = {("home" if v == "Home" else "away"): t for t, v in con.execute(
        "select str_team_id, str_venue from intermediate.backbone where str_match_id=?", [mid]).fetchall()}
    season = con.execute("select any_value(str_season) from marts.mart_1n2 where str_match_id=?",
                         [mid]).fetchone()[0]
    tmpl = build_template(con)
    compo, xi_pos = {}, []
    for side in ("home", "away"):
        t, fid = venue[side], str(spec[side]["formation_id"])
        ids = [resolve_player(con, nm, t, season) for nm in spec[side]["xi"]]
        compo[t] = {"xi": ids, "gk": ids[0]}           # slot 1 = gardien
        for slot, pid in enumerate(ids, start=1):
            gv, gh = tmpl[fid][slot]
            xi_pos.append((t, pid, gv, gh))            # 4-tuple
    return mid, compo, xi_pos
