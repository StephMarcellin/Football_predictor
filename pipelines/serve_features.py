"""
serve_features.py — Chemin de service : recalcule les features de compo pour UN
match depuis la compo fournie (ids joueurs), à l'identique des modèles dbt.
Chaque fonction est vérifiée contre dbt (écart = 0) avant usage.
"""

import json, unicodedata
from pathlib import Path
import yaml

def lineup_features(con, match_id, team_id, xi_ids):
    """Reproduit gold.equipe_lineup_match : agrège les profils SEASON-LAG des 11
    titulaires (joueur_saison) fournis dans la compo."""
    ids = ",".join(map(str, xi_ids))
    return con.execute(f"""
        select sum(scorer_xg_per90_lag)     as lineup_sum_xg_per90_lag,
               avg(scorer_shots_per90_lag)  as lineup_avg_shots_per90_lag,
               avg(def_actions_per90_lag)   as lineup_avg_def_actions_per90_lag,
               avg(def_aerial_win_rate_lag) as lineup_avg_aerial_win_rate_lag
        from gold.joueur_saison
        where match_id=? and team_id=? and player_id in ({ids})""",
        [match_id, team_id]).fetchdf().iloc[0].to_dict()


def keeper_features(con, match_id, team_id, gk_id, season):
    """Reproduit gold.equipe_gardien_match : profil SEASON-LAG du gardien titulaire
    (gardien_saison) désigné dans la compo."""
    cols = ["keeper_psxg_plus_minus_lag","keeper_psxg_per_shot_lag",
            "keeper_save_pct_lag","keeper_shots_faced_lag"]
    r = con.execute(f"""select {", ".join(cols)} from gold.gardien_saison
        where keeper_id=? and season=?""", [gk_id, season]).fetchdf()
    return r.iloc[0].to_dict() if len(r) else {c: None for c in cols}


def match_compo_features(con, match_id, team_id, opp_id, compo, season):
    """Assemble pour team_id : son onze + gardien + l'onze ADVERSE (préfixe opp_).
    `compo` = {team_id: {"xi": [ids...], "gk": id}, opp_id: {...}}."""
    own = lineup_features(con, match_id, team_id, compo[team_id]["xi"])
    opp = lineup_features(con, match_id, opp_id, compo[opp_id]["xi"])
    gk  = keeper_features(con, match_id, team_id, compo[team_id]["gk"], season)
    feats = dict(own)
    feats.update({f"opp_{k}": v for k, v in opp.items()})
    feats.update(gk)
    return feats

def zonal_features(con, match_id, xi_positions):
    """Reproduit la chaîne zonale (team_corridor_profile → zone_confrontation_match
    → equipe_confrontation_zone) : 16 features off_/def_ par couloir, pour les DEUX
    équipes. `xi_positions` = liste de (team_id, player_id, grid_horizontal).

    NB : joint gold.joueur_zone_saison — ce sur quoi le modèle a été ENTRAÎNÉ.
    (Le .sql dbt pointe vers zonal_profiles_imputed mais n'est pas reconstruit ;
    on reste sur joueur_zone_saison pour éviter le train/serve skew.)
    """
    vals = ",".join(f"('{match_id}',{t},{p},{gh})" for (t, p, gh) in xi_positions)
    sql = f"""
    with serve_xi(match_id,team_id,player_id,grid_horizontal) as (values {vals}),
    xi as (select distinct s.match_id,s.team_id,b.opponent_id,s.player_id,b.season,
        case when s.grid_horizontal<4.5 then 'gauche' when s.grid_horizontal<=5.5 then 'axe' else 'droit' end corridor
      from serve_xi s join intermediate.backbone b on b.match_id=s.match_id and b.team_id=s.team_id),
    prof as (select x.match_id,x.team_id,x.opponent_id,x.corridor,
        jz.off_shot_volume_by_zone_lag off_vol, jz.off_cross_volume_by_zone_lag off_cross,
        jz.off_progressive_actions_by_zone_lag off_prog, jz.off_touch_share_by_zone_lag off_touch,
        jz.def_duel_win_rate_by_zone_lag def_wr, jz.def_actions_by_zone_lag def_act, jz.n_duels_prev,
        cast(substr(jz.zone_5x5,2,1) as int) z, cast(substr(jz.zone_5x5,5,1) as int) c
      from xi x join gold.joueur_zone_saison jz on jz.player_id=x.player_id and jz.season=x.season),
    tcp as (select match_id,team_id,opponent_id,corridor,
        sum(case when z in(4,5) and ((corridor='gauche' and c in(1,2)) or (corridor='axe' and c=3) or (corridor='droit' and c in(4,5))) then off_vol else 0 end) off_strength,
        sum(case when z in(4,5) and ((corridor='gauche' and c in(1,2)) or (corridor='axe' and c=3) or (corridor='droit' and c in(4,5))) then off_cross else 0 end) off_cross_strength,
        sum(case when z in(4,5) and ((corridor='gauche' and c in(1,2)) or (corridor='axe' and c=3) or (corridor='droit' and c in(4,5))) then off_prog else 0 end) off_dribble_strength,
        sum(case when z in(3,4) and c=3 then off_prog else 0 end) off_central_progression,
        sum(case when z in(1,2,3) and c=3 then def_act else 0 end) def_central_density,
        sum(case when z in(1,2) and ((corridor='gauche' and c in(1,2)) or (corridor='axe' and c=3) or (corridor='droit' and c in(4,5))) then def_wr*n_duels_prev else 0 end)
          / nullif(sum(case when z in(1,2) and ((corridor='gauche' and c in(1,2)) or (corridor='axe' and c=3) or (corridor='droit' and c in(4,5))) then n_duels_prev else 0 end),0) def_solidity
      from prof group by 1,2,3,4),
    zc as (select a.match_id,a.team_id attacking_team_id,a.opponent_id defending_team_id,a.corridor attack_corridor,
        a.off_strength*(1-b.def_solidity) matchup_danger_by_corridor,
        a.off_cross_strength*(1-b.def_solidity) matchup_cross_threat,
        a.off_dribble_strength*(1-b.def_solidity) matchup_dribble_threat,
        case when a.corridor='axe' then a.off_central_progression/nullif(b.def_central_density,0) end matchup_central_control
      from tcp a join tcp b on b.match_id=a.match_id and b.team_id=a.opponent_id
        and b.corridor=case a.corridor when 'gauche' then 'droit' when 'droit' then 'gauche' else 'axe' end),
    off as (select match_id,attacking_team_id team_id,
      max(case when attack_corridor='gauche' then matchup_danger_by_corridor end) off_danger_gauche,
      max(case when attack_corridor='axe' then matchup_danger_by_corridor end) off_danger_axe,
      max(case when attack_corridor='droit' then matchup_danger_by_corridor end) off_danger_droit,
      max(case when attack_corridor='gauche' then matchup_cross_threat end) off_cross_gauche,
      max(case when attack_corridor='droit' then matchup_cross_threat end) off_cross_droit,
      max(case when attack_corridor='gauche' then matchup_dribble_threat end) off_dribble_gauche,
      max(case when attack_corridor='droit' then matchup_dribble_threat end) off_dribble_droit,
      max(case when attack_corridor='axe' then matchup_central_control end) off_central_axe from zc group by 1,2),
    def as (select match_id,defending_team_id team_id,
      max(case when attack_corridor='gauche' then matchup_danger_by_corridor end) def_danger_gauche,
      max(case when attack_corridor='axe' then matchup_danger_by_corridor end) def_danger_axe,
      max(case when attack_corridor='droit' then matchup_danger_by_corridor end) def_danger_droit,
      max(case when attack_corridor='gauche' then matchup_cross_threat end) def_cross_gauche,
      max(case when attack_corridor='droit' then matchup_cross_threat end) def_cross_droit,
      max(case when attack_corridor='gauche' then matchup_dribble_threat end) def_dribble_gauche,
      max(case when attack_corridor='droit' then matchup_dribble_threat end) def_dribble_droit,
      max(case when attack_corridor='axe' then matchup_central_control end) def_central_axe from zc group by 1,2)
    select o.team_id, o.* exclude(team_id,match_id), d.* exclude(team_id,match_id)
    from off o join def d using(match_id,team_id)
    """
    df = con.execute(sql).fetchdf()
    return {int(row["team_id"]): {c: row[c] for c in df.columns if c != "team_id"}
            for _, row in df.iterrows()}

def serve_match(con, match_id, compo, xi_positions):
    """Assemble le vecteur complet du match pour les 2 équipes :
    lit la ligne mart existante (204 hors-compo + 24 rolling), puis remplace les
    28 features de compo par le recompute depuis `compo`. Rend le DataFrame prêt
    pour prepare_x/predict.
      compo        = {team_id: {"xi": [ids...], "gk": id}, opp_id: {...}}
      xi_positions = [(team_id, player_id, grid_horizontal), ...] (les 2 équipes)
    """
    df = con.execute("select * from marts.mart_1n2 where match_id=? order by team_id",
                     [match_id]).fetchdf()
    teams = list(df["team_id"])
    zonal = zonal_features(con, match_id, xi_positions)
    for i, t in enumerate(teams):
        opp = teams[1 - i]
        season = df.loc[df.team_id == t, "season"].iloc[0]
        feats = match_compo_features(con, match_id, t, opp, compo, season)
        feats.update(zonal.get(t, {}))
        for k, v in feats.items():
            df.loc[df.team_id == t, k] = v
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
    out = df[["match_id", "team_id"]].copy()
    for i, cls in enumerate(payload["model"].classes_):
        out[f"prob_{inv[cls]}"] = proba[:, i]
    return out

def _norm(s):
    """Minuscule + sans accents, pour comparer les noms de façon robuste."""
    return "".join(c for c in unicodedata.normalize("NFD", s.lower().strip())
                   if unicodedata.category(c) != "Mn")


def build_template(con):
    """Gabarit formation_id → {slot: grid_horizontal}, déterministe (validé).
    Fournit la position de chaque joueur à partir de son slot dans la formation."""
    tmpl = {}
    for fid, pos in con.execute("""select formation_id, any_value(formation_positions)
        from intermediate.int_whoscored_formations group by formation_id""").fetchall():
        p = json.loads(pos)
        tmpl[fid] = {i + 1: float(p[i]["horizontal"]) for i in range(min(11, len(p)))}
    return tmpl


def resolve_player(con, name, team_id, season):
    """Nom → player_id, restreint à (équipe, saison). Exact d'abord, puis 'contient'.
    Lève une erreur claire si 0 ou >1 correspondance (homonyme / graphie)."""
    rows = con.execute("""select player_id, player_name from intermediate.int_whoscored_players
        where team_id=? and season=?""", [team_id, season]).fetchall()
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
    mid = spec["match_id"]
    venue = {("home" if v == "Home" else "away"): t for t, v in con.execute(
        "select team_id, venue from intermediate.backbone where match_id=?", [mid]).fetchall()}
    season = con.execute("select any_value(season) from marts.mart_1n2 where match_id=?",
                         [mid]).fetchone()[0]
    tmpl = build_template(con)
    compo, xi_pos = {}, []
    for side in ("home", "away"):
        t, fid = venue[side], spec[side]["formation_id"]
        ids = [resolve_player(con, nm, t, season) for nm in spec[side]["xi"]]
        compo[t] = {"xi": ids, "gk": ids[0]}           # slot 1 = gardien
        for slot, pid in enumerate(ids, start=1):
            xi_pos.append((t, pid, tmpl[fid][slot]))
    return mid, compo, xi_pos