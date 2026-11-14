"""
player_data.py — couche de données de l'explorateur de joueurs (app/player_explorer.py).

Toutes les fonctions prennent une CONNEXION DuckDB (ouverte en lecture seule, fermée aussitôt par l'appelant) :
l'application ne garde jamais la base verrouillée, dbt peut tourner pendant qu'elle est ouverte.

Sources :
  · catalogue saison / championnat / club / joueur : intermediate_player_match_event_stats + intermediate_whoscored_player_team_season
  · infos : taille, poids, poste, titularisations (player_team_season) ; âge (silver.stg_whoscored_player_match via le pont des matchs)
  · stats de saison : event_stats, passes, finition, engagement défensif, tirs (buts)
  · caractérisation : gold_player_behaviour_profile (z-scores contre les pairs du même poste, de la même saison et ligue),
    machine_learning.player_trait_types et player_trait_type_labels
  · joueurs similaires : distance dans l'espace du type global (pipelines/features/trait_types.global_features)
Pas de nationalité dans la base. Pas de profil gardien : un gardien s'affiche sans caractérisation.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("trait_types", ROOT / "pipelines" / "features" / "trait_types.py")
tt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tt)

ROLES_FR = {"GK": "Gardien", "CB": "Défenseur central", "FB": "Latéral", "MID": "Milieu",
            "WIDE": "Ailier / milieu excentré", "ST": "Attaquant"}
ROLES_FIN_FR = {"GK": "Gardien", "CB": "Défenseur central", "FB": "Latéral", "DM": "Milieu défensif",
                "CM": "Milieu central", "AM": "Milieu offensif", "WM": "Milieu latéral", "W": "Ailier", "ST": "Avant-centre"}
FAMILIES_FR = {"aerien": "Jeu aérien", "sacrifice": "Sacrifice défensif", "duel_sol": "Duels au sol",
               "creation": "Création", "distribution": "Distribution", "finition": "Finition",
               "coups_arretes": "Coups de pied arrêtés", "reseau": "Rôle dans le réseau de passes", "global": "Profil global"}
FEATURES_FR = {
    "aerial_duels": "Duels aériens", "aerial_win_rate": "Duels aériens gagnés (%)",
    "a_woe_net_off": "Duels aériens offensifs (au-dessus de l'attendu)", "a_woe_net_def": "Duels aériens défensifs (au-dessus de l'attendu)",
    "a_head_shots_per_match": "Tirs de la tête par match",
    "shots_blocked": "Tirs bloqués", "passes_blocked": "Passes bloquées", "crosses_blocked": "Centres bloqués",
    "def_actions_own_box": "Actions défensives dans sa surface", "def_own_box_share": "Part des actions défensives dans sa surface",
    "clearances": "Dégagements", "tackles": "Tacles", "interceptions": "Interceptions",
    "g_dribbles_per_90": "Dribbles tentés", "g_dribble_woe_adj": "Réussite des dribbles (ajustée)",
    "g_defended_woe_adj": "Réussite en défense face aux dribbleurs", "g_dispossessed_share": "Part de duels perdus par dépossession",
    "g_fouls_committed_per_90": "Fautes commises", "g_fouls_drawn_per_90": "Fautes subies", "g_yellow_per_foul": "Jaunes par faute",
    "tackle_win_rate": "Tacles gagnés (%)", "challenges": "Duels défensifs", "recoveries": "Récupérations",
    "key_passes": "Passes clés", "chances_created": "Occasions créées", "xt_created": "xT créé", "crosses": "Centres",
    "throughballs": "Passes en profondeur", "xg_contribution": "Contribution xG", "shots": "Tirs",
    "passes_open": "Passes (jeu ouvert)", "pass_accuracy": "Précision des passes", "pass_forward_share": "Part de passes vers l'avant",
    "pass_long_share": "Part de passes longues", "pass_short_share": "Part de passes courtes",
    "pass_length_mean": "Longueur moyenne des passes", "progressive_passes": "Passes progressives", "longballs": "Longs ballons",
    "xt_per_pass": "xT par passe", "passes_into_final_third": "Passes dans le dernier tiers", "touches": "Touches",
    "shots_in_box": "Tirs dans la surface", "shots_outside_box": "Tirs hors de la surface", "shot_dist_mean": "Distance moyenne de tir",
    "shot_head_share": "Part de tirs de la tête", "shot_big_chance_share": "Part de grosses occasions",
    "shots_on_target": "Tirs cadrés", "goal_conversion": "Conversion des tirs", "shot_on_target_rate": "Taux de cadrage",
    "corners_taken": "Corners tirés", "freekicks_played": "Coups francs joués", "freekicks_direct_shots": "Tirs de coup franc direct",
    "penalties_taken": "Penaltys tirés", "net_pass_share": "Part des passes de l'équipe",
    "net_creative_rate": "Part de passes créatives", "net_betweenness": "Centralité dans le réseau de passes",
}
SECTORS = [("att", "Tiers offensif"), ("mid", "Tiers médian"), ("def", "Tiers défensif")]
# ATTENTION : dans les données (repère Opta/WhoScored, y faible = flanc DROIT du sens d'attaque), la colonne « left »
# des tables gold correspond au flanc DROIT réel (ex. Vinícius Jr, ailier gauche : 68 % en « right » ; Saka, Salah, Yamal,
# ailiers droits : 61-69 % en « left »). L'affichage inverse donc les libellés ; les noms de colonnes restent inchangés.
SIDES = [("right", "Gauche"), ("centre", "Centre"), ("left", "Droite")]
MIN_PEER_MINUTES = 450


def feature_label(name: str) -> str:
    return FEATURES_FR.get(name, name)


def translate_description(text: str) -> str:
    """Remplace les noms de variables d'une description de type par leur libellé français."""
    for name in sorted(FEATURES_FR, key=len, reverse=True):
        text = re.sub(rf"(?<![A-Za-z_]){re.escape(name)}(?![A-Za-z_])", FEATURES_FR[name], text)
    return text.replace("haut :", "valeurs les plus hautes :").replace("bas :", "valeurs les plus basses :")


# ── Catalogue ─────────────────────────────────────────────────────────────────────────────────
def load_catalog(con) -> pd.DataFrame:
    """Une ligne par (saison, championnat, club, joueur) : minutes et matchs joués."""
    return con.execute("""
        SELECT e.str_season AS season, e.str_league_source AS league, e.str_team_id AS team_id,
               COALESCE(MAX(t.str_team_name), 'Club ' || e.str_team_id) AS team,
               e.str_player_id AS player_id,
               COALESCE(MAX(t.str_player_name), 'Joueur ' || e.str_player_id) AS player,
               SUM(e.int_minutes_played) AS minutes, COUNT(*) AS matches
        FROM intermediate.intermediate_player_match_event_stats e
        LEFT JOIN intermediate.intermediate_whoscored_player_team_season t
          ON t.str_player_id = e.str_player_id AND t.str_season = e.str_season AND t.str_team_id = e.str_team_id
        WHERE e.int_minutes_played > 0
        GROUP BY e.str_season, e.str_league_source, e.str_team_id, e.str_player_id
    """).df()


# ── Infos de base ─────────────────────────────────────────────────────────────────────────────
def player_info(con, player_id: str, season: str, team_id: str) -> dict:
    """Taille, poids, poste, titularisations (player_team_season), poste fin modal (rôle des matchs), âge."""
    row = con.execute("""
        SELECT str_macro_position, int_height, int_weight, int_n_matchs, int_n_starts
        FROM intermediate.intermediate_whoscored_player_team_season
        WHERE str_player_id = ? AND str_season = ? AND str_team_id = ?
        LIMIT 1""", [player_id, season, team_id]).fetchone()
    info = {"macro_position": None, "height": None, "weight": None, "matches": None, "starts": None,
            "role_fin": None, "age": None}
    if row:
        info.update(macro_position=row[0], height=row[1], weight=row[2], matches=row[3], starts=row[4])
    r = con.execute("""
        SELECT str_role_fin_resolved FROM intermediate.intermediate_player_match_role
        WHERE str_player_id = ? AND str_season = ? AND str_team_id = ? AND str_role_fin_resolved IS NOT NULL
        GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1""", [player_id, season, team_id]).fetchone()
    if r:
        info["role_fin"] = r[0]
    a = con.execute("""
        SELECT MAX(p.age) FROM silver.stg_whoscored_player_match p
        JOIN intermediate.intermediate_whoscored_match_bridge b ON b.str_ws_match_id = p.ws_match_id
        WHERE p.player_id = TRY_CAST(? AS INTEGER) AND b.str_season = ? AND p.age BETWEEN 14 AND 50""",
                    [player_id, season]).fetchone()
    if a and a[0] is not None:
        info["age"] = int(a[0])
    return info


# ── Stats de la saison ────────────────────────────────────────────────────────────────────────
STAT_SQL = """
    SELECT COUNT(*) AS matches, SUM(e.int_minutes_played) AS minutes,
           SUM(e.int_n_shots) AS shots, SUM(e.int_n_assists) AS assists, SUM(e.int_n_key_passes) AS key_passes,
           SUM(e.int_n_chances_created) AS chances_created, SUM(e.dec_xg_contribution) AS xg_contribution,
           SUM(e.int_n_progressive_passes) AS progressive_passes, SUM(e.int_n_touches) AS touches,
           SUM(e.int_n_tackles) AS tackles, SUM(e.int_n_tackles_won) AS tackles_won,
           SUM(e.int_n_interceptions) AS interceptions, SUM(e.int_n_clearances) AS clearances,
           SUM(e.int_n_ball_recoveries) AS recoveries,
           SUM(e.int_n_aerial_duels) AS aerial_duels, SUM(e.int_n_aerial_won) AS aerial_won,
           SUM(e.int_n_yellow_cards) AS yellow_cards, SUM(e.int_n_red_cards) AS red_cards,
           SUM(p.int_passes_open) AS passes_open, SUM(p.int_passes_open_completed) AS passes_completed,
           SUM(p.dec_xt_created) AS xt_created,
           SUM(f.int_shots_on_target) AS shots_on_target
    FROM intermediate.intermediate_player_match_event_stats e
    LEFT JOIN intermediate.intermediate_player_match_passes p
      ON p.str_match_id = e.str_match_id AND p.str_team_id = e.str_team_id AND p.str_player_id = e.str_player_id
    LEFT JOIN intermediate.intermediate_player_match_finishing f
      ON f.str_match_id = e.str_match_id AND f.str_team_id = e.str_team_id AND f.str_player_id = e.str_player_id
    WHERE e.str_player_id = ? AND e.str_season = ? AND e.str_team_id = ? AND e.int_minutes_played > 0
"""


def season_stats(con, player_id: str, season: str, team_id: str) -> dict:
    """Cumuls de la saison pour un joueur dans un club (buts compris, penaltys compris, csc exclus)."""
    cur = con.execute(STAT_SQL, [player_id, season, team_id])
    s = dict(zip([d[0] for d in cur.description], cur.fetchone()))
    g = con.execute("""
        SELECT COUNT(*) FILTER (WHERE bool_is_goal AND NOT COALESCE(bool_is_own_goal, FALSE))
        FROM intermediate.intermediate_shots WHERE str_player_id = ? AND str_season = ? AND str_team_id = ?""",
                    [player_id, season, team_id]).fetchone()
    s["goals"] = int(g[0]) if g and g[0] is not None else 0
    return {k: (0 if v is None else v) for k, v in s.items()}


def stats_table(s: dict, starts=None) -> pd.DataFrame:
    """Tableau d'affichage : statistique, total, par 90 minutes."""
    m = float(s.get("minutes") or 0)
    per90 = (lambda v: round(v * 90.0 / m, 2) if m > 0 else None)
    comp = lambda a, b: f"{int(s[a])} / {int(s[b])}"
    rows = [("Matchs joués", int(s["matches"]), None),
            ("Titularisations", None if starts is None else int(starts), None),
            ("Minutes", int(m), None),
            ("Buts (penaltys compris)", s["goals"], per90(s["goals"])),
            ("Passes décisives", int(s["assists"]), per90(s["assists"])),
            ("Tirs", int(s["shots"]), per90(s["shots"])),
            ("Tirs cadrés (hors penaltys)", int(s["shots_on_target"]), per90(s["shots_on_target"])),
            ("Contribution xG", round(s["xg_contribution"], 2), per90(s["xg_contribution"])),
            ("Passes de jeu ouvert", int(s["passes_open"]), per90(s["passes_open"])),
            ("Passes réussies (jeu ouvert)", int(s["passes_completed"]), per90(s["passes_completed"])),
            ("Précision des passes (%)", round(100.0 * s["passes_completed"] / s["passes_open"], 1) if s["passes_open"] else None, None),
            ("Passes clés", int(s["key_passes"]), per90(s["key_passes"])),
            ("Passes progressives", int(s["progressive_passes"]), per90(s["progressive_passes"])),
            ("xT créé par les passes", round(s["xt_created"], 2), per90(s["xt_created"])),
            ("Touches", int(s["touches"]), per90(s["touches"])),
            ("Tacles gagnés / tentés", comp("tackles_won", "tackles"), per90(s["tackles"])),
            ("Interceptions", int(s["interceptions"]), per90(s["interceptions"])),
            ("Dégagements", int(s["clearances"]), per90(s["clearances"])),
            ("Récupérations", int(s["recoveries"]), per90(s["recoveries"])),
            ("Duels aériens gagnés / total", comp("aerial_won", "aerial_duels"), per90(s["aerial_duels"])),
            ("Cartons jaunes", int(s["yellow_cards"]), None), ("Cartons rouges", int(s["red_cards"]), None)]
    df = pd.DataFrame(rows, columns=["Statistique", "Total", "Par 90 min"])
    # Colonne homogène en texte (mélange nombres / « 1 / 4 » incompatible avec Arrow)
    df["Total"] = df["Total"].map(lambda v: "–" if v is None or (isinstance(v, float) and v != v) else str(v))
    df["Par 90 min"] = df["Par 90 min"].map(lambda v: "–" if v is None or (isinstance(v, float) and v != v) else f"{v:.2f}")
    return df


# ── Caractérisation ───────────────────────────────────────────────────────────────────────────
def _z_cols() -> list[str]:
    return [f"dec_z_{n}" for ns in tt.FAMILIES.values() for n in ns]


def load_profiles(con) -> pd.DataFrame:
    """Profil de comportement (fenêtre de 3 saisons terminant en `str_season`) + types, une ligne par joueur et saison."""
    z = ", ".join(f"p.{c}" for c in _z_cols())
    sec = ", ".join(f"p.dec_sec_{d}_{s}" for d, _ in SECTORS for s, _ in SIDES)
    ty = ", ".join(f"t.int_type_{f}, t.dec_dist_{f}" for f in list(tt.FAMILIES) + ["global"])
    return con.execute(f"""
        SELECT p.str_player_id, p.str_season, p.str_macro_role, p.str_league_source, p.int_minutes, p.int_n_matches,
               p.bool_is_peer, {z}, {sec}, {ty}
        FROM gold.gold_player_behaviour_profile p
        LEFT JOIN machine_learning.player_trait_types t
          ON t.str_player_id = p.str_player_id AND t.str_season = p.str_season
    """).df()


def load_type_labels(con) -> pd.DataFrame:
    d = con.execute("SELECT str_family, int_type, str_description FROM machine_learning.player_trait_type_labels").df()
    d["description_fr"] = d["str_description"].map(translate_description)
    return d


def player_types(row: pd.Series, labels: pd.DataFrame) -> pd.DataFrame:
    """Types du joueur dans chaque famille et en global, avec la description française du type."""
    out = []
    for fam in list(tt.FAMILIES) + ["global"]:
        t = row.get(f"int_type_{fam}")
        if t is None or pd.isna(t):
            continue
        lab = labels[(labels.str_family == fam) & (labels.int_type == int(t))]
        out.append({"Famille": FAMILIES_FR[fam], "Type": int(t),
                    "Description": lab.description_fr.iloc[0] if len(lab) else ""})
    return pd.DataFrame(out, columns=["Famille", "Type", "Description"])


def family_scores(row: pd.Series) -> pd.DataFrame:
    """z-scores du joueur, une ligne par variable, avec sa famille."""
    rows = [{"Famille": FAMILIES_FR[f], "Variable": feature_label(n), "name": n, "z": row.get(f"dec_z_{n}")}
            for f, ns in tt.FAMILIES.items() for n in ns]
    return pd.DataFrame(rows)


# ── Joueurs similaires ────────────────────────────────────────────────────────────────────────
def _space(df: pd.DataFrame) -> np.ndarray:
    return tt.global_features(df)[0]


def similar_players(profiles: pd.DataFrame, catalog: pd.DataFrame, player_id: str, season: str,
                    same_role: bool = True, per_league: int = 1) -> pd.DataFrame:
    """Joueurs les plus proches dans l'espace du type global, au plus `per_league` par championnat.

    Candidats : même saison (fenêtre se terminant à cette saison), joueurs de champ avec au moins 450 minutes ; poste identique si
    `same_role`. Le championnat est celui du club de la saison. `percentile` : part des candidats plus éloignés que ce voisin
    (99 = plus proche que 99 % des candidats). ▲ / ▼ : variable où les deux joueurs sont tous deux au-dessus / en dessous des pairs."""
    cur = profiles[(profiles.str_season == season) & profiles.str_player_id.eq(player_id)]
    if cur.empty:
        return pd.DataFrame()
    me = cur.iloc[0]
    cand = profiles[(profiles.str_season == season) & profiles.bool_is_peer & (profiles.str_macro_role != "GK")
                    & (profiles.str_player_id != player_id)].copy()
    if same_role:
        cand = cand[cand.str_macro_role == me.str_macro_role]
    if cand.empty:
        return pd.DataFrame()
    xa, xc = _space(cur.iloc[[0]])[0], _space(cand)
    cand["distance"] = np.linalg.norm(xc - xa, axis=1)
    cand["percentile"] = 100.0 * (1.0 - cand["distance"].rank(method="min") / len(cand))
    club = catalog[catalog.season == season].sort_values("minutes", ascending=False) \
        .drop_duplicates("player_id")[["player_id", "player", "team", "league"]]
    cand = cand.merge(club, left_on="str_player_id", right_on="player_id", how="inner").sort_values("distance")
    out = cand.groupby("league", sort=False).head(per_league).copy()
    za = me[_z_cols()].astype(float).fillna(0.0).clip(-tt.Z_CLIP, tt.Z_CLIP)
    commons, diffs = [], []
    for _, r in out.iterrows():
        zb = r[_z_cols()].astype(float).fillna(0.0).clip(-tt.Z_CLIP, tt.Z_CLIP)
        same = np.sign(za.values) == np.sign(zb.values)
        score = np.where(same, np.minimum(np.abs(za.values), np.abs(zb.values)), 0.0)
        names = [c[len("dec_z_"):] for c in _z_cols()]
        commons.append(", ".join(("▲ " if za.values[i] > 0 else "▼ ") + feature_label(names[i])
                                 for i in np.argsort(-score)[:3] if score[i] > 0.3))
        diffs.append(", ".join(feature_label(names[i]) for i in np.argsort(-np.abs(za.values - zb.values))[:2]))
    out["points_communs"], out["plus_grands_ecarts"] = commons, diffs
    return out[["league", "player", "team", "str_macro_role", "distance", "percentile", "int_type_global",
                "points_communs", "plus_grands_ecarts", "str_player_id"]].reset_index(drop=True)


# ── Types : exemples et contrôle de cohérence ─────────────────────────────────────────────────
def representative_players(profiles: pd.DataFrame, catalog: pd.DataFrame, season: str, family: str, type_id: int,
                           n: int = 10, min_minutes: int = 1500) -> pd.DataFrame:
    """Joueurs les plus proches du centre d'un type (distance au centre la plus faible), saison donnée."""
    d = profiles[(profiles.str_season == season) & (profiles[f"int_type_{family}"] == type_id)
                 & (profiles.int_minutes >= min_minutes)].sort_values(f"dec_dist_{family}").head(n)
    club = catalog[catalog.season == season].sort_values("minutes", ascending=False) \
        .drop_duplicates("player_id")[["player_id", "player", "team"]]
    out = d.merge(club, left_on="str_player_id", right_on="player_id", how="left")
    return out[["player", "team", "str_league_source", "str_macro_role", f"dec_dist_{family}"]]


def type_role_mix(profiles: pd.DataFrame, season: str, family: str) -> pd.DataFrame:
    """Part de chaque poste dans chaque type, saison donnée."""
    d = profiles[(profiles.str_season == season) & profiles[f"int_type_{family}"].notna()]
    ct = pd.crosstab(d[f"int_type_{family}"].astype(int), d.str_macro_role, normalize="index")
    return ct


def neighbour_coherence(profiles: pd.DataFrame, season: str, sample: int = 400, seed: int = 0) -> dict:
    """Contrôle : le plus proche voisin (tous postes confondus) a-t-il le même poste ? Comparé au hasard."""
    d = profiles[(profiles.str_season == season) & profiles.bool_is_peer & (profiles.str_macro_role != "GK")].reset_index(drop=True)
    if len(d) < 50:
        return {}
    x = _space(d)
    idx = np.random.default_rng(seed).choice(len(d), min(sample, len(d)), replace=False)
    dist = np.linalg.norm(x[idx][:, None, :] - x[None, :, :], axis=2)
    dist[np.arange(len(idx)), idx] = np.inf
    nn = dist.argmin(axis=1)
    share = float((d.str_macro_role.values[idx] == d.str_macro_role.values[nn]).mean())
    p = d.str_macro_role.value_counts(normalize=True).values
    return {"share_same_role": share, "chance": float((p ** 2).sum()), "n": int(len(idx))}
