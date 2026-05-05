"""
features/columns.py — Registre centralisé des colonnes Gold
============================================================
Source unique de vérité pour toutes les listes de colonnes ajoutées
aux tables gold.features_training et gold.features_final.

Utilisé par :
  - features/whoscored.py   (NEW_COLS_WS, DIFF_COLS_WS)
  - features/draw.py        (NEW_COLS_DRAW_BEHAVIOR, NEW_COLS_DRAW_SIGNALS, DIFF_COLS_DRAW)
  - 04_train.py             → importer FEATURE_COLS pour éviter la liste en dur

Convention de nommage :
  (col_name, sql_type)  → directement utilisable dans ALTER TABLE / ADD COLUMN IF NOT EXISTS
"""

# ══════════════════════════════════════════════════════════════════════════════
# MODULE : WhoScored Events (features/whoscored.py — ex-03b)
# ══════════════════════════════════════════════════════════════════════════════

# Colonnes ajoutées à gold.features_training
NEW_COLS_WS: list[tuple[str, str]] = [
    # ── Bloc A : Pépites (v1 — inchangées) ───────────────────────────────────
    ("ws_field_tilt_actions",  "DOUBLE"),  # % touches en zone off. (x > 66)
    ("ws_high_turnover_rate",  "DOUBLE"),  # pertes balle zone haute
    ("ws_deep_completion_rt",  "DOUBLE"),  # passes réussies end_x > 83
    ("ws_momentum_delta",      "DOUBLE"),  # résilience post-but encaissé
    ("ws_counter_shot_rate",   "DOUBLE"),  # tirs en contre / total tirs
    ("ws_set_piece_pressure",  "DOUBLE"),  # phases arrêtées off. / actions off.

    # ── Bloc B : Positional Report — Attack Sides (axe y) ────────────────────
    ("ws_attack_left_pct",     "DOUBLE"),  # % actions offensives côté gauche  (y < 33.3)
    ("ws_attack_center_pct",   "DOUBLE"),  # % actions offensives axe central  (33.3 ≤ y ≤ 66.6)
    ("ws_attack_right_pct",    "DOUBLE"),  # % actions offensives côté droit   (y > 66.6)

    # ── Bloc C : Positional Report — Action Zones (axe x) ────────────────────
    ("ws_zone_def_pct",        "DOUBLE"),  # % touches en bloc défensif        (x < 33.3)
    ("ws_zone_mid_pct",        "DOUBLE"),  # % touches en milieu de terrain    (33.3 ≤ x ≤ 66.6)
    ("ws_zone_att_pct",        "DOUBLE"),  # % touches en bloc offensif        (x > 66.6)

    # ── Bloc D : Shot Zones (combinaison x/y) ────────────────────────────────
    ("ws_shot_six_yard_pct",   "DOUBLE"),  # % tirs depuis la cage 6m          (x > 94, y ∈ [36,64])
    ("ws_shot_penalty_pct",    "DOUBLE"),  # % tirs depuis la surface de répar. (x > 83, y ∈ [21,79])
    ("ws_shot_oob_pct",        "DOUBLE"),  # % tirs hors surface               (le reste)

    # ── Bloc E : Attempt Types (situations) ──────────────────────────────────
    ("ws_shot_open_play_pct",  "DOUBLE"),  # % tirs en jeu ouvert
    ("ws_shot_set_piece_pct",  "DOUBLE"),  # % tirs sur phase arrêtée (FK/corner)
    ("ws_shot_penalty_att_pct","DOUBLE"),  # % tirs = penaltys
    ("ws_conversion_rate",     "DOUBLE"),  # buts / total tirs (efficacité brute)

    # ── Bloc F : Pass Types (style) ──────────────────────────────────────────
    ("ws_cross_rate",          "DOUBLE"),  # centres / total passes
    ("ws_through_ball_rate",   "DOUBLE"),  # through balls / total passes
    ("ws_long_ball_rate",      "DOUBLE"),  # longues balles / total passes
    ("ws_short_pass_rate",     "DOUBLE"),  # passes courtes / total passes (résiduel)

    # ── Bloc G : Defensive Exposure (v2) ─────────────────────────────────────
    ("ws_def_exposed_left_pct",   "DOUBLE"),  # % actions adverses sur notre gauche  (y < 33.3)
    ("ws_def_exposed_center_pct", "DOUBLE"),  # % actions adverses dans notre axe    (33.3 ≤ y ≤ 66.6)
    ("ws_def_exposed_right_pct",  "DOUBLE"),  # % actions adverses sur notre droite  (y > 66.6)

    # ── Qualité de données (v2) ───────────────────────────────────────────────
    ("has_ws_events",             "INTEGER"),  # 0 / 1 — couverture WhoScored events
]

# Colonnes différentielles ajoutées à gold.features_final
DIFF_COLS_WS: list[tuple[str, str]] = [
    # Pépites diffs (v1)
    ("ws_turnover_zone_diff",  "DOUBLE"),
    ("ws_deep_pass_diff",      "DOUBLE"),
    ("ws_momentum_diff",       "DOUBLE"),
    ("ws_counter_threat_diff", "DOUBLE"),
    # Classics diffs (v2)
    ("ws_attack_width_diff",   "DOUBLE"),  # center_pct team - center_pct opp
    ("ws_zone_att_diff",       "DOUBLE"),  # zone_att_pct diff
    ("ws_shot_zone_diff",      "DOUBLE"),  # shot_penalty_pct diff
    ("ws_conversion_diff",     "DOUBLE"),  # conversion_rate diff
    ("ws_cross_diff",          "DOUBLE"),  # cross_rate diff
    ("ws_long_ball_diff",      "DOUBLE"),  # long_ball_rate diff
    # Matchup advantages — soustraction (v2)
    ("ws_left_matchup_adv",    "DOUBLE"),  # team.attack_left  - opp.def_exposed_right
    ("ws_right_matchup_adv",   "DOUBLE"),  # team.attack_right - opp.def_exposed_left
    ("ws_center_matchup_adv",  "DOUBLE"),  # team.attack_center - opp.def_exposed_center
    # Matchup structurel — produit (exploitation réelle) — Groupe 3
    ("ws_structural_matchup",    "DOUBLE"),  # dot product att_team · def_opp normalisé
    ("ws_left_exploit_score",    "DOUBLE"),  # attack_left_team  × def_exposed_left_opp
    ("ws_center_exploit_score",  "DOUBLE"),  # attack_center_team × def_exposed_center_opp
    ("ws_right_exploit_score",   "DOUBLE"),  # attack_right_team × def_exposed_right_opp
    # Comportementaux avancés diffs (v3)
    ("ws_counter_attack_diff",   "DOUBLE"),  # ws_counter_attack_dna team - opp
    ("ws_def_line_diff",         "DOUBLE"),  # ws_defensive_line_height team - opp
    ("ws_flank_asymm_diff",      "DOUBLE"),  # ws_flank_exposure_asymm team - opp
]


# ══════════════════════════════════════════════════════════════════════════════
# MODULE : Draw Behavior (features/draw.py — ex-03c draw_behavior)
# ══════════════════════════════════════════════════════════════════════════════

# Colonnes ajoutées à gold.features_training (Bloc H)
NEW_COLS_DRAW_BEHAVIOR: list[tuple[str, str]] = [
    ("ws_late_equalizer_rate",          "DOUBLE"),  # % matchs avec égalisateur >70min quand menés
    ("ws_post_yellowcard_concede_rate",  "DOUBLE"),  # % matchs où on concède dans les 10min après un jaune
    ("ws_post_redcard_resilience",       "DOUBLE"),  # ratio actions offensives post/pré rouge reçu
]


# ══════════════════════════════════════════════════════════════════════════════
# MODULE : Draw Signals (features/draw.py — ex-03c draw_signals)
# ══════════════════════════════════════════════════════════════════════════════

# Colonnes ajoutées à gold.features_training (F1–F20)
NEW_COLS_DRAW_SIGNALS: list[tuple[str, str]] = [
    # ── AXE 1 — Détecteurs Nul ───────────────────────────────────────────────
    ("f1_mutual_cancel_idx",       "DOUBLE"),  # stérilité croisée × save_rate
    ("f2_defensive_mirror",        "DOUBLE"),  # alignement axe attaque vs défense adverse
    ("f3_draw_market_dev",         "DOUBLE"),  # pinnacle_draw - league_draw_rate
    ("f4_momentum_convergence",    "DOUBLE"),  # |momentum_delta_team - momentum_delta_opp|
    ("f5_cs_mutual_rate",          "DOUBLE"),  # clean_sheet_rate_5 × cs_rate_5_opp
    ("f6_ht_draw_tendency",        "DOUBLE"),  # % matchs récents : égalité mi-temps ET nul final

    # ── AXE 2 — Domination Relative ──────────────────────────────────────────
    ("f7_off_def_mismatch",        "DOUBLE"),  # season_att_team - season_def_opp
    ("f7_def_off_mismatch",        "DOUBLE"),  # season_att_opp  - season_def_team
    ("f8_press_dominance_ratio",   "DOUBLE"),  # log(opp_ppda / team_ppda)
    ("f9_chance_quality_gap",      "DOUBLE"),  # sqr_5_team - sqr_5_opp
    ("f10_venue_power_adj",        "DOUBLE"),  # xG_venue_5 - xG_global_5

    # ── AXE 3 — Résilience & Psychologie ─────────────────────────────────────
    ("f11_comeback_rate",          "DOUBLE"),  # % matchs récents avec retour au score
    ("f12_red_card_resilience",    "DOUBLE"),  # pts gagnés / matchs avec carton rouge récent
    ("f13_late_goal_tendency",     "DOUBLE"),  # % buts après la 75e (source WhoScored events)
    ("f14_goal_timing_variance",   "DOUBLE"),  # écart-type minute des buts marqués (WS events)

    # ── AXE 4 — Yield / Efficacité ───────────────────────────────────────────
    ("f15_xg_yield_ratio",         "DOUBLE"),  # gf_5 / np_xg_5 (surperformance offensive)
    ("f16_def_yield_ratio",        "DOUBLE"),  # ga_5 / np_xg_conceded_5 (surperformance défensive)
    ("f17_shots_to_goal_eff",      "DOUBLE"),  # gf_5 / shots_total_5
    ("f18_sot_conversion",         "DOUBLE"),  # gf_5 / shots_on_target_5

    # ── AXE 5 — Composites Signatures ────────────────────────────────────────
    ("f19_tactical_lock_idx",      "DOUBLE"),  # triple verrou : stérilité × pressing × territoire
    ("f20_upset_composite",        "DOUBLE"),  # (1/prob_team) × yield_adverse × comeback_opp
]

# Colonnes différentielles dans gold.features_final pour Draw Signals
DIFF_COLS_DRAW: list[tuple[str, str]] = [
    ("f1_mutual_cancel_diff",      "DOUBLE"),
    ("f7_mismatch_diff",           "DOUBLE"),
    ("f8_press_dominance_diff",    "DOUBLE"),
    ("f9_chance_quality_diff",     "DOUBLE"),
    ("f10_venue_power_diff",       "DOUBLE"),
    ("f11_comeback_diff",          "DOUBLE"),
    ("f13_late_goal_diff",         "DOUBLE"),
    ("f15_xg_yield_diff",          "DOUBLE"),
    ("f16_def_yield_diff",         "DOUBLE"),
    ("f19_tactical_lock_diff",     "DOUBLE"),
    ("f20_upset_diff",             "DOUBLE"),
]


# ══════════════════════════════════════════════════════════════════════════════
# MODULE : Fatigue / Contexte (features/rolling.py — Blocs 2 et 3) — Groupe 5
# ══════════════════════════════════════════════════════════════════════════════

NEW_COLS_FATIGUE: list[tuple[str, str]] = [
    ("congestion_14d",  "INTEGER"),  # nb matchs dans les 14 jours précédents (anti-leakage : date < ft.date)
]

DIFF_COLS_FATIGUE: list[tuple[str, str]] = [
    ("rest_days_diff",  "DOUBLE"),   # days_since_last_match_team - days_since_last_match_opp
]


# ══════════════════════════════════════════════════════════════════════════════
# MODULE : Draw Rate & Home Strength (features/rolling.py — Blocs 2 et 3) — Groupe 4
# ══════════════════════════════════════════════════════════════════════════════════════

NEW_COLS_DRAW_RATE: list[tuple[str, str]] = [
    ("draw_rate_5",        "DOUBLE"),  # taux de nuls sur les 5 derniers matchs (rolling)
    ("home_win_rate_hist", "DOUBLE"),  # taux de victoires à domicile (historique complet, LAG strict)
]

DIFF_COLS_DRAW_RATE: list[tuple[str, str]] = [
    ("draw_rate_diff",  "DOUBLE"),   # draw_rate_5_team - draw_rate_5_opp
    ("draw_affinity",   "DOUBLE"),   # draw_rate_5_team × draw_rate_5_opp (compatibilité nul)
]


# ══════════════════════════════════════════════════════════════════════════════
# MODULE : Formation tactique (features/rolling.py — Blocs 2 et 3) — Groupe 2
# ══════════════════════════════════════════════════════════════════════════════

NEW_COLS_FORMATION: list[tuple[str, str]] = [
    ("form_n_defenders",    "INTEGER"),  # 1er chiffre de la formation (ex: 4 dans "4-3-3")
    ("form_n_midfielders",  "INTEGER"),  # chiffre(s) central/aux (somme si 4 blocs, ex: 4-2-3-1 → 5)
    ("form_n_attackers",    "INTEGER"),  # dernier chiffre de la formation
    ("form_familiarity_5",  "DOUBLE"),   # % des 5 derniers matchs joués avec cette même formation
    ("form_change_flag",    "INTEGER"),  # 1 si formation différente du match précédent, sinon 0
]

DIFF_COLS_FORMATION: list[tuple[str, str]] = [
    ("form_att_vs_def_gap",  "DOUBLE"),   # n_attackers_team - n_defenders_opp
    ("form_mid_dominance",   "DOUBLE"),   # n_midfielders_team - n_midfielders_opp
]




# ══════════════════════════════════════════════════════════════════════════════
# MODULE : Squad / Joueurs (features/whoscored.py — Passe 0 + Passe 3) — Groupe 1
# ══════════════════════════════════════════════════════════════════════════════

NEW_COLS_SQUAD: list[tuple[str, str]] = [
    ("squad_avg_form_5",   "DOUBLE"),  # AVG(n_actions rolling 5) des joueurs du squad
    ("squad_xg_quality_5", "DOUBLE"),  # AVG(xg_contribution rolling 5) des joueurs du squad
    ("squad_regularity",   "DOUBLE"),  # % joueurs présents dans M qui étaient dans M-1
    ("squad_top3_share",   "DOUBLE"),  # part actions 3 meilleurs contributors / total squad
]

DIFF_COLS_SQUAD: list[tuple[str, str]] = [
    ("squad_quality_gap",  "DOUBLE"),  # squad_avg_form_5_team - squad_avg_form_5_opp
    ("squad_xg_matchup",   "DOUBLE"),  # squad_xg_quality_5_team / squad_xg_quality_5_opp
]

# ══════════════════════════════════════════════════════════════════════════════
# VUE CONSOLIDÉE — toutes les colonnes WhoScored dans features_training
# ══════════════════════════════════════════════════════════════════════════════

ALL_WS_COLS_TRAINING: list[str] = (
    [c for c, _ in NEW_COLS_WS]
    + [c for c, _ in NEW_COLS_DRAW_BEHAVIOR]
    + [c for c, _ in NEW_COLS_DRAW_SIGNALS]
    + [c for c, _ in NEW_COLS_SQUAD]
)

ALL_WS_COLS_FINAL: list[str] = (
    [c for c, _ in DIFF_COLS_WS]
    + [c for c, _ in DIFF_COLS_DRAW]
    + [c for c, _ in DIFF_COLS_SQUAD]
)

ALL_WS_COLS_FINAL: list[str] = (
    [c for c, _ in DIFF_COLS_WS]
    + [c for c, _ in DIFF_COLS_DRAW]
)