"""
config_columns.py
==================
Mapping centralisé des colonnes FBref pour tous les pipelines.
Importé par 02_process.py et 03_features.py.

Format : {nom_brut_duckdb: nom_semantique_final}
"""

COLUMN_MAPPING = {

    "defense": {
        "tackles":      "tackles_tkl",
        "tackles.1":    "tackles_tklw",
        "tackles.2":    "tackles_def_3rd",
        "tackles.3":    "tackles_mid_3rd",
        "tackles.4":    "tackles_att_3rd",
        "challenges":   "challenges_tkl",
        "challenges.1": "challenges_att",
        "challenges.2": "challenges_tkl_pct",
        "challenges.3": "challenges_lost",
        "blocks":       "blocks_blocks",
        "blocks.1":     "blocks_sh",
        "blocks.2":     "blocks_pass",
        "tkl_plus_int": "tkl_int",
    },

    "shooting": {
        "standard":   "standard_gls",
        "standard.1": "standard_sh",
        "standard.2": "standard_sot",
        "standard.3": "standard_sot_pct",
        "standard.4": "standard_g_sh",
        "standard.5": "standard_g_sot",
        "standard.6": "standard_dist",
        "standard.7": "standard_fk",
        "standard.8": "standard_pk",
        "standard.9": "standard_pkatt",
        "expected":   "xg",
        "expected.1": "npxg",
        "expected.2": "npxg_sh",
        "expected.3": "g_xg",       # sur/sous-performance vs xG
        "expected.4": "np_g_xg",    # idem sans pénaltys
    },

    "passing": {
        "total":    "total_cmp",
        "total.1":  "total_att",
        "total.2":  "total_cmp_pct",
        "total.3":  "total_totdist",
        "total.4":  "total_prgdist",
        "short":    "short_cmp",
        "short.1":  "short_att",
        "short.2":  "short_cmp_pct",
        "medium":   "medium_cmp",
        "medium.1": "medium_att",
        "medium.2": "medium_cmp_pct",
        "long":     "long_cmp",
        "long.1":   "long_att",
        "long.2":   "long_cmp_pct",
        "1_3":      "passes_final_third",
        # colonnes sans groupe (déjà bien nommées)
        # ast, xag, xa, kp, ppa, crspa, prgp
    },

    "passing_types": {
        "att":            "att_passes",
        "pass types":     "pass_live",
        "pass types.1":   "pass_dead",
        "pass types.2":   "pass_fk",
        "pass types.3":   "pass_tb",
        "pass types.4":   "pass_sw",
        "pass types.5":   "pass_crs",
        "pass types.6":   "pass_ti",
        "pass types.7":   "pass_ck",
        "corner kicks":   "ck_in",
        "corner kicks.1": "ck_out",
        "corner kicks.2": "ck_str",
        "outcomes":       "outcomes_cmp",
        "outcomes.1":     "outcomes_off",
        "outcomes.2":     "outcomes_blocks",
    },

    "possession": {
        "poss":        "poss",
        "touches":     "touches_total",
        "touches.1":   "touches_def_pen",
        "touches.2":   "touches_def_3rd",
        "touches.3":   "touches_mid_3rd",
        "touches.4":   "touches_att_3rd",
        "touches.5":   "touches_att_pen",
        "touches.6":   "touches_live",
        "take_ons":    "take_ons_att",
        "take_ons.1":  "take_ons_succ",
        "take_ons.2":  "take_ons_succ_pct",
        "take_ons.3":  "take_ons_tkld",
        "take_ons.4":  "take_ons_tkld_pct",
        "carries":     "carries_total",
        "carries.1":   "carries_totdist",
        "carries.2":   "carries_prgdist",
        "carries.3":   "carries_prgc",
        "carries.4":   "carries_final_third",
        "carries.5":   "carries_cpa",
        "carries.6":   "carries_mis",
        "carries.7":   "carries_dis",
        "receiving":   "receiving_rec",
        "receiving.1": "receiving_prgr",
    },

    "goal_shot_creation": {
        "sca_types":   "sca",
        "sca_types.1": "sca_passlive",
        "sca_types.2": "sca_passdead",
        "sca_types.3": "sca_to",
        "sca_types.4": "sca_sh",
        "sca_types.5": "sca_fld",
        "sca_types.6": "sca_def",
        "gca_types":   "gca",
        "gca_types.1": "gca_passlive",
        "gca_types.2": "gca_passdead",
        "gca_types.3": "gca_to",
        "gca_types.4": "gca_sh",
        "gca_types.5": "gca_fld",
        "gca_types.6": "gca_def",
    },

    "misc": {
        "performance":    "crdy",
        "performance.1":  "crdr",
        "performance.2":  "crdy2",
        "performance.3":  "fls",
        "performance.4":  "fld",
        "performance.5":  "off",
        "performance.6":  "crs",
        "performance.7":  "int",
        "performance.8":  "tklw",
        "performance.9":  "pkwon",
        "performance.10": "pkcon",
        "performance.11": "og",
        "performance.12": "recov",
        "aerial_duels":   "aerial_won",
        "aerial_duels.1": "aerial_lost",
        "aerial_duels.2": "aerial_won_pct",
    },

    "keeper": {
        "performance":     "sota",
        "performance.1":   "ga_keeper",
        "performance.2":   "saves",
        "performance.3":   "save_pct",
        "performance.4":   "cs",
        "performance.5":   "psxg",
        "performance.6":   "psxg_diff",
        "penalty_kicks":   "pk_att",
        "penalty_kicks.1": "pk_allowed",
        "penalty_kicks.2": "pk_saved",
        "penalty_kicks.3": "pk_missed",
        "launched":        "launched_cmp",
        "launched.1":      "launched_att",
        "launched.2":      "launched_cmp_pct",
        "passes":          "passes_att_gk",
        "passes.1":        "passes_thr",
        "passes.2":        "passes_launch_pct",
        "passes.3":        "passes_avglen",
        "goal_kicks":      "gk_att",
        "goal_kicks.1":    "gk_launch_pct",
        "goal_kicks.2":    "gk_avglen",
        "crosses":         "crosses_opp",
        "crosses.1":       "crosses_stp",
        "crosses.2":       "crosses_stp_pct",
        "sweeper":         "sweeper_opa",
        "sweeper.1":       "sweeper_avgdist",
    },

    "schedule": {},
}

# Colonnes dont le suffixe indique un type float (vs int)
FLOAT_SUFFIXES = [
    "_pct", "xg", "npxg", "psxg", "xa", "xag", "dist",
    "avglen", "avgdist", "poss", "g_sh", "g_sot", "g_xg",
    "np_g_xg", "save_pct", "cmp_pct", "succ_pct", "tkld_pct",
    "won_pct", "launch_pct", "stp_pct",
]

# Noms finaux des colonnes stats par catégorie
# (valeurs du COLUMN_MAPPING — utile pour le typage et les features)
STAT_COLS = {cat: list(mapping.values()) for cat, mapping in COLUMN_MAPPING.items()}


# ── Préfixes par catégorie ────────────────────────────────────────────────────
# Utilisés dans 03_features.py pour éviter les conflits de noms entre catégories
# lors des merges. Ex: xg (shooting) -> sh_xg pour ne pas conflicater avec
# xg (schedule) ou d'autres catégories.

CAT_PREFIX = {
    "defense":            "def",
    "shooting":           "sh",
    "passing":            "pass",
    "passing_types":      "pt",
    "possession":         "pos",
    "goal_shot_creation": "gsc",
    "misc":               "misc",
    "keeper":             "gk",
}


# ── Colonnes meta communes à toutes les catégories ───────────────────────────
# Ces colonnes sont exclues des stat_cols dans get_stat_cols()
#
# Trois types :
#   - Identifiants     : team, opponent, date, league_source, season, round...
#   - Contexte match   : venue, result, result_1n2, time, formation...
#   - Stats résultat   : gf, ga — buts marqués/encaissés, présents dans TOUTES
#                        les catégories FBref. On les garde ici pour éviter
#                        de les dupliquer avec un préfixe dans chaque catégorie.
#                        Ils sont accessibles directement via df["gf"] / df["ga"].
#
# ATTENTION : xg, xga, poss sont des STATS de catégorie — elles reçoivent un préfixe.

META_COLS = [
    # Identifiants
    "league","league_source", "season", "season_raw", "season_norm", "source_file",
    "team", "opponent", "date", "round", "day",
    # Contexte match
    "venue", "result", "result_1n2", "time", "match_report",
    "game", "notes", "attendance", "captain", "formation", "opp_formation", "referee",
    # Stats résultat (partagées par toutes les catégories — non préfixées)
    "gf", "ga",
]