"""
Pipeline 03 — Features (complet)
==================================
Construit le dataset ML final en combinant toutes les catégories FBref.

Étapes :
  1. Jointure home/away sur chaque catégorie (date + team + opponent)
  2. Features de forme rolling (3/5/10 matchs) + EWMA (5/10)
  3. Features de fatigue
  4. Features dérivées par catégorie (reprises de tes EDA)
  5. Super-marges inter-catégories
  6. Classement dynamique (avec shift pour éviter data leakage)
  7. H2H (confrontations directes)
  8. Features contextuelles (kickoff_category, mois)
  9. Rolling sur les marges finales
 10. Assemblage final -> features.ml_dataset

Usage :
    python pipelines/03_features.py
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
import yaml
from config_columns import CAT_PREFIX, META_COLS

# Config
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH     = CFG["paths"]["db"]
FORM_WINDOW = CFG["features"]["form_window"]
EWMA_SPANS  = [5, 10]
WINDOWS     = [3, 5, 10]

# Logs
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# META_COLS importé depuis config_columns.py


# ============================================================
# BLOC 1 - Chargement
# ============================================================

def load_all(con):
    logger.info("Chargement des tables silver.*...")
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'"
    ).fetchall()]
    data = {}
    for t in tables:
        df = con.execute(f"SELECT * FROM silver.{t}").df()
        df["date"] = pd.to_datetime(df["date"])
        data[t] = df
        logger.info(f"  silver.{t} : {len(df):,} lignes")
    return data


# ============================================================
# BLOC 2 - Jointure home/away
# ============================================================

def get_stat_cols(df):
    return [c for c in df.columns if c not in META_COLS]


def build_match_view(df, stat_cols, cat_prefix=None):
    """
    Joint les lignes home/away. Si cat_prefix est fourni,
    les colonnes stats sont préfixées pour éviter les conflits entre catégories.
    Ex: cat_prefix="sh" -> xg -> sh_xg, xg_opp -> sh_xg_opp
    """
    df_home = df[df["venue"].str.lower() == "home"].copy()
    df_away = df[df["venue"].str.lower() == "away"].copy()

    # Renommer les stats avec préfixe si demandé
    if cat_prefix:
        home_rename = {c: f"{cat_prefix}_{c}" for c in stat_cols}
        away_rename = {c: f"{cat_prefix}_{c}_opp" for c in stat_cols}
        prefixed_stat_cols = [f"{cat_prefix}_{c}" for c in stat_cols]
    else:
        home_rename = {}
        away_rename = {c: f"{c}_opp" for c in stat_cols}
        prefixed_stat_cols = stat_cols

    df_home_r = df_home.rename(columns=home_rename)
    df_away_r = df_away.rename(columns=away_rename)

    df_match = df_home_r.merge(
        df_away_r[["date", "team", "opponent"] + [f"{cat_prefix}_{c}_opp" if cat_prefix else f"{c}_opp" for c in stat_cols]],
        left_on=["date", "team", "opponent"],
        right_on=["date", "opponent", "team"],
        how="inner",
        suffixes=("", "_drop"),
    )
    drop_cols = [c for c in df_match.columns if c.endswith("_drop")]
    df_match = df_match.drop(columns=drop_cols)
    df_match = df_match.loc[:, ~df_match.columns.duplicated(keep="first")]
    return df_match


# ============================================================
# BLOC 3 - Fatigue
# ============================================================

def build_fatigue_features(df_all, df_match):
    logger.info("  Features de fatigue...")
    df_fat = df_all.copy().sort_values(["team", "date"])
    grp = df_fat.groupby("team", group_keys=False)

    df_fat["days_since_last"] = grp["date"].transform(lambda x: x.diff().dt.days)
    df_fat["is_cup_last"] = grp["round"].transform(
        lambda x: (~x.str.startswith("Matchweek", na=False)).astype(int).shift(1)
    ).fillna(0)

    def count_14d(group):
        group = group.sort_values("date")
        result = []
        for _, row in group.iterrows():
            n = ((group["date"] < row["date"]) &
                 (group["date"] >= row["date"] - pd.Timedelta(days=14))).sum()
            result.append(n)
        return pd.Series(result, index=group.index)

    df_fat["matches_14d"] = grp.apply(count_14d, include_groups=False).reset_index(level=0, drop=True)
    fat_cols = ["days_since_last", "is_cup_last", "matches_14d"]

    df_match = df_match.merge(
        df_fat[["team", "date"] + fat_cols].rename(
            columns={c: f"home_{c}" for c in fat_cols}),
        on=["team", "date"], how="left"
    )
    df_match = df_match.merge(
        df_fat[["team", "date"] + fat_cols].rename(
            columns={"team": "opponent", **{c: f"away_{c}" for c in fat_cols}}),
        on=["opponent", "date"], how="left"
    )
    return df_match


# ============================================================
# BLOC 4 - Features derivees defense
# ============================================================

def build_derived_defense(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : defense")
    df["home_tkl_win_rate"] = np.where(df["def_tackles_tkl"] > 0, df["def_tackles_tklw"] / df["def_tackles_tkl"], 0)
    df["away_tkl_win_rate"] = np.where(df["def_tackles_tkl_opp"] > 0, df["def_tackles_tklw_opp"] / df["def_tackles_tkl_opp"], 0)
    df["home_shot_block_ratio"] = np.where(df["def_blocks_blocks"] > 0, df["def_blocks_sh"] / df["def_blocks_blocks"], 0)
    df["away_shot_block_ratio"] = np.where(df["def_blocks_blocks_opp"] > 0, df["def_blocks_sh_opp"] / df["def_blocks_blocks_opp"], 0)
    df["home_pass_block_pct"] = np.where(df["def_blocks_blocks"] > 0, df["def_blocks_pass"] / df["def_blocks_blocks"], 0)
    df["away_pass_block_pct"] = np.where(df["def_blocks_blocks_opp"] > 0, df["def_blocks_pass_opp"] / df["def_blocks_blocks_opp"], 0)
    home_act = df["def_tackles_tklw"] + df["def_int"] + df["def_clr"]
    away_act = df["def_tackles_tklw_opp"] + df["def_int_opp"] + df["def_clr_opp"]
    df["home_def_quality_index"] = np.log1p(home_act) * df["home_tkl_win_rate"] + 0.5 * df["def_blocks_sh"] - 5 * df["def_err"]
    df["away_def_quality_index"] = np.log1p(away_act) * df["away_tkl_win_rate"] + 0.5 * df["def_blocks_sh_opp"] - 5 * df["def_err_opp"]
    df["home_def_style_index"] = (df["def_tackles_att_3rd"] + 1) / (df["def_tackles_def_3rd"] + 1)
    df["away_def_style_index"] = (df["def_tackles_att_3rd_opp"] + 1) / (df["def_tackles_def_3rd_opp"] + 1)
    df["home_safety_index"] = df["def_int"] * 2 + df["def_tackles_tklw"] * 0.5 - df["def_challenges_lost"] * 1.5 - df["def_err"] * 5
    df["away_safety_index"] = df["def_int_opp"] * 2 + df["def_tackles_tklw_opp"] * 0.5 - df["def_challenges_lost_opp"] * 1.5 - df["def_err_opp"] * 5
    df["def_quality_marge"]  = df["home_def_quality_index"] - df["away_def_quality_index"]
    df["def_style_marge"]    = df["home_def_style_index"]   - df["away_def_style_index"]
    df["safety_marge"]       = df["home_safety_index"]      - df["away_safety_index"]
    df["tkl_win_rate_marge"] = df["home_tkl_win_rate"]      - df["away_tkl_win_rate"]
    df["att_3rd_tkl_marge"]  = df["def_tackles_att_3rd"]        - df["def_tackles_att_3rd_opp"]
    df["tkl_int_clr_marge"]  = (df["def_tkl_int"] + df["def_clr"]) - (df["def_tkl_int_opp"] + df["def_clr_opp"])
    df["challenge_marge"]    = df["def_challenges_tkl_pct"]     - df["def_challenges_tkl_pct_opp"]
    df["err_marge"]          = df["def_err_opp"]                - df["def_err"]
    df["shot_block_marge"]   = df["home_shot_block_ratio"]  - df["away_shot_block_ratio"]
    return df


# ============================================================
# BLOC 5 - Features derivees shooting
# ============================================================

def build_derived_shooting(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : shooting")
    df["home_efficiency_index"] = df["sh_g_xg"] * 2
    df["away_efficiency_index"] = df["sh_g_xg_opp"] * 2
    df["home_chance_quality"] = df["sh_npxg"] + 0.1 * df["sh_standard_sh"] + 5 * df["sh_npxg_sh"]
    df["away_chance_quality"] = df["sh_npxg_opp"] + 0.1 * df["sh_standard_sh_opp"] + 5 * df["sh_npxg_sh_opp"]
    safe_dist     = df["sh_standard_dist"].replace(0, 1) + 0.01
    safe_dist_opp = df["sh_standard_dist_opp"].replace(0, 1) + 0.01
    df["home_danger_index"] = df["sh_standard_sot"] + 50 * df["sh_standard_sot_pct"] - 1 / safe_dist
    df["away_danger_index"] = df["sh_standard_sot_opp"] + 50 * df["sh_standard_sot_pct_opp"] - 1 / safe_dist_opp
    df["home_np_g_xg_luck"] = df["sh_np_g_xg"]
    df["away_np_g_xg_luck"] = df["sh_np_g_xg_opp"]
    df["chance_quality_marge"] = df["home_chance_quality"] - df["away_chance_quality"]
    df["danger_index_marge"]   = df["home_danger_index"]   - df["away_danger_index"]
    df["net_shots_marge"]      = df["sh_standard_sh"]         - df["sh_standard_sh_opp"]
    df["xg_marge"]             = df["sh_xg"]                  - df["sh_xg_opp"]
    df["npxg_marge"]           = df["sh_npxg"]                - df["sh_npxg_opp"]
    df["luck_marge"]           = df["home_np_g_xg_luck"]   - df["away_np_g_xg_luck"]
    return df


# ============================================================
# BLOC 6 - Features derivees passing
# ============================================================

def build_derived_passing(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : passing")
    df["home_progression"] = df["pass_total_prgdist"] + 5 * df["pass_passes_final_third"]
    df["away_progression"] = df["pass_total_prgdist_opp"] + 5 * df["pass_passes_final_third_opp"]
    df["home_creative_quality"] = df["pass_xag"] + 2 * df["pass_kp"] + df["pass_ppa"]
    df["away_creative_quality"] = df["pass_xag_opp"] + 2 * df["pass_kp_opp"] + df["pass_ppa_opp"]
    df["home_long_eff"] = np.where(df["pass_long_att"] > 0, df["pass_long_cmp"] / df["pass_long_att"], 0)
    df["away_long_eff"] = np.where(df["pass_long_att_opp"] > 0, df["pass_long_cmp_opp"] / df["pass_long_att_opp"], 0)
    df["home_short_eff"] = np.where(df["pass_short_att"] > 0, df["pass_short_cmp"] / df["pass_short_att"], 0)
    df["away_short_eff"] = np.where(df["pass_short_att_opp"] > 0, df["pass_short_cmp_opp"] / df["pass_short_att_opp"], 0)
    df["progression_marge"]      = df["home_progression"]      - df["away_progression"]
    df["creative_quality_marge"] = df["home_creative_quality"] - df["away_creative_quality"]
    df["long_eff_marge"]         = df["home_long_eff"]         - df["away_long_eff"]
    df["short_eff_marge"]        = df["home_short_eff"]        - df["away_short_eff"]
    df["total_cmp_pct_marge"]    = df["pass_total_cmp_pct"]         - df["pass_total_cmp_pct_opp"]
    return df


# ============================================================
# BLOC 7 - Features derivees possession
# ============================================================

def build_derived_possession(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : possession")
    df["home_zone_control"] = df["pos_touches_att_3rd"] + 2 * df["pos_touches_att_pen"]
    df["away_zone_control"] = df["pos_touches_att_3rd_opp"] + 2 * df["pos_touches_att_pen_opp"]
    df["home_advancement"] = df["pos_carries_prgdist"] + 3 * df["pos_carries_final_third"]
    df["away_advancement"] = df["pos_carries_prgdist_opp"] + 3 * df["pos_carries_final_third_opp"]
    df["home_takeon_quality"] = np.where(df["pos_take_ons_att"] > 0, df["pos_take_ons_succ"] / df["pos_take_ons_att"], 0)
    df["away_takeon_quality"] = np.where(df["pos_take_ons_att_opp"] > 0, df["pos_take_ons_succ_opp"] / df["pos_take_ons_att_opp"], 0)
    df["poss_marge"]         = df["pos_poss"]                - df["pos_poss_opp"]
    df["zone_control_marge"] = df["home_zone_control"]   - df["away_zone_control"]
    df["advancement_marge"]  = df["home_advancement"]    - df["away_advancement"]
    df["takeon_marge"]       = df["home_takeon_quality"] - df["away_takeon_quality"]
    return df


# ============================================================
# BLOC 8 - Features derivees goal_shot_creation
# ============================================================

def build_derived_gsc(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : goal_shot_creation")
    df["home_gca_conv_rate"] = np.where(df["gsc_sca"] > 0, df["gsc_gca"] / df["gsc_sca"], 0)
    df["away_gca_conv_rate"] = np.where(df["gsc_sca_opp"] > 0, df["gsc_gca_opp"] / df["gsc_sca_opp"], 0)
    df["home_deadball_pct"] = np.where(df["gsc_gca"] > 0, df["gsc_gca_passdead"] / df["gsc_gca"], 0)
    df["away_deadball_pct"] = np.where(df["gsc_gca_opp"] > 0, df["gsc_gca_passdead_opp"] / df["gsc_gca_opp"], 0)
    df["sca_marge"]           = df["gsc_sca"]                - df["gsc_sca_opp"]
    df["gca_marge"]           = df["gsc_gca"]                - df["gsc_gca_opp"]
    df["gca_conv_rate_marge"] = df["home_gca_conv_rate"] - df["away_gca_conv_rate"]
    df["deadball_pct_marge"]  = df["home_deadball_pct"]  - df["away_deadball_pct"]
    return df


# ============================================================
# BLOC 9 - Features derivees misc
# ============================================================

def build_derived_misc(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : misc")
    df["home_discipline_index"] = df["misc_crdy"] + 3 * df["misc_crdr"] + df["misc_fls"] * 0.5
    df["away_discipline_index"] = df["misc_crdy_opp"] + 3 * df["misc_crdr_opp"] + df["misc_fls_opp"] * 0.5
    df["home_press_recov"] = np.where(df["misc_fls"] > 0, df["misc_recov"] / df["misc_fls"], 0)
    df["away_press_recov"] = np.where(df["misc_fls_opp"] > 0, df["misc_recov_opp"] / df["misc_fls_opp"], 0)
    df["home_aerial_perf"] = np.where(
        (df["misc_aerial_won"] + df["misc_aerial_lost"]) > 0,
        df["misc_aerial_won"] / (df["misc_aerial_won"] + df["misc_aerial_lost"]), 0)
    df["away_aerial_perf"] = np.where(
        (df["misc_aerial_won_opp"] + df["misc_aerial_lost_opp"]) > 0,
        df["misc_aerial_won_opp"] / (df["misc_aerial_won_opp"] + df["misc_aerial_lost_opp"]), 0)
    df["discipline_marge"]  = df["away_discipline_index"] - df["home_discipline_index"]
    df["press_recov_marge"] = df["home_press_recov"]      - df["away_press_recov"]
    df["aerial_marge"]      = df["home_aerial_perf"]      - df["away_aerial_perf"]
    df["net_cross_marge"]   = df["misc_crs"]                   - df["misc_crs_opp"]
    return df


# ============================================================
# BLOC 10 - Features derivees keeper
# ============================================================

def build_derived_keeper(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Features derivees : keeper")
    df["home_psxg_per_sota"] = np.where(df["gk_sota"] > 0, df["gk_psxg"] / df["gk_sota"], 0)
    df["away_psxg_per_sota"] = np.where(df["gk_sota_opp"] > 0, df["gk_psxg_opp"] / df["gk_sota_opp"], 0)
    df["home_pk_save_rate"] = np.where(df["gk_pk_att"] > 0, df["gk_pk_saved"] / df["gk_pk_att"], 0)
    df["away_pk_save_rate"] = np.where(df["gk_pk_att_opp"] > 0, df["gk_pk_saved_opp"] / df["gk_pk_att_opp"], 0)
    df["home_launch_eff"] = np.where(df["gk_launched_att"] > 0, df["gk_launched_cmp"] / df["gk_launched_att"], 0)
    df["away_launch_eff"] = np.where(df["gk_launched_att_opp"] > 0, df["gk_launched_cmp_opp"] / df["gk_launched_att_opp"], 0)
    df["home_modern_gk"] = df["gk_save_pct"] + df["gk_sweeper_opa"] * 2 + df["home_launch_eff"] * 10
    df["away_modern_gk"] = df["gk_save_pct_opp"] + df["gk_sweeper_opa_opp"] * 2 + df["away_launch_eff"] * 10
    df["save_pct_marge"]   = df["gk_save_pct"]       - df["gk_save_pct_opp"]
    df["psxg_diff_marge"]  = df["gk_psxg_diff"]      - df["gk_psxg_diff_opp"]
    df["modern_gk_marge"]  = df["home_modern_gk"] - df["away_modern_gk"]
    df["cross_stop_marge"] = df["gk_crosses_stp_pct"] - df["gk_crosses_stp_pct_opp"]
    return df


# ============================================================
# BLOC 11 - Super-marges inter-categories
# ============================================================

def build_super_margins(df):
    df = df.copy()  # eviter PerformanceWarning
    logger.info("  Super-marges inter-categories...")
    df["tklintclr_home"] = df["def_int"] + df["def_clr"]
    df["tklintclr_away"] = df["def_int_opp"] + df["def_clr_opp"]
    df["home_off_def_eff"] = np.where(df["tklintclr_away"] > 0, df["gsc_sca"] / df["tklintclr_away"], 0)
    df["away_off_def_eff"] = np.where(df["tklintclr_home"] > 0, df["gsc_sca_opp"] / df["tklintclr_home"], 0)
    df["off_def_eff_marge"] = df["home_off_def_eff"] - df["away_off_def_eff"]

    df["home_threat_ctrl"] = np.where(df["away_safety_index"].abs() > 0,
                                       df["home_danger_index"] / df["away_safety_index"].replace(0, 0.01), 0)
    df["away_threat_ctrl"] = np.where(df["home_safety_index"].abs() > 0,
                                       df["away_danger_index"] / df["home_safety_index"].replace(0, 0.01), 0)
    df["threat_ctrl_marge"] = df["home_threat_ctrl"] - df["away_threat_ctrl"]

    df["home_keeper_vs_threat"] = np.where(df["sh_xg_opp"] > 0, df["gk_save_pct"] / df["sh_xg_opp"], 0)
    df["away_keeper_vs_threat"] = np.where(df["sh_xg"] > 0, df["gk_save_pct_opp"] / df["sh_xg"], 0)
    df["keeper_vs_threat_marge"] = df["home_keeper_vs_threat"] - df["away_keeper_vs_threat"]

    df["home_press_discipline"] = np.where(df["misc_fls"] > 0, df["misc_recov"] / df["misc_fls"], 0)
    df["away_press_discipline"] = np.where(df["misc_fls_opp"] > 0, df["misc_recov_opp"] / df["misc_fls_opp"], 0)
    df["press_discipline_marge"] = df["home_press_discipline"] - df["away_press_discipline"]

    df["home_attack_vs_gk"] = np.where(df["gk_save_pct_opp"] > 0, df["sh_xg"] / (df["gk_save_pct_opp"] / 100 + 0.01), 0)
    df["away_attack_vs_gk"] = np.where(df["gk_save_pct"] > 0, df["sh_xg_opp"] / (df["gk_save_pct"] / 100 + 0.01), 0)
    df["attack_vs_gk_marge"] = df["home_attack_vs_gk"] - df["away_attack_vs_gk"]

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df


# ============================================================
# BLOC 12 - Classement dynamique
# ============================================================

def build_ranking_features(df):
    logger.info("  Classement dynamique...")
    df = df.sort_values(["league_source", "season", "date", "team"]).copy()
    df["points"]          = df["result"].map({"W": 3, "D": 1, "L": 0})
    df["goal_difference"] = df["gf"] - df["ga"]
    df["pts_cum"] = df.groupby(["league_source", "season", "team"])["points"].cumsum()
    df["gd_cum"]  = df.groupby(["league_source", "season", "team"])["goal_difference"].cumsum()
    df["gf_cum"]  = df.groupby(["league_source", "season", "team"])["gf"].cumsum()

    df_sorted = df.sort_values(
        ["league_source", "season", "date", "pts_cum", "gd_cum", "gf_cum"],
        ascending=[True, True, True, False, False, False]
    ).copy()
    df_sorted["team_rank"] = df_sorted.groupby(
        ["league_source", "season", "date"]).cumcount() + 1

    df = df.merge(
        df_sorted[["league_source", "season", "date", "team", "team_rank"]],
        on=["league_source", "season", "date", "team"], how="left"
    )
    df["team_rank_pre"] = df.groupby(
        ["league_source", "season", "team"])["team_rank"].shift(1).fillna(10)
    df["pts_cum_pre"] = df.groupby(
        ["league_source", "season", "team"])["pts_cum"].shift(1).fillna(0)
    df["gd_cum_pre"] = df.groupby(
        ["league_source", "season", "team"])["gd_cum"].shift(1).fillna(0)

    n_teams = df.groupby(["league_source", "season"])["team"].transform("nunique")

    conditions = [
        df["team_rank_pre"] <= (n_teams / 3),
        df["team_rank_pre"] <= (2 * n_teams / 3)
    ]

    choices = ["Haut", "Milieu"]

    df["rank_tier"] = np.select(conditions, choices, default="Bas")

    df = df.drop(columns=["pts_cum", "gd_cum", "gf_cum", "team_rank",
                           "points", "goal_difference"], errors="ignore")
    return df


# ============================================================
# BLOC 13 - H2H
# ============================================================

def build_h2h_features(df):
    logger.info("  Features H2H...")
    N = FORM_WINDOW
    df = df.sort_values(["team", "opponent", "date"]).copy()
    df["points_h2h"] = df["result"].map({"W": 3, "D": 1, "L": 0})
    df["gd_h2h"]     = df["gf"] - df["ga"]
    df["is_win_h2h"] = (df["result"] == "W").astype(int)
    grp = df.groupby(["team", "opponent"], group_keys=False)
    df["h2h_win_pct"]    = grp["is_win_h2h"].transform(
        lambda x: x.shift(1).rolling(N, min_periods=1).mean()).fillna(0.5)
    df["h2h_avg_gd"]     = grp["gd_h2h"].transform(
        lambda x: x.shift(1).rolling(N, min_periods=1).mean()).fillna(0)
    df["h2h_avg_points"] = grp["points_h2h"].transform(
        lambda x: x.shift(1).rolling(N, min_periods=1).mean()).fillna(1)
    df = df.drop(columns=["points_h2h", "gd_h2h", "is_win_h2h"], errors="ignore")
    return df


# ============================================================
# BLOC 14 - Contexte (schedule)
# ============================================================

def build_context_features(df, df_schedule):
    logger.info("  Features contextuelles (schedule)...")

    def classify_time(t):
        try:
            if pd.isna(t): return "Inconnu"
            hour = int(str(t).split(":")[0])
            if hour >= 20:   return "Soiree"
            elif hour >= 17: return "Fin_Apres_Midi"
            else:            return "Apres_Midi"
        except Exception:
            return "Inconnu"

    sched = df_schedule[["team", "date", "time"]].copy()
    sched["kickoff_category"] = sched["time"].apply(classify_time)
    sched["month"] = pd.to_datetime(sched["date"]).dt.month
    df = df.merge(sched[["team", "date", "kickoff_category", "month"]],
                  on=["team", "date"], how="left")
    df["is_matchweek"] = df["round"].str.startswith("Matchweek", na=False).astype(int)
    df["kickoff_category"] = df["kickoff_category"].fillna("Inconnu")
    df["month"] = df["month"].fillna(df["date"].dt.month)
    return df


# ============================================================
# BLOC 15 - Rolling sur les marges finales
# ============================================================

def build_margin_rolling(df):
    logger.info("  Rolling sur les marges finales...")
    marge_cols = [c for c in df.columns if c.endswith("_marge")]
    df = df.sort_values(["team", "date"]).copy()
    grp = df.groupby("team", group_keys=False)
    new_cols = {}

    for col in marge_cols:
        new_cols[f"{col}_lag1"] = grp[col].transform(lambda x: x.shift(1)).fillna(0)
        for w in WINDOWS:
            new_cols[f"{col}_{w}m_avg"] = grp[col].transform(
                lambda x: x.shift(1).rolling(w, min_periods=1).mean()).fillna(0)
        for span in EWMA_SPANS:
            new_cols[f"{col}_ewma{span}"] = grp[col].transform(
                lambda x: x.shift(1).ewm(span=span, adjust=False).mean()).fillna(0)
        new_cols[f"{col}_overall_avg"] = grp[col].transform(
            lambda x: x.shift(1).expanding(min_periods=1).mean()).fillna(0)

    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
    logger.info(f"  {len(marge_cols)} marges x transformations")
    return df


# ============================================================
# POINT D'ENTREE
# ============================================================

def main():
    logger.info("=== Demarrage features (complet) ===")
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS features")

    data = load_all(con)

    # Jointure home/away par categorie
    # CAT_PREFIX importé depuis config_columns.py
    logger.info("Jointures home/away...")
    dfs = {}
    for cat, df in data.items():
        # if cat == "schedule":
        #     continue
        stat_cols = get_stat_cols(df)
        prefix = CAT_PREFIX.get(cat)
        dfs[cat] = build_match_view(df, stat_cols, cat_prefix=prefix)
        logger.info(f"  {cat} : {len(dfs[cat]):,} matchs")

    # Table de base = defense
    # df_ml = dfs["defense"].copy()
    df_ml = dfs["schedule"].copy()

    # Merge des autres categories
    logger.info("Merge des categories...")
    merge_key = ["date", "team", "opponent","league","league_source"]

    for cat in ["defense","shooting", "passing", "possession", "goal_shot_creation", "misc", "keeper"]:
        if cat not in dfs:
            logger.warning(f"  {cat} absent, ignore")
            continue

        df_cat = dfs[cat].copy()

        # Supprimer les doublons dans df_cat avant le merge
        df_cat = df_cat.loc[:, ~df_cat.columns.duplicated(keep="first")]

        # Supprimer les doublons dans df_ml avant le merge
        df_ml = df_ml.loc[:, ~df_ml.columns.duplicated(keep="first")]

        # Colonnes a merger : uniquement les stats nouvelles + clés
        existing = set(df_ml.columns) - set(merge_key)
        new_stat_cols = [c for c in df_cat.columns if c not in existing]
        cols_to_merge = merge_key + new_stat_cols

        # Ne merger que si on a des nouvelles colonnes
        if not new_stat_cols:
            logger.warning(f"  {cat} : aucune nouvelle colonne, ignore")
            continue

        right = df_cat[[c for c in cols_to_merge if c in df_cat.columns]].copy()
        right = right.loc[:, ~right.columns.duplicated(keep="first")]

        df_ml = df_ml.merge(right, on=merge_key, how="left")
        df_ml = df_ml.loc[:, ~df_ml.columns.duplicated(keep="first")]
        logger.info(f"  + {cat} : {len(df_ml.columns)} colonnes")

    # Conversion de toutes les colonnes stats en numerique
    # (elles arrivent en VARCHAR depuis DuckDB)
    logger.info("Conversion des colonnes en numerique...")
    meta_keep = ["league","date", "team", "opponent", "league_source", "season",
                 "round", "day", "venue", "result", "result_1n2",
                 "time", "match_report", "source_file", "season_norm",
                 "season_raw", "game", "notes","captain","formation","opp_formation","referee"]
    for col in df_ml.columns:
        if col not in meta_keep and df_ml[col].dtype == object:
            df_ml[col] = pd.to_numeric(df_ml[col], errors="coerce")
    logger.info(f"  Conversion terminee")
    df_ml = df_ml.copy()  # Defragmentation du DataFrame

    # Features derivees
    logger.info("Features derivees par categorie...")
    df_ml = build_derived_defense(df_ml)
    df_ml = build_derived_shooting(df_ml)
    df_ml = build_derived_passing(df_ml)
    df_ml = build_derived_possession(df_ml)
    df_ml = build_derived_gsc(df_ml)
    df_ml = build_derived_misc(df_ml)
    df_ml = build_derived_keeper(df_ml)
    df_ml = build_super_margins(df_ml)

    # Fatigue
    df_all_defense = data["defense"].copy()
    df_ml = build_fatigue_features(df_all_defense, df_ml)

    # Classement + H2H + Contexte
    df_ml = build_ranking_features(df_ml)
    df_ml = build_h2h_features(df_ml)
    df_ml = build_context_features(df_ml, data["schedule"])

    # Rolling marges
    df_ml = build_margin_rolling(df_ml)

    # Nettoyage final
    df_ml.replace([np.inf, -np.inf], np.nan, inplace=True)

    logger.info("-- Rapport features.ml_dataset --")
    logger.info(f"  Lignes      : {len(df_ml):,}")
    logger.info(f"  Colonnes    : {len(df_ml.columns)}")
    logger.info(f"  NaN totaux  : {df_ml.isnull().sum().sum():,}")
    mw = df_ml[df_ml["is_matchweek"] == 1]
    dist = mw["result_1n2"].value_counts().to_dict()
    logger.info(f"  Distribution 1N2 (Matchweeks) : {dist}")

    logger.info("Ecriture de features.ml_dataset...")
    con.execute("DROP TABLE IF EXISTS features.ml_dataset")
    con.execute("CREATE TABLE features.ml_dataset AS SELECT * FROM df_ml")
    n = con.execute("SELECT COUNT(*) FROM features.ml_dataset").fetchone()[0]
    logger.success(f"  features.ml_dataset : {n:,} lignes ecrites")

    con.close()
    logger.success("=== features termine ===")


if __name__ == "__main__":
    main()