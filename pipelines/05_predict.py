"""
05_predict.py — Inférence & Value Bets
Usage :
    python pipelines/05_predict.py
    python pipelines/05_predict.py --date-from 2024-08-01 --date-to 2024-08-31
    python pipelines/05_predict.py --upcoming          # résultat IS NULL uniquement
"""

import argparse
import sys
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
from loguru import logger

# ── Constantes (identiques à 04_train.py) ────────────────────────────────────
DB_PATH   = "db/football.duckdb"
MODEL_PATH = "models/football_stacking_v1.joblib"
OUTPUT_PATH = "models/predictions_upcoming.csv"

ID_COLS = ["final_match_id", "date", "season", "league_source",
           "comp_category", "venue", "result_1n2", "result_match"]
TARGET  = "result_1n2"

DRAW_THRESHOLD = 0.28
VALUE_EDGE     = 0.05      # seuil value bet


# ── Chargement artefacts ──────────────────────────────────────────────────────
def load_artefacts(model_path: str) -> dict:
    logger.info(f"Chargement artefacts : {model_path}")
    art = joblib.load(model_path)
    required = ["preprocessors", "label_encoders", "models",
                "meta_model", "calibrators", "winsorize_caps",
                "feature_names"]
    for k in required:
        if k not in art:
            logger.error(f"Clé manquante dans le joblib : {k}")
            sys.exit(1)
    return art


# ── Chargement données ────────────────────────────────────────────────────────
def load_data(upcoming_only: bool = False,
              date_from: str = None,
              date_to: str = None) -> pd.DataFrame:
    logger.info("Chargement gold.features_final...")
    con = duckdb.connect(DB_PATH, read_only=True)

    filters = ["comp_category = 'Big5'"]
    if upcoming_only:
        filters.append("result_1n2 IS NULL")
    elif date_from and date_to:
        filters.append(f"date >= '{date_from}' AND date <= '{date_to}'")
    elif date_from:
        filters.append(f"date >= '{date_from}'")
    elif date_to:
        filters.append(f"date <= '{date_to}'")

    where = " AND ".join(filters)
    query = f"SELECT * FROM gold.features_final WHERE {where}"

    df = con.execute(query).df()
    con.close()

    logger.info(f"  {len(df):,} lignes × {df.shape[1]} colonnes")
    return df


# ── Pivot (copie exacte de 04_train.py) ──────────────────────────────────────
def pivot_to_match(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Pivot home/away → 1 ligne par match...")

    id_cols_pivot = ["final_match_id", "date", "season",
                     "league_source", "comp_category"]
    feature_cols  = [c for c in df.columns
                     if c not in ID_COLS + id_cols_pivot
                     and c not in ["team", "opponent"]]
    odds_check = [c for c in feature_cols if "odds" in c or "prob" in c or "market" in c or "pinnacle" in c]
    logger.debug(f"  Colonnes cotes dans feature_cols : {odds_check}")

    home = df[df["venue"] == "Home"].copy()
    away = df[df["venue"] == "Away"].copy()

    home_renamed = home[id_cols_pivot + ["team", "opponent", TARGET] + feature_cols].copy()
    away_renamed = away[id_cols_pivot + ["team", "opponent"] + feature_cols].copy()

    home_renamed = home_renamed.rename(
        columns={c: f"h_{c}" for c in feature_cols}
    ).rename(columns={"team": "home_team", "opponent": "away_team",
                       TARGET: "result_match"})

    away_renamed = away_renamed.rename(
        columns={c: f"a_{c}" for c in feature_cols}
    ).rename(columns={"team": "away_team_check", "opponent": "home_team_check"})

    merged = home_renamed.merge(
        away_renamed[["final_match_id"] + [f"a_{c}" for c in feature_cols]],
        on="final_match_id",
        how="inner",
    )

    logger.info(f"  {len(merged):,} matchs consolidés")
    return merged


# ── Winsorisation (apply only, no fit) ───────────────────────────────────────
def apply_winsorize(df: pd.DataFrame, caps: dict) -> pd.DataFrame:
    for col, bounds in caps.items():
        if col in df.columns:
            df[col] = df[col].clip(bounds["lower"], bounds["upper"])
    return df


# ── Extraction features (copie exacte de prepare_perspective/combined) ────────
def extract_home_features(df_match: pd.DataFrame) -> pd.DataFrame:
    feat_cols = [c for c in df_match.columns if c.startswith("h_")]
    X = df_match[feat_cols].copy()
    X.columns = [c[2:] for c in X.columns]
    return X


def extract_away_features(df_match: pd.DataFrame) -> pd.DataFrame:
    feat_cols = [c for c in df_match.columns if c.startswith("a_")]
    X = df_match[feat_cols].copy()
    X.columns = [c[2:] for c in X.columns]
    return X


def extract_combined_features(df_match: pd.DataFrame) -> pd.DataFrame:
    h_cols = [c for c in df_match.columns if c.startswith("h_")]
    a_cols = [c for c in df_match.columns if c.startswith("a_")]
    return df_match[h_cols + a_cols].copy()


# ── Alignement colonnes (robustesse équipes nouvelles) ────────────────────────
def align_columns(X: pd.DataFrame, expected_cols: list) -> pd.DataFrame:
    missing = set(expected_cols) - set(X.columns)
    if missing:
        logger.warning(f"  {len(missing)} features manquantes → imputées NaN : {sorted(missing)[:5]}{'...' if len(missing) > 5 else ''}")
        for col in missing:
            X[col] = np.nan   # ← NaN au lieu de 0.0
    return X[expected_cols]


# ── Draw boost (identique à 04_train.py) ─────────────────────────────────────
def predict_with_draw_boost(y_proba: np.ndarray, le,
                            draw_threshold: float = DRAW_THRESHOLD) -> np.ndarray:
    idx_D = list(le.classes_).index("D")
    idx_H = list(le.classes_).index("H")
    idx_A = list(le.classes_).index("A")

    p_d = y_proba[:, idx_D]
    p_h = y_proba[:, idx_H]
    p_a = y_proba[:, idx_A]

    draw_mask = (p_d > draw_threshold) & (p_d > p_h * 0.7) & (p_d > p_a * 0.7)

    y_pred = np.argmax(y_proba, axis=1).copy()
    y_pred[draw_mask] = idx_D
    return y_pred


# ── Pipeline inférence ────────────────────────────────────────────────────────
def run_inference(df_match: pd.DataFrame, art: dict) -> pd.DataFrame:

    pp        = art["preprocessors"]
    le_home   = art["label_encoders"]["home"]
    le_away = art["label_encoders"]["away"]
    le_combined = art["label_encoders"]["combined"]
    lgbm_home = art["models"]["lgbm_home"]
    lgbm_away = art["models"]["lgbm_away"]
    lr_base   = art["models"]["lr_baseline"]
    meta      = art["meta_model"]
    cals      = art["calibrators"]          # [cal_H, cal_D, cal_A]
    fn        = art["feature_names"]        # home_cols, away_cols, comb_cols

    # ── Features brutes ───────────────────────────────────────────────────────
    X_home = align_columns(extract_home_features(df_match),     fn["home"])
    X_away = align_columns(extract_away_features(df_match),     fn["away"])
    X_comb = align_columns(extract_combined_features(df_match), fn["combined"])

    # ── Preprocessing (transform only) ───────────────────────────────────────
    # X_home_s = pp["home"].transform(X_home)
    # X_away_s = pp["away"].transform(X_away)
    # X_comb_s = pp["combined"].transform(X_comb)
    
    # Après transform, réemballer en DataFrame
    home_cols = fn["home"]
    away_cols = fn["away"]
    comb_cols = fn["combined"]

    X_home_s = pd.DataFrame(pp["home"].transform(X_home), columns=home_cols)
    X_away_s = pd.DataFrame(pp["away"].transform(X_away), columns=away_cols)
    X_comb_s = pd.DataFrame(pp["combined"].transform(X_comb), columns=comb_cols)

    # ── Stage 1 ───────────────────────────────────────────────────────────────
    proba_home = lgbm_home.predict_proba(X_home_s)   # P(H/D/A) vue home
    proba_away = lgbm_away.predict_proba(X_away_s)   # P(H/D/A) vue away (inversé)
    proba_lr = lr_base.predict_proba(X_comb_s.values if hasattr(X_comb_s, 'values') else X_comb_s)

    # Réinverser la perspective Away → point de vue domicile
    away_classes = list(le_away.classes_)
    reorder_idx  = [away_classes.index(c) for c in ["A", "D", "H"]]
    proba_away_dom = proba_away[:, reorder_idx]



# ── Stage 2 ───────────────────────────────────────────────────────────────
    odds_cols = ["h_pinnacle_prob_team", "h_pinnacle_prob_draw", "h_pinnacle_prob_opp"]
    if all(c in df_match.columns for c in odds_cols):
        p_market = df_match[odds_cols].values.astype(float)
        p_market = np.where(np.isnan(p_market), 1/3, p_market)
    else:
        p_market = np.full((len(df_match), 3), 1/3)

    X_meta = np.hstack([proba_home, proba_away_dom, proba_lr, p_market])

    # Debug
    logger.debug(f"  Classes le_home     : {list(le_home.classes_)}")
    logger.debug(f"  Classes le_away     : {list(le_away.classes_)}")
    logger.debug(f"  Classes le_combined : {list(le_combined.classes_)}")
    logger.debug(f"  proba_home[0]       : {proba_home[0]}")
    logger.debug(f"  proba_away[0]       : {proba_away[0]}")
    logger.debug(f"  proba_away_dom[0]   : {proba_away_dom[0]}")
    logger.debug(f"  p_market[0]         : {p_market[0]}")
    logger.debug(f"  X_meta[0]           : {X_meta[0]}")

    proba_meta = meta.predict_proba(X_meta)

    # ── Calibration ───────────────────────────────────────────────────────────
    # cals = [cal_H, cal_D, cal_A] dans l'ordre des classes de le_combined
    proba_cal = np.zeros_like(proba_meta)
    for c, cal in enumerate(cals):
        proba_cal[:, c] = cal.transform(proba_meta[:, c])

    # Renormaliser (isotonique ne garantit pas la somme = 1)
    proba_cal = proba_cal / proba_cal.sum(axis=1, keepdims=True)

    # ── Décision finale ───────────────────────────────────────────────────────
    y_pred_enc = predict_with_draw_boost(proba_cal, le_combined)
    y_pred     = le_combined.inverse_transform(y_pred_enc)

    # ── Assemblage résultat ───────────────────────────────────────────────────
    idx_H_c = list(le_combined.classes_).index("H")
    idx_D_c = list(le_combined.classes_).index("D")
    idx_A_c = list(le_combined.classes_).index("A")

    out = df_match[["final_match_id", "date", "home_team",
                    "away_team", "season", "league_source"]].copy().reset_index(drop=True)
    out["prob_H"]            = proba_cal[:, idx_H_c].round(4)
    out["prob_D"]            = proba_cal[:, idx_D_c].round(4)
    out["prob_A"]            = proba_cal[:, idx_A_c].round(4)
    out["predicted_result"]  = y_pred

    return out


# ── Value Bets ────────────────────────────────────────────────────────────────
def compute_value_bets(df_pred: pd.DataFrame,
                       df_match: pd.DataFrame) -> pd.DataFrame:
    
    odds_cols_map = {
        "h_odds_avg_team": "odd_H",
        "h_odds_avg_draw": "odd_D",
        "h_odds_avg_opp":  "odd_A",
    }

    available = [c for c in odds_cols_map if c in df_match.columns]
    if not available:
        logger.warning("  Colonnes cotes absentes → value bets désactivés")
        df_pred["suggested_bet"] = None
        return df_pred

    odds_map = df_match[["final_match_id"] + available].copy()
    odds_map = odds_map.rename(columns=odds_cols_map)

    # Calculer les probabilités implicites depuis les cotes
    for outcome in ["H", "D", "A"]:
        odd_col = f"odd_{outcome}"
        if odd_col in odds_map.columns:
            odds_map[f"implied_{outcome}"] = 1.0 / odds_map[odd_col]

    # Normaliser pour retirer la marge bookmaker
    total = sum(
        odds_map[f"implied_{o}"]
        for o in ["H", "D", "A"]
        if f"implied_{o}" in odds_map.columns
    )
    for outcome in ["H", "D", "A"]:
        if f"implied_{outcome}" in odds_map.columns:
            odds_map[f"implied_{outcome}"] = odds_map[f"implied_{outcome}"] / total

    df = df_pred.merge(odds_map, on="final_match_id", how="left")

    def _best_bet(row):
        bets = {}
        for outcome, prob_col in [("H", "prob_H"), ("D", "prob_D"), ("A", "prob_A")]:
            odd_col     = f"odd_{outcome}"
            implied_col = f"implied_{outcome}"

            if pd.isna(row.get(odd_col)) or row.get(odd_col, 0) <= 1:
                continue

            prob_model   = row[prob_col]
            prob_implied = row.get(implied_col, 0)
            edge         = prob_model - prob_implied

            # Cohérence : ne parier que sur l'outcome prédit
            if edge > VALUE_EDGE and outcome == row["predicted_result"]:
                bets[outcome] = round(edge, 4)

        if not bets:
            return None
        best = max(bets, key=bets.get)
        return f"{best} (edge={bets[best]:.2%})"

    df["suggested_bet"] = df.apply(_best_bet, axis=1)
    return df.drop(
        columns=[c for c in ["odd_H", "odd_D", "odd_A",
                              "implied_H", "implied_D", "implied_A"]
                 if c in df.columns],
        errors="ignore"
    )




# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Football Predictor — Inférence")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--upcoming",   action="store_true",
                       help="Matchs sans résultat (result_1n2 IS NULL)")
    group.add_argument("--date-from",  type=str, metavar="YYYY-MM-DD")
    parser.add_argument("--date-to",   type=str, metavar="YYYY-MM-DD")
    parser.add_argument("--odds-csv",  type=str, default=None,
                        help="CSV avec colonnes [final_match_id, odd_H, odd_D, odd_A]")
    parser.add_argument("--output",    type=str, default=OUTPUT_PATH)
    parser.add_argument("--model",     type=str, default=MODEL_PATH)
    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    art = load_artefacts(args.model)

    df = load_data(
        upcoming_only=args.upcoming,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    if df.empty:
        logger.warning("Aucune donnée à prédire.")
        sys.exit(0)

    df_match = pivot_to_match(df)
    df_match = apply_winsorize(df_match, art["winsorize_caps"])

    df_pred = run_inference(df_match, art)

    # Value bets
    # odds = None
    # if args.odds_csv:
    #     odds = pd.read_csv(args.odds_csv)
    #     logger.info(f"Cotes chargées : {len(odds)} matchs")
    df_pred = compute_value_bets(df_pred, df_match)

    # Export
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_pred.to_csv(out_path, index=False)
    logger.success(f"Prédictions sauvegardées : {out_path}  ({len(df_pred)} matchs)")

    # Aperçu
    value_bets = df_pred[df_pred["suggested_bet"].notna()]
    logger.info(f"Value bets détectés : {len(value_bets)}")
    if not value_bets.empty:
        logger.info("\n" + value_bets[["date", "home_team", "away_team",
                                       "prob_H", "prob_D", "prob_A",
                                       "predicted_result", "suggested_bet"]
                                      ].to_string(index=False))


if __name__ == "__main__":
    main()