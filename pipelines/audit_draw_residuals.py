"""
Audit des Résidus Draw — Projet 3-Étoiles
==========================================
Identifie les 50 matchs où le modèle a été le plus surpris par un nul :
  - Le résultat réel est D
  - La probabilité prédite P(D) était la plus faible

Pour chaque match, analyse les features disponibles afin de détecter
les signaux manquants qui caractérisent les nuls "invisibles" au modèle.

Usage :
    python pipelines/audit_draw_residuals.py
    python pipelines/audit_draw_residuals.py --n 100
    python pipelines/audit_draw_residuals.py --split test
"""

import argparse
import yaml
import numpy as np
import pandas as pd
import duckdb
import joblib
from pathlib import Path
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = CFG["paths"]["duckdb"]
MODEL_PATH = Path("models/football_stacking_v1.joblib")
OUT_DIR    = Path("models/diagnostics")
OUT_DIR.mkdir(parents=True, exist_ok=True)

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/audit_draw.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# ── Features d'analyse (présentes dans features_final) ───────────────────────
# On ne les utilise PAS pour prédire — uniquement pour comprendre les résidus
AUDIT_FEATURES = [
    # Équilibre tactique — signal fort pour les nuls
    "xg_net_diff",          # Différentiel xG net : proche de 0 = match équilibré
    "xg_net_diff_5",        # Idem sur fenêtre 5
    "sqr_diff",             # Différentiel qualité de tir
    "ppda_diff",            # Différentiel pressing
    "tactical_advantage",   # Avantage tactique composite
    # Marché — le bookmaker encode déjà une info sur le nul
    "odds_pinnacle_draw",   # Cote draw Pinnacle
    "odds_avg_draw",        # Cote draw moyenne
    # Cartons / fautes — contexte physique
    "red_card_rate_roll_5", # Taux de cartons rouges récents
    "red_card_rate_diff",   # Différentiel cartons rouges
    "fouls_per_tackle_roll_5",
    # Possession / domination
    "poss_roll_5",
    "poss_roll_venue_5",
    # H2H
    "h2h_draw_rate",        # Taux historique de nuls entre ces deux équipes
    "h2h_n_matches",        # Fiabilité du signal H2H
    # Ligue
    "league_draw_rate",     # Taux de nuls historique de la ligue
    # Forme
    "xg_overperformance_5", # Surperformance xG récente
    "sterility_diff",       # Différentiel stérilité offensive
    "shield_efficiency_diff",
    "shots_faced_per_goal_conceded_3",
    "sterility_weighted_3",
    "shots_faced_per_goal_conceded_10",
    "sterility_weighted_10",
    "shots_faced_per_goal_conceded_5",
    "sterility_weighted_5",
]


def load_model():
    """Charge le joblib et extrait les composants nécessaires."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modèle introuvable : {MODEL_PATH}")
    bundle = joblib.load(MODEL_PATH)
    logger.info(f"  Modèle chargé : {MODEL_PATH}")
    logger.info(f"  Clés disponibles : {list(bundle.keys())}")
    return bundle


def load_predictions(split: str) -> pd.DataFrame:
    """Charge les prédictions sauvegardées par 04_train.py."""
    path = Path(f"models/predictions_{split}.csv")
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {path}\n"
            f"Lance d'abord 04_train.py pour générer les prédictions."
        )
    df = pd.read_csv(path)
    logger.info(f"  Prédictions chargées : {len(df):,} matchs ({split})")
    return df


def load_gold_features(final_match_ids: list) -> pd.DataFrame:
    """
    Charge les features d'audit depuis gold.features_final
    pour les matchs concernés (via final_match_id).
    On prend la perspective Home (is_home=1) pour éviter les doublons.
    """
    conn = duckdb.connect(DB_PATH, read_only=True)

    # Colonnes disponibles
    available = {
        row[0] for row in
        conn.execute("SELECT column_name FROM information_schema.columns "
                     "WHERE table_schema='gold' AND table_name='features_final'").fetchall()
    }

    cols = [c for c in AUDIT_FEATURES if c in available]
    missing = [c for c in AUDIT_FEATURES if c not in available]
    if missing:
        logger.warning(f"  Colonnes absentes de features_final : {missing}")

    select_cols = ", ".join(
        ["final_match_id",
         "team AS gold_home_team",        # ← renommé pour éviter le conflit
         "opponent AS gold_away_team",    # ← idem
         "season"] + cols
    )

    ids_str = ", ".join(f"'{fid}'" for fid in final_match_ids)
    query = f"""
        SELECT {select_cols}
        FROM gold.features_final
        WHERE final_match_id IN ({ids_str})
          AND is_home = 1
    """
    df = conn.execute(query).df()
    conn.close()
    logger.info(f"  Features Gold chargées : {len(df):,} matchs")
    return df


def find_draw_residuals(df_pred: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Identifie les N matchs réels=D où P(D) était la plus basse.
    'Résidu' = à quel point le modèle a RATÉ ce nul.
    Score de surprise = 1 - P(D) prédit (plus c'est haut, plus le modèle était sûr que ce n'était PAS un nul)
    """
    # Filtre : résultat réel = Draw
    df_draws = df_pred[df_pred["actual_result"] == "D"].copy()

    if "prob_draw" not in df_draws.columns:
        raise ValueError(
            "Colonne 'prob_draw' absente. "
            "Vérifie que predictions_{split}.csv est bien généré par 04_train.py."
        )

    df_draws["surprise_score"] = 1.0 - df_draws["prob_draw"]
    df_draws = df_draws.sort_values("surprise_score", ascending=False).head(n)

    logger.info(f"\n  Top {n} nuls les plus surprenants :")
    logger.info(f"  P(D) moyenne : {df_draws['prob_draw'].mean():.4f}")
    logger.info(f"  P(D) max     : {df_draws['prob_draw'].max():.4f}")
    logger.info(f"  P(D) min     : {df_draws['prob_draw'].min():.4f}")
    return df_draws


def analyze_residuals(df_residuals: pd.DataFrame,
                      df_all_draws: pd.DataFrame,
                      df_features: pd.DataFrame) -> None:
    """
    Compare les features des matchs 'surprenants' vs
    l'ensemble des vrais nuls — pour détecter les patterns manquants.
    """
    # Jointure avec features Gold
    df = df_residuals.merge(df_features, on="final_match_id", how="left")
    df = df.drop(columns=["gold_home_team", "gold_away_team", "gold_league"], errors="ignore")
    df_ref = df_all_draws.merge(df_features, on="final_match_id", how="left")

    logger.info("\n" + "═" * 70)
    logger.info("  ANALYSE DES RÉSIDUS DRAW")
    logger.info("═" * 70)

    # ── 1. Distribution par ligue ─────────────────────────────────────────────
    logger.info("\n  [1] Distribution par ligue (Top résidus vs tous les nuls)")
    league_residuals = df["league"].value_counts(normalize=True).mul(100).round(1)
    league_all       = df_ref["league"].value_counts(normalize=True).mul(100).round(1)
    league_cmp = pd.DataFrame({
        "% résidus": league_residuals,
        "% tous nuls": league_all
    }).fillna(0).sort_values("% résidus", ascending=False)
    logger.info("\n" + league_cmp.to_string())

    # ── 2. Features numériques : comparaison résidus vs tous nuls ────────────
    num_cols = [c for c in AUDIT_FEATURES
                if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]

    logger.info("\n  [2] Comparaison features numériques (médiane)")
    logger.info(f"  {'Feature':<35} | {'Résidus':>10} | {'Tous nuls':>10} | {'Δ':>8}")
    logger.info(f"  {'-'*70}")

    diffs = []
    for col in num_cols:
        med_res = df[col].median()
        med_all = df_ref[col].median()
        if pd.isna(med_res) or pd.isna(med_all):
            continue
        delta = med_res - med_all
        diffs.append((col, med_res, med_all, delta))

    # Trier par |Δ| décroissant — les features les plus différentes en premier
    diffs.sort(key=lambda x: abs(x[3]), reverse=True)
    for col, med_res, med_all, delta in diffs:
        flag = " ◄" if abs(delta) > 0.05 else ""
        logger.info(f"  {col:<35} | {med_res:>10.4f} | {med_all:>10.4f} | {delta:>+8.4f}{flag}")

    # ── 3. Focus xg_net_diff — est-ce que les nuls surprenants ont un grand écart xG ?
    if "xg_net_diff" in df.columns:
        logger.info("\n  [3] xg_net_diff — matchs surprenants dominés (|diff| > 0.5)")
        dominated = df[df["xg_net_diff"].abs() > 0.5]
        logger.info(f"  {len(dominated)}/{len(df)} matchs avaient un xg_net_diff > 0.5")
        logger.info("  → Le modèle a raté des nuls où une équipe dominait clairement.")
        logger.info("     Signal manquant probable : résistance défensive, VAR, fatigue.")

    # ── 4. Focus cartons rouges ───────────────────────────────────────────────
    if "red_card_rate_roll_5" in df.columns:
        high_cards = df[df["red_card_rate_roll_5"] > df_ref["red_card_rate_roll_5"].quantile(0.75)]
        logger.info(f"\n  [4] Cartons rouges élevés dans les résidus : {len(high_cards)}/{len(df)}")
        if len(high_cards) / len(df) > 0.3:
            logger.info("  ◄ Signal fort : les nuls surprenants impliquent souvent des équipes")
            logger.info("    à haute intensité physique. Feature manquante : cartons IN-MATCH.")

    # ── 5. Focus H2H draw rate ────────────────────────────────────────────────
    if "h2h_draw_rate" in df.columns:
        low_h2h = df[df["h2h_draw_rate"] < 0.2]
        logger.info(f"\n  [5] H2H draw rate faible (<20%) dans les résidus : {len(low_h2h)}/{len(df)}")
        if len(low_h2h) / len(df) > 0.4:
            logger.info("  ◄ Ces équipes ne font historically pas nul entre elles.")
            logger.info("    Le modèle n'avait pas de signal H2H favorable au Draw.")

    # ── 6. Focus marché (odds draw) ───────────────────────────────────────────
    if "odds_pinnacle_draw" in df.columns:
        logger.info("\n  [6] Cotes Draw Pinnacle — résidus vs tous nuls")
        logger.info(f"  Médiane résidus  : {df['odds_pinnacle_draw'].median():.2f}")
        logger.info(f"  Médiane tous nuls: {df_ref['odds_pinnacle_draw'].median():.2f}")
        if df["odds_pinnacle_draw"].median() > df_ref["odds_pinnacle_draw"].median() * 1.1:
            logger.info("  ◄ Le marché aussi sous-estimait ces nuls (cotes élevées).")
            logger.info("    Ce sont des nuls 'impossibles à prévoir' — acceptables.")
        else:
            logger.info("  ◄ Le marché voyait ces nuls mais le modèle non.")
            logger.info("    Signal manquant dans nos features vs bookmakers.")

    # ── 7. Top 10 matchs les plus surprenants ────────────────────────────────
    logger.info("\n  [7] Top 10 matchs les plus surprenants")
    logger.info(f"  {'Date':<12} {'Home':<20} {'Away':<20} {'P(D)':>6} {'P(H)':>6} {'P(A)':>6} {'Ligue'}")
    logger.info(f"  {'-'*90}")
    display_cols = ["date", "home_team", "away_team", "prob_draw",
                    "prob_home", "prob_away", "league"]
    available_display = [c for c in display_cols if c in df.columns]
    for _, row in df.head(10).iterrows():
        date     = str(row.get("date", "?"))[:10]
        home     = str(row.get("home_team", "?"))[:18]
        away     = str(row.get("away_team", "?"))[:18]
        p_d      = row.get("prob_draw", float("nan"))
        p_h      = row.get("prob_home", float("nan"))
        p_a      = row.get("prob_away", float("nan"))
        league   = str(row.get("league", "?"))[:20]
        logger.info(f"  {date:<12} {home:<20} {away:<20} {p_d:>6.3f} {p_h:>6.3f} {p_a:>6.3f} {league}")

    # ── 8. Sauvegarde CSV ─────────────────────────────────────────────────────
    out_path = OUT_DIR / "draw_residuals_top50.csv"
    export_cols = [c for c in
                   ["final_match_id", "date", "home_team", "away_team",
                    "league", "season", "prob_draw", "prob_home",
                    "prob_away", "surprise_score"] + AUDIT_FEATURES
                   if c in df.columns]
    df[export_cols].to_csv(out_path, index=False)
    logger.success(f"\n  Export CSV : {out_path}")
    logger.info("  → Ouvre ce fichier dans Excel/DuckDB pour approfondir l'analyse.")


def main(n: int = 50, split: str = "val"):
    logger.info(f"=== Audit Résidus Draw (split={split}, n={n}) ===")

    # 1. Charger prédictions
    df_pred = load_predictions(split)

    # 2. Identifier les résidus
    df_all_draws  = df_pred[df_pred["actual_result"] == "D"].copy()
    df_residuals  = find_draw_residuals(df_pred, n=n)

    logger.info(f"  Vrais nuls dans le split : {len(df_all_draws):,}")
    logger.info(f"  Résidus sélectionnés     : {len(df_residuals):,}")

    # 3. Charger features Gold
    all_ids = df_residuals["final_match_id"].tolist()
    ref_ids = df_all_draws["final_match_id"].tolist()
    all_needed = list(set(all_ids + ref_ids))

    df_features = load_gold_features(all_needed)

    # 4. Analyser
    analyze_residuals(df_residuals, df_all_draws, df_features)

    logger.success("=== Audit terminé ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit résidus Draw")
    parser.add_argument("--n",     type=int, default=50,
                        help="Nombre de matchs à analyser (défaut: 50)")
    parser.add_argument("--split", type=str, default="val",
                        choices=["val", "test"],
                        help="Split à analyser (défaut: val)")
    args = parser.parse_args()
    main(n=args.n, split=args.split)
