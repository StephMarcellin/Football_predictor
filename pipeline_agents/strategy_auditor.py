"""
Agent Stratégie — pipeline_agents/strategy_auditor.py
======================================================
Audit complet du signal modèle et optimisation de la stratégie de paris.

Flux :
  1. Signal brut     → ROI mise fixe par outcome / ligue
  2. Modèle vs marché → gap de calibration par ligue
  3. Optimisation     → grille edge_min × confidence_min → meilleure config
  4. Rapport Markdown → models/strategy_audit_report.md

S'insère dans agent_manager.py après l'auditor existant.
Peut aussi être lancé seul :
    python pipeline_agents/strategy_auditor.py
"""

from __future__ import annotations

import argparse
import itertools
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from loguru import logger

# ── Chemins ───────────────────────────────────────────────────────────────────

ROOT_DIR   = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
MODELS_DIR = ROOT_DIR / "models"

REPORTS_DIR  = ROOT_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

REPORT_OUT = REPORTS_DIR / "strategy_audit_report.md"

PRED_FILE  = MODELS_DIR / "predictions_val.csv"

# ── Grille d'optimisation ─────────────────────────────────────────────────────

EDGE_GRID       = [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15]
CONFIDENCE_GRID = [0.40, 0.45, 0.50, 0.55, 0.60]
MIN_BETS        = 30   # nombre minimum de paris pour qu'une config soit valide


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 — CHARGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def load_data() -> pd.DataFrame:
    """Charge predictions_val.csv et joint les cotes depuis DuckDB."""

    if not PRED_FILE.exists():
        raise FileNotFoundError(f"Fichier introuvable : {PRED_FILE}")

    df = pd.read_csv(PRED_FILE)
    logger.info(f"  Prédictions : {len(df)} lignes depuis {PRED_FILE.name}")

    # Normalisation colonnes
    rename = {
        "prob_home": "prob_H",
        "prob_draw": "prob_D",
        "prob_away": "prob_A",
        "pred":      "predicted_result",
        "league":    "league_source",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "predicted_result" not in df.columns:
        df["predicted_result"] = (
            df[["prob_H", "prob_D", "prob_A"]].idxmax(axis=1).str[-1]
        )

    # Jointure cotes
    try:
        conn    = duckdb.connect(str(DB_PATH), read_only=True)
        df_odds = conn.execute("""
            SELECT
                f.final_match_id,
                o.odds_avg_h    AS odd_H,
                o.odds_avg_d    AS odd_D,
                o.odds_avg_a    AS odd_A,
                o.market_prob_h AS implied_H,
                o.market_prob_d AS implied_D,
                o.market_prob_a AS implied_A,
                o.season
            FROM silver.odds o
            LEFT JOIN gold.features_final f
                ON  o.date::DATE    = f.date::DATE
                AND o.home_team     = f.team
                AND o.league_source = f.league_source
                AND o.season        = f.season
                AND f.venue         = 'Home'
            WHERE o.odds_avg_h IS NOT NULL
        """).df()
        conn.close()
        df = df.merge(df_odds, on="final_match_id", how="left")
        logger.info(f"  Cotes jointes : {df['odd_H'].notna().sum()} matchs")
    except Exception as exc:
        logger.warning(f"  Jointure cotes échouée : {exc}")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — SIGNAL BRUT (MISE FIXE)
# ══════════════════════════════════════════════════════════════════════════════

def analyse_flat_stake(df: pd.DataFrame) -> dict:
    """ROI mise fixe global + par outcome + par ligue."""

    df = df.copy()
    df["correct"]    = df["predicted_result"] == df["actual_result"]
    df["odd_played"] = df.apply(
        lambda r: r.get(f"odd_{r['predicted_result']}"), axis=1
    )
    df = df.dropna(subset=["odd_played"])
    df["profit"] = df.apply(
        lambda r: float(r["odd_played"]) - 1 if r["correct"] else -1.0, axis=1
    )

    results: dict = {}

    # Global
    n   = len(df)
    roi = df["profit"].sum() / n if n > 0 else 0.0
    results["global"] = {
        "n":       n,
        "wr":      float(df["correct"].mean()),
        "roi":     roi,
        "profit":  float(df["profit"].sum()),
        "odd_avg": float(df["odd_played"].mean()),
    }

    # Par outcome
    results["by_outcome"] = {}
    for out in ["H", "D", "A"]:
        sub = df[df["predicted_result"] == out]
        if len(sub) == 0:
            continue
        results["by_outcome"][out] = {
            "n":       len(sub),
            "wr":      float(sub["correct"].mean()),
            "roi":     float(sub["profit"].sum() / len(sub)),
            "odd_avg": float(sub["odd_played"].mean()),
        }

    # Par ligue
    results["by_league"] = {}
    if "league_source" in df.columns:
        for league, grp in df.groupby("league_source"):
            results["by_league"][league] = {
                "n":   len(grp),
                "wr":  float(grp["correct"].mean()),
                "roi": float(grp["profit"].sum() / len(grp)),
            }

    logger.info(
        f"  Signal brut : ROI {roi:+.2%} | "
        f"WR {results['global']['wr']:.2%} | {n} matchs"
    )
    return results


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 — MODÈLE VS MARCHÉ
# ══════════════════════════════════════════════════════════════════════════════

def analyse_model_vs_market(df: pd.DataFrame) -> dict:
    """Compare la proba modèle vs implicite marché sur le résultat réel."""

    df = df.copy()

    df["model_prob"]  = df.apply(
        lambda r: r.get(f"prob_{r['actual_result']}"), axis=1
    )
    df["market_prob"] = df.apply(
        lambda r: r.get(f"implied_{r['actual_result']}"), axis=1
    )
    df = df.dropna(subset=["model_prob", "market_prob"])
    df["gap"] = df["model_prob"] - df["market_prob"]

    results: dict = {
        "n":              len(df),
        "model_mean":     float(df["model_prob"].mean()),
        "market_mean":    float(df["market_prob"].mean()),
        "gap_mean":       float(df["gap"].mean()),
        "pct_model_wins": float((df["gap"] > 0).mean()),
    }

    # Par ligue
    results["by_league"] = {}
    if "league_source" in df.columns:
        for league, grp in df.groupby("league_source"):
            results["by_league"][league] = {
                "gap_mean": float(grp["gap"].mean()),
                "n":        len(grp),
            }

    logger.info(
        f"  Modèle vs marché : gap moyen {results['gap_mean']:+.4f} | "
        f"modèle > marché sur {results['pct_model_wins']:.1%} des matchs"
    )
    return results


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 4 — OPTIMISATION STRATÉGIE (GRILLE edge × confidence)
# ══════════════════════════════════════════════════════════════════════════════

def _compute_edge(row: pd.Series) -> float:
    """Edge = prob_modèle - prob_implicite marché pour l'outcome prédit."""
    out = row["predicted_result"]
    p_model  = row.get(f"prob_{out}", None)
    p_market = row.get(f"implied_{out}", None)
    if p_model is None or p_market is None or pd.isna(p_model) or pd.isna(p_market):
        return float("nan")
    return float(p_model) - float(p_market)


def optimise_strategy(df: pd.DataFrame) -> dict:
    """
    Teste toutes les combinaisons edge_min × confidence_min.
    Retourne la meilleure config (ROI max avec N >= MIN_BETS).
    """

    df = df.copy()
    df["edge"]       = df.apply(_compute_edge, axis=1)
    df["correct"]    = df["predicted_result"] == df["actual_result"]
    df["odd_played"] = df.apply(
        lambda r: r.get(f"odd_{r['predicted_result']}"), axis=1
    )
    df["profit"] = df.apply(
        lambda r: float(r["odd_played"]) - 1 if r["correct"] else -1.0, axis=1
    )

    # Colonne confiance = max des probas
    if "confidence" not in df.columns:
        df["confidence"] = df[["prob_H", "prob_D", "prob_A"]].max(axis=1)

    df = df.dropna(subset=["edge", "odd_played", "confidence"])

    grid_results = []
    for edge_min, conf_min in itertools.product(EDGE_GRID, CONFIDENCE_GRID):
        sub = df[(df["edge"] >= edge_min) & (df["confidence"] >= conf_min)]
        if len(sub) < MIN_BETS:
            continue
        roi = float(sub["profit"].sum() / len(sub))
        grid_results.append({
            "edge_min":    edge_min,
            "conf_min":    conf_min,
            "n":           len(sub),
            "wr":          float(sub["correct"].mean()),
            "roi":         roi,
            "profit":      float(sub["profit"].sum()),
        })

    if not grid_results:
        logger.warning("  Aucune config valide trouvée dans la grille")
        return {"best": None, "grid": []}

    grid_df = pd.DataFrame(grid_results).sort_values("roi", ascending=False)
    best    = grid_df.iloc[0].to_dict()

    logger.info(
        f"  Meilleure config : edge≥{best['edge_min']:.0%} | "
        f"conf≥{best['conf_min']:.0%} | "
        f"ROI {best['roi']:+.2%} | {best['n']} paris"
    )

    # Top 5 configs par ligue (sur la meilleure config globale)
    best_by_league: dict = {}
    if "league_source" in df.columns:
        sub_best = df[
            (df["edge"] >= best["edge_min"]) &
            (df["confidence"] >= best["conf_min"])
        ]
        for league, grp in sub_best.groupby("league_source"):
            if len(grp) < 10:
                continue
            best_by_league[league] = {
                "n":      len(grp),
                "wr":     float(grp["correct"].mean()),
                "roi":    float(grp["profit"].sum() / len(grp)),
                "profit": float(grp["profit"].sum()),
            }

    return {
        "best":          best,
        "grid":          grid_df.head(10).to_dict(orient="records"),
        "best_by_league": best_by_league,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 5 — RAPPORT MARKDOWN
# ══════════════════════════════════════════════════════════════════════════════

def generate_report(
    flat:   dict,
    vs_mkt: dict,
    optim:  dict,
) -> Path:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    L   = []

    def h(text):  L.extend([f"## {text}", ""])
    def h3(text): L.extend([f"### {text}", ""])
    def line(t=""): L.append(t)

    # ── En-tête ───────────────────────────────────────────────────────────────
    L += [
        "# 📈 Agent Stratégie — Rapport d'Audit",
        "",
        f"**Généré le :** {now}  ",
        f"**Source :** `models/predictions_val.csv`  ",
        f"**Matchs analysés :** {flat['global']['n']}",
        "",
        "---",
        "",
    ]

    # ── 1. Signal brut ────────────────────────────────────────────────────────
    h("1. Signal brut — Mise fixe")

    g = flat["global"]
    verdict = "✅ Signal positif" if g["roi"] > 0 else "❌ Signal négatif"
    L += [
        f"| Métrique | Valeur |",
        f"|---|---|",
        f"| Matchs | {g['n']} |",
        f"| Taux de réussite | {g['wr']:.2%} |",
        f"| ROI mise fixe | **{g['roi']:+.2%}** |",
        f"| Profit total | {g['profit']:+.2f} u |",
        f"| Cote moyenne jouée | {g['odd_avg']:.2f} |",
        f"| Verdict | {verdict} |",
        "",
    ]

    h3("Par outcome")
    L += ["| Outcome | N | WR | ROI | Cote moy |", "|---|---|---|---|---|"]
    for out, s in flat.get("by_outcome", {}).items():
        flag = "✅" if s["roi"] > 0 else "❌"
        L.append(
            f"| {flag} **{out}** | {s['n']} | {s['wr']:.2%} | "
            f"{s['roi']:+.2%} | {s['odd_avg']:.2f} |"
        )
    line()

    h3("Par ligue")
    L += ["| Ligue | N | WR | ROI |", "|---|---|---|---|"]
    for league, s in sorted(
        flat.get("by_league", {}).items(), key=lambda x: -x[1]["roi"]
    ):
        flag = "✅" if s["roi"] > 0 else "❌"
        L.append(
            f"| {flag} {league} | {s['n']} | {s['wr']:.2%} | {s['roi']:+.2%} |"
        )
    L += ["", "---", ""]

    # ── 2. Modèle vs Marché ───────────────────────────────────────────────────
    h("2. Modèle vs Marché")

    gap  = vs_mkt["gap_mean"]
    flag = "✅ Signal exploitable" if gap > 0.01 else (
           "⚠️  Équivalent au marché" if gap > -0.01 else
           "❌ Marché meilleur que le modèle"
    )
    L += [
        f"| Métrique | Valeur |",
        f"|---|---|",
        f"| Proba modèle moy (sur résultat réel) | {vs_mkt['model_mean']:.4f} |",
        f"| Proba marché moy (sur résultat réel) | {vs_mkt['market_mean']:.4f} |",
        f"| Gap moyen (modèle − marché) | **{gap:+.4f}** |",
        f"| % matchs modèle > marché | {vs_mkt['pct_model_wins']:.2%} |",
        f"| Verdict | {flag} |",
        "",
    ]

    h3("Gap par ligue")
    L += ["| Ligue | Gap moyen | N | Verdict |", "|---|---|---|---|"]
    for league, s in sorted(
        vs_mkt.get("by_league", {}).items(), key=lambda x: -x[1]["gap_mean"]
    ):
        g_val = s["gap_mean"]
        v     = "✅" if g_val > 0.01 else ("⚠️ " if g_val > -0.01 else "❌")
        L.append(f"| {league} | {g_val:+.4f} | {s['n']} | {v} |")
    L += ["", "---", ""]

    # ── 3. Optimisation stratégie ─────────────────────────────────────────────
    h("3. Optimisation stratégie")

    best = optim.get("best")
    if best:
        roi_flag = "✅" if best["roi"] > 0 else "❌"
        L += [
            "### Config optimale recommandée",
            "",
            f"| Paramètre | Valeur |",
            f"|---|---|",
            f"| `edge_min` | **{best['edge_min']:.0%}** |",
            f"| `confidence_min` | **{best['conf_min']:.0%}** |",
            f"| Paris sélectionnés | {best['n']} |",
            f"| Taux de réussite | {best['wr']:.2%} |",
            f"| ROI | {roi_flag} **{best['roi']:+.2%}** |",
            f"| Profit | {best['profit']:+.2f} u |",
            "",
        ]

        # Top 10 configs
        h3("Top 10 configurations (grille complète)")
        L += [
            "| edge_min | conf_min | N | WR | ROI | Profit |",
            "|---|---|---|---|---|---|",
        ]
        for row in optim.get("grid", []):
            flag = "✅" if row["roi"] > 0 else "❌"
            L.append(
                f"| {row['edge_min']:.0%} | {row['conf_min']:.0%} | "
                f"{row['n']} | {row['wr']:.2%} | "
                f"{flag} {row['roi']:+.2%} | {row['profit']:+.2f} |"
            )
        line()

        # Par ligue sur config optimale
        if optim.get("best_by_league"):
            h3("ROI par ligue avec config optimale")
            L += [
                "| Ligue | N | WR | ROI | Profit |",
                "|---|---|---|---|---|",
            ]
            for league, s in sorted(
                optim["best_by_league"].items(), key=lambda x: -x[1]["roi"]
            ):
                flag = "✅" if s["roi"] > 0 else "❌"
                L.append(
                    f"| {flag} {league} | {s['n']} | {s['wr']:.2%} | "
                    f"{s['roi']:+.2%} | {s['profit']:+.2f} |"
                )
            line()
    else:
        L.append("_Aucune configuration valide trouvée (N minimum non atteint)._")
        line()

    L += ["---", ""]

    # ── 4. Recommandations ────────────────────────────────────────────────────
    h("4. Recommandations")

    recs = []

    # Signal brut
    flat_away = flat.get("by_outcome", {}).get("A", {})
    if flat_away.get("roi", -1) > 0:
        recs.append(
            f"**✅ Away profitable** : ROI {flat_away['roi']:+.2%} en mise fixe. "
            f"Concentrer la stratégie sur les victoires Away."
        )

    flat_home = flat.get("by_outcome", {}).get("H", {})
    if flat_home.get("roi", 0) < -0.05:
        recs.append(
            f"**❌ Home sur-parié** : ROI {flat_home['roi']:+.2%}. "
            f"Réduire ou éliminer les paris Home de la stratégie."
        )

    # Ligues positives
    positive_leagues = [
        league for league, s in flat.get("by_league", {}).items()
        if s["roi"] > 0
    ]
    if positive_leagues:
        recs.append(
            f"**Ligues à cibler** : {', '.join(positive_leagues)} "
            f"sont profitables en mise fixe."
        )

    negative_leagues = [
        league for league, s in flat.get("by_league", {}).items()
        if s["roi"] < -0.05
    ]
    if negative_leagues:
        recs.append(
            f"**Ligues à éviter** : {', '.join(negative_leagues)} "
            f"sont déficitaires (ROI < -5%)."
        )

    # Gap modèle vs marché
    if vs_mkt["gap_mean"] < -0.02:
        recs.append(
            f"**⚠️ Gap modèle−marché : {vs_mkt['gap_mean']:+.4f}**. "
            f"Le marché est meilleur que le modèle. "
            f"Priorité : enrichir les features avant d'optimiser la stratégie."
        )

    # Config optimale
    if best and best["roi"] > 0:
        recs.append(
            f"**Config suggérée pour `config.yaml`** : "
            f"`edge_min: {best['edge_min']}` | "
            f"`confidence_min: {best['conf_min']}`"
        )
    elif best:
        recs.append(
            f"**Aucune config profitable trouvée**. "
            f"La meilleure config (edge≥{best['edge_min']:.0%}, "
            f"conf≥{best['conf_min']:.0%}) donne ROI {best['roi']:+.2%}. "
            f"Retravailler les features avant d'optimiser les seuils."
        )

    for rec in recs:
        L.append(f"- {rec}")
    line()

    L += [
        "---",
        "",
        "> Rapport généré par `pipeline_agents/strategy_auditor.py` — Projet 3-Étoiles",
    ]

    REPORT_OUT.write_text("\n".join(L), encoding="utf-8")
    logger.success(f"  Rapport sauvegardé : {REPORT_OUT}")
    return REPORT_OUT


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run(context: dict | None = None) -> dict:
    if context is None:
        context = {}

    logger.info("Démarrage audit stratégie")

    df     = load_data()
    flat   = analyse_flat_stake(df)
    vs_mkt = analyse_model_vs_market(df)
    optim  = optimise_strategy(df)
    report = generate_report(flat, vs_mkt, optim)

    context.update({
        "strategy_flat":    flat,
        "strategy_vs_mkt":  vs_mkt,
        "strategy_optim":   optim,
        "strategy_report":  str(report),
    })
    return context


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agent Stratégie — Projet 3-Étoiles"
    )
    parser.parse_args()
    run()