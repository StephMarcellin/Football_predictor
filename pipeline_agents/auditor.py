"""
Agent Analyste — agents/auditor.py
===================================
Identifie les 50 matchs les plus catastrophiques (Log Loss le plus élevé)
depuis models/predictions_val.csv, croise avec gold.features_final via DuckDB,
et génère reports/agent_insights.md.

Source prioritaire : models/predictions_val.csv
  → Colonnes attendues : prob_H / prob_D / prob_A + actual_result + final_match_id

Usage autonome :
    python agents/auditor.py
    python agents/auditor.py --top-n 100

Appelé par l'orchestrateur via run(context).
"""

import argparse
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR     = Path(__file__).resolve().parent.parent
REPORTS_DIR  = ROOT_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH    = ROOT_DIR / CFG["paths"]["duckdb"]
REPORT_OUT = REPORTS_DIR / "agent_insights.md"
TOP_N      = 50

# Colonnes probabilités : 04_train.py peut produire deux formats selon la version
PROB_COL_ALIASES = {
    "H": ["prob_H", "prob_home"],
    "D": ["prob_D", "prob_draw"],
    "A": ["prob_A", "prob_away"],
}
RESULT_COL_CANDIDATES   = ["actual_result", "result_1n2", "result_fdc", "y_true"]
MATCH_ID_COL_CANDIDATES = ["final_match_id", "match_id"]

# ── Logging ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>[AUDITOR]</cyan> {message}",
)
logger.add(
    ROOT_DIR / "logs" / "auditor.log",
    level="DEBUG",
    rotation="5 MB",
    retention=10,
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [AUDITOR] {message}",
)


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 — CHARGEMENT ET CALCUL DU LOG LOSS PAR MATCH
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_col(df: pd.DataFrame, candidates: list) -> str | None:
    return next((c for c in candidates if c in df.columns), None)


def load_outliers(top_n: int):
    """
    Charge predictions_val.csv, calcule -log(p_vraie_classe) par match,
    retourne (df_outliers_top_n, series_logloss_complète).
    """
    csv_path = ROOT_DIR / "models" / "predictions_val.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"predictions_val.csv manquant : {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"  predictions_val.csv chargé : {len(df)} lignes, {len(df.columns)} colonnes")
    logger.debug(f"  Colonnes : {df.columns.tolist()}")

    # ── Résolution des colonnes ───────────────────────────────────────────────
    result_col   = _resolve_col(df, RESULT_COL_CANDIDATES)
    match_id_col = _resolve_col(df, MATCH_ID_COL_CANDIDATES)

    if result_col is None:
        raise KeyError(
            f"Colonne résultat introuvable parmi {RESULT_COL_CANDIDATES}. "
            f"Disponibles : {df.columns.tolist()}"
        )

    prob_H_col = _resolve_col(df, PROB_COL_ALIASES["H"])
    prob_D_col = _resolve_col(df, PROB_COL_ALIASES["D"])
    prob_A_col = _resolve_col(df, PROB_COL_ALIASES["A"])

    missing_probs = [k for k, v in {"H": prob_H_col, "D": prob_D_col, "A": prob_A_col}.items() if v is None]
    if missing_probs:
        raise KeyError(
            f"Colonnes probabilité manquantes pour : {missing_probs}. "
            f"Disponibles : {df.columns.tolist()}"
        )

    logger.debug(
        f"  Colonnes résolues → résultat='{result_col}', "
        f"probs='{prob_H_col}'/'{prob_D_col}'/'{prob_A_col}', "
        f"match_id='{match_id_col}'"
    )

    # ── Matrice de probabilités ───────────────────────────────────────────────
    outcome_order     = ["H", "D", "A"]
    prob_cols_ordered = [prob_H_col, prob_D_col, prob_A_col]
    prob_matrix       = df[prob_cols_ordered].values.astype(float)
    prob_matrix       = np.clip(prob_matrix, 1e-7, 1 - 1e-7)
    prob_matrix       = prob_matrix / prob_matrix.sum(axis=1, keepdims=True)

    # ── Encodage du résultat réel ─────────────────────────────────────────────
    result_series  = df[result_col].astype(str).str.strip().str.upper()
    result_map     = {o: i for i, o in enumerate(outcome_order)}
    result_indices = result_series.map(result_map)

    n_unmapped = result_indices.isna().sum()
    if n_unmapped > 0:
        unknown = result_series[result_indices.isna()].unique()
        logger.warning(f"  {n_unmapped} résultats non mappés ({unknown}) → lignes exclues")
        valid_mask     = result_indices.notna()
        df             = df[valid_mask].copy().reset_index(drop=True)
        prob_matrix    = prob_matrix[valid_mask]
        result_indices = result_indices[valid_mask].reset_index(drop=True)
        result_series  = result_series[valid_mask].reset_index(drop=True)

    result_idx = result_indices.astype(int).values

    # ── Log loss per-row ──────────────────────────────────────────────────────
    row_logloss = -np.log(prob_matrix[np.arange(len(df)), result_idx])

    df = df.copy()
    df["_logloss"]      = row_logloss
    df["_result"]       = result_series.values
    df["_match_id_key"] = df[match_id_col].values if match_id_col else None

    global_mean = df["_logloss"].mean()
    global_med  = df["_logloss"].median()
    logger.info(f"  Log Loss global → mean: {global_mean:.4f} | median: {global_med:.4f}")

    df_outliers = df.nlargest(top_n, "_logloss").reset_index(drop=True)
    logger.info(
        f"  {top_n} outliers extraits — "
        f"LL min: {df_outliers['_logloss'].min():.4f} | max: {df_outliers['_logloss'].max():.4f}"
    )

    return df_outliers, df["_logloss"]


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — ENRICHISSEMENT DUCKDB
# ══════════════════════════════════════════════════════════════════════════════

def enrich_with_duckdb(df_outliers: pd.DataFrame) -> pd.DataFrame:

    # ── Mapping inverse depuis config.yaml ───────────────────────────────────
    # team_mapping : {nom_brut → nom_canonique}
    # On l'inverse pour normaliser les noms WhoScored → canonique
    with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
        CFG_LOCAL = yaml.safe_load(f)

    team_mapping = CFG_LOCAL.get("team_mapping", {})
    # {nom_canonique → nom_canonique} via le mapping existant
    # La normalisation se fait en passant ws_name dans team_mapping
    
    match_ids = df_outliers["_match_id_key"].dropna().unique().tolist()
    if not match_ids:
        logger.warning("  Aucun final_match_id disponible — enrichissement ignoré")
        return df_outliers

    ids_sql = ", ".join(f"'{mid}'" for mid in match_ids)
    conn    = duckdb.connect(str(DB_PATH), read_only=True)

    # ── Charger le mapping dans DuckDB comme table temporaire ────────────────
    # On transforme le team_mapping en DataFrame et on le charge dans DuckDB
    mapping_df = pd.DataFrame([
        {"raw_name": k, "canonical_name": v}
        for k, v in team_mapping.items()
    ])
    conn.execute("CREATE TEMP TABLE team_map AS SELECT * FROM mapping_df")

    # ── gold.features_final ───────────────────────────────────────────────────
    try:
        df_gold = conn.execute(f"""
            SELECT
                final_match_id,
                date,
                team,
                opponent,
                season,
                league_source,
                comp_category,
                result_1n2,
                np_xg_roll_5,
                np_xg_conceded_roll_5,
                ppda_roll_5,
                ppda_allowed_roll_5,
                xg_overperformance_5,
                sterility_index_5,
                press_resistance_5,
                poss_roll_5,
                shot_accuracy_roll_5,
                save_rate_roll_5,
                season_att_rating,
                season_def_rating,
                ws_dribbles_pg,
                ws_shots_ot_pg
            FROM gold.features_final
            WHERE final_match_id IN ({ids_sql})
              AND venue = 'Home'
        """).df()
        logger.debug(f"  gold.features_final : {len(df_gold)} lignes enrichies")
    except Exception as e:
        logger.warning(f"  gold.features_final inaccessible : {e}")
        df_gold = pd.DataFrame()

    # ── Jointure via team_mapping + stg_whoscored_match_index ────────────────
    ws_event_counts: dict = {}
    try:
        df_ws = conn.execute(f"""
            WITH ws_normalized AS (
                -- Normalise home_team_name WhoScored → nom canonique
                SELECT
                    w.ws_match_id,
                    w.match_date,
                    w.league_source,
                    w.season,
                    COALESCE(th.canonical_name, w.home_team_name) AS home_canonical,
                    COALESCE(ta.canonical_name, w.away_team_name) AS away_canonical
                FROM silver.stg_whoscored_match_index w
                LEFT JOIN team_map th ON th.raw_name = w.home_team_name
                LEFT JOIN team_map ta ON ta.raw_name = w.away_team_name
            )
            SELECT
                f.final_match_id,
                ws_normalized.ws_match_id
            FROM gold.features_final f
            JOIN ws_normalized
                ON  f.date::DATE      = ws_normalized.match_date::DATE
                AND f.league_source   = ws_normalized.league_source
                AND f.season          = ws_normalized.season
                AND f.team            = ws_normalized.home_canonical
                AND f.venue           = 'Home'
            WHERE f.final_match_id IN ({ids_sql})
        """).df()

        # Récupérer le nombre d'events par ws_match_id
        if not df_ws.empty:
            ws_ids_sql = ", ".join(f"'{mid}'" for mid in df_ws["ws_match_id"].tolist())
            df_events = conn.execute(f"""
                SELECT ws_match_id, COUNT(*) AS event_count
                FROM silver.stg_whoscored_events
                WHERE ws_match_id IN ({ws_ids_sql})
                GROUP BY ws_match_id
            """).df()

            # Mapper final_match_id → event_count via ws_match_id
            id_bridge   = dict(zip(df_ws["final_match_id"], df_ws["ws_match_id"]))
            event_map   = dict(zip(df_events["ws_match_id"], df_events["event_count"]))
            ws_event_counts = {
                fid: event_map.get(wid, 0)
                for fid, wid in id_bridge.items()
            }
            logger.debug(
                f"  WhoScored events : {len(ws_event_counts)}/{len(match_ids)} matchs couverts"
            )

    except Exception as e:
        logger.debug(f"  stg_whoscored_events non accessible : {e}")

    conn.close()

    # ── Fusion ────────────────────────────────────────────────────────────────
    if not df_gold.empty:
        df_merged = df_outliers.merge(
            df_gold,
            left_on="_match_id_key",
            right_on="final_match_id",
            how="left",
        )
    else:
        df_merged = df_outliers.copy()

    df_merged["ws_event_count"] = (
        df_merged["_match_id_key"].map(ws_event_counts).fillna(0).astype(int)
    )

    if "has_ws_events" not in df_merged.columns:
        df_merged["has_ws_events"] = (df_merged["ws_event_count"] > 0).astype(int)

    return df_merged


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 — ANALYSE DES PATTERNS
# ══════════════════════════════════════════════════════════════════════════════

def analyse_patterns(df: pd.DataFrame, all_logloss: pd.Series) -> dict:
    patterns: dict = {}

    # Log Loss stats comparées
    if "_logloss" in df.columns and df["_logloss"].sum() > 0:
        patterns["logloss_stats"] = {
            "outlier_mean":   round(float(df["_logloss"].mean()), 4),
            "outlier_median": round(float(df["_logloss"].median()), 4),
            "outlier_max":    round(float(df["_logloss"].max()), 4),
            "global_mean":    round(float(all_logloss.mean()), 4),
            "global_median":  round(float(all_logloss.median()), 4),
        }
        ls = patterns["logloss_stats"]
        logger.info(f"  LL outliers mean: {ls['outlier_mean']:.4f} vs global: {ls['global_mean']:.4f}")

    # Résultat réel
    result_col = _resolve_col(df, ["_result", "result_1n2"])
    if result_col:
        dist     = df[result_col].value_counts()
        draw_pct = float((df[result_col] == "D").mean())
        patterns["result_distribution"]      = dist.to_dict()
        patterns["draw_overrepresentation"]  = round(draw_pct, 3)
        logger.info(f"  % Nuls parmi les outliers : {draw_pct:.1%} (baseline Big5 ≈ 26%)")

    # Championnat
    if "league_source" in df.columns:
        counts = df["league_source"].value_counts()
        patterns["league_distribution"] = counts.to_dict()
        logger.info(f"  Top ligues : {counts.head(3).to_dict()}")

    # Saison
    if "season" in df.columns:
        patterns["season_distribution"] = df["season"].value_counts().to_dict()

    # Couverture WhoScored
    if "has_ws_events" in df.columns:
        ws_pct     = float(df["has_ws_events"].mean())
        ws_missing = int((df["has_ws_events"] == 0).sum())
        patterns["ws_coverage_pct"]  = round(ws_pct, 3)
        patterns["ws_missing_count"] = ws_missing
        logger.info(f"  WhoScored events couverts : {ws_pct:.1%} ({ws_missing} manquants)")

    # Features manquantes
    diag_features = [
        "np_xg_roll_5", "ppda_roll_5", "xg_overperformance_5",
        "sterility_index_5", "press_resistance_5", "ws_shots_ot_pg",
    ]
    for col in diag_features:
        if col in df.columns:
            miss = float(df[col].isna().mean())
            patterns[f"missing_{col}"] = round(miss, 3)
            if miss > 0.1:
                logger.warning(f"  '{col}' : {miss:.1%} NULL parmi les outliers !")

    # xG signals
    if "np_xg_roll_5" in df.columns and "np_xg_conceded_roll_5" in df.columns:
        xg_net = (df["np_xg_roll_5"] - df["np_xg_conceded_roll_5"]).mean()
        patterns["xg_net_outliers_mean"] = round(float(xg_net), 4) if pd.notna(xg_net) else None

    if "xg_overperformance_5" in df.columns:
        opi = df["xg_overperformance_5"].mean()
        patterns["xg_opi_mean_outliers"] = round(float(opi), 4) if pd.notna(opi) else None

    # Top 10
    display_cols = [
        "_match_id_key", "date", "team", "opponent", "league_source",
        "season", "_result", "_logloss", "has_ws_events",
    ]
    available = [c for c in display_cols if c in df.columns]
    patterns["top10_worst"] = df.nlargest(10, "_logloss")[available].copy()

    return patterns


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 4 — RAPPORT MARKDOWN
# ══════════════════════════════════════════════════════════════════════════════

def generate_report(df: pd.DataFrame, patterns: dict, top_n: int) -> Path:
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    L = []  # lignes du rapport

    def h(text): L.extend([f"## {text}", ""])
    def line(text=""): L.append(text)

    L += [
        "# 📊 Agent Analyste — Rapport d'Insights", "",
        f"**Généré le :** {now}  ",
        f"**Source :** `models/predictions_val.csv` — top {top_n} outliers (Log Loss maximum)  ",
        "**Base :** `gold.features_final` × `silver.stg_whoscored_events`",
        "", "---", "",
    ]

    h("1. Vue d'ensemble")
    L += [
        "| Métrique | Outliers (top 50) | Population globale |",
        "|---|---|---|",
        f"| Matchs analysés | {len(df)} | — |",
    ]
    if "logloss_stats" in patterns:
        ls = patterns["logloss_stats"]
        L += [
            f"| Log Loss moyen | **{ls['outlier_mean']:.4f}** | {ls['global_mean']:.4f} |",
            f"| Log Loss médian | **{ls['outlier_median']:.4f}** | {ls['global_median']:.4f} |",
            f"| Log Loss max | **{ls['outlier_max']:.4f}** | — |",
        ]
    if "ws_coverage_pct" in patterns:
        L += [
            f"| Couverture WhoScored events | {patterns['ws_coverage_pct']:.1%} | — |",
            f"| Matchs sans events WS | {patterns['ws_missing_count']} | — |",
        ]
    L += ["", "---", ""]

    h("2. Distribution par championnat")
    if "league_distribution" in patterns:
        L += ["| Championnat | # Outliers |", "|---|---|"]
        for league, cnt in sorted(patterns["league_distribution"].items(), key=lambda x: -x[1]):
            L.append(f"| {league} | {cnt} |")
    else:
        L.append("_Métadonnées non disponibles (gold.features_final non joint)_")
    L += ["", "---", ""]

    h("3. Distribution par résultat réel")
    if "result_distribution" in patterns:
        total = sum(patterns["result_distribution"].values())
        L += ["| Résultat | # | % |", "|---|---|---|"]
        for res in ["H", "D", "A"]:
            cnt = patterns["result_distribution"].get(res, 0)
            L.append(f"| {res} | {cnt} | {cnt/total:.1%} |")
        draw_pct = patterns.get("draw_overrepresentation", 0)
        delta    = draw_pct - 0.26
        flag     = f"⚠️ Sur-représenté (+{delta:.1%})" if delta > 0.05 else "✅ Normal"
        L += ["", f"**% Nuls :** {draw_pct:.1%} (baseline Big5 ≈ 26%) → {flag}"]
    else:
        L.append("_Non disponible_")
    L += ["", "---", ""]

    h("4. Features diagnostiques — valeurs NULL (outliers uniquement)")
    feature_flags = {k: v for k, v in patterns.items() if k.startswith("missing_")}
    if feature_flags:
        L += ["| Feature | % NULL | Criticité |", "|---|---|---|"]
        for feat, pct in sorted(feature_flags.items(), key=lambda x: -x[1]):
            fname    = feat.replace("missing_", "")
            severity = "🔴 CRITIQUE" if pct > 0.3 else ("🟡 Modéré" if pct > 0.1 else "🟢 OK")
            L.append(f"| `{fname}` | {pct:.1%} | {severity} |")
    else:
        L.append("_Aucune feature manquante détectée_")
    L += ["", "---", ""]

    if "xg_net_outliers_mean" in patterns:
        h("5. Signaux xG")
        xg_net = patterns["xg_net_outliers_mean"]
        L += [
            f"**xG net moyen (outliers)** : `{xg_net:+.4f}`  ",
            "_Un xG net positif chez les équipes qui ont mal performé = occasion ratées / malchance_",
        ]
        opi = patterns.get("xg_opi_mean_outliers")
        if opi is not None:
            L.append(f"\n**xG overperformance index moyen** : `{opi:+.4f}`")
        L += ["", "---", ""]

    h("6. Les 50 matchs les plus catastrophiques")
    if "_logloss" in df.columns and not df.empty:
        display_cols = [
            "_match_id_key", "date", "team", "opponent", "league_source",
            "season", "_result", "_logloss", "has_ws_events",
        ]
        available = [c for c in display_cols if c in df.columns]
        col_labels = {
            "_match_id_key": "Match ID", "date": "Date", "team": "Équipe",
            "opponent": "Adversaire", "league_source": "Ligue", "season": "Saison",
            "_result": "Résultat", "_logloss": "Log Loss", "has_ws_events": "WS",
        }
        # Trier par Log Loss décroissant
        df_display = df.nlargest(len(df), "_logloss")[available].copy()

        L.append("| " + " | ".join(col_labels[c] for c in available) + " |")
        L.append("| " + " | ".join("---" for _ in available) + " |")
        for _, row in df_display.iterrows():
            vals = []
            for c in available:
                v = row[c]
                if c == "_logloss":        vals.append(f"{v:.4f}")
                elif c == "has_ws_events": vals.append("✅" if int(v) == 1 else "❌")
                elif pd.isna(v):           vals.append("_")
                else:                      vals.append(str(v))
            L.append("| " + " | ".join(vals) + " |")
    else:
        L.append("_Non disponible_")
    L += ["", "---", ""]

    h("7. Hypothèses et pistes d'action")
    hypotheses = []

    draw_pct = patterns.get("draw_overrepresentation", 0)
    if draw_pct > 0.30:
        hypotheses.append(
            f"**H1 — Nuls sous-modélisés :** {draw_pct:.1%} des outliers sont des nuls "
            f"(baseline ≈ 26%). → Intégrer `draw_rate_5` et `draw_affinity` "
            f"(`03c_suggested_features.py`, feature H2)."
        )

    ws_pct = patterns.get("ws_coverage_pct", 1.0)
    if ws_pct < 0.7:
        hypotheses.append(
            f"**H2 — Coverage WhoScored insuffisante :** couverture {ws_pct:.1%} "
            f"({patterns.get('ws_missing_count', '?')} matchs sans events). "
            f"→ Relancer `scrape_whoscored_details.py` sur les `final_match_id` manquants."
        )

    for feat, pct in feature_flags.items():
        if pct > 0.2:
            fname = feat.replace("missing_", "")
            hypotheses.append(
                f"**H3 — `{fname}` lacunaire :** {pct:.1%} NULL. "
                f"→ Vérifier la couverture scraping pour les matchs concernés (pipeline 03b)."
            )

    opi = patterns.get("xg_opi_mean_outliers")
    if opi is not None and abs(opi) > 0.15:
        direction = "surchance" if opi > 0 else "malchance"
        hypotheses.append(
            f"**H4 — Signal xG OPI :** moyenne `{opi:+.4f}` ({direction}). "
            f"→ Le modèle ne corrige pas assez pour la régression vers la moyenne xG."
        )

    if not hypotheses:
        hypotheses.append(
            "_Aucun pattern dominant. L'erreur est distribuée uniformément — "
            "considérer un audit par type de match ou par tranche de cotes._"
        )

    for h_text in hypotheses:
        L += [f"- {h_text}", ""]

    L += [
        "---", "",
        "> Rapport généré par `agents/auditor.py` — Projet 3-Étoiles  ",
        "> Features candidates : `python pipelines/03c_suggested_features.py --dry-run`",
    ]

    REPORT_OUT.write_text("\n".join(L), encoding="utf-8")
    logger.success(f"  Rapport sauvegardé : {REPORT_OUT}")
    return REPORT_OUT


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run(context: dict | None = None, top_n: int = TOP_N) -> dict:
    if context is None:
        context = {}

    logger.info(f"Démarrage analyse — top {top_n} outliers (source: predictions_val.csv)")

    df_outliers, all_logloss = load_outliers(top_n)
    df_enriched  = enrich_with_duckdb(df_outliers)
    patterns     = analyse_patterns(df_enriched, all_logloss)
    report_path  = generate_report(df_enriched, patterns, top_n)

    logger.success(f"{top_n} outliers identifiés — rapport : {report_path.name}")

    context.update({
        "outliers_df": df_enriched,
        "all_logloss": all_logloss,
        "patterns":    patterns,
        "report_path": str(report_path),
        "top_n":       top_n,
    })
    return context


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agent Analyste — Projet 3-Étoiles")
    parser.add_argument("--top-n", type=int, default=TOP_N)
    args = parser.parse_args()
    run(top_n=args.top_n)