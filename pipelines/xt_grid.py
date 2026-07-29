"""
xt_grid.py — Estimation de la grille Expected Threat (xT)
=========================================================
Lit intermediate.int_xt_actions (grille fine 16x12, 192 cases) — seule source, y
compris les pertes de balle (action_kind='turnover') — estime par itération de
valeur (Markov) la valeur xT de chaque case :

    xT(c) = shoot%(c)·goalProb(c) + move%(c)·Σ T(c→c')·xT(c')
    avec  shoot% + move% < 1  ;  le complément (1-shoot%-move%) = perte de balle,

puis écrit la grille (192 lignes) dans machine_learning.xt_grid.

Usage :
    python pipelines/xt_grid.py            # calcule ET écrit la table
    python pipelines/xt_grid.py --dry-run  # calcule et affiche, sans écrire
"""

import argparse
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import yaml
from loguru import logger

# ── Config (convention maison : ROOT_DIR + config.yaml) ───────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)
DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]

# ── Grille : source unique de la convention d'indexation ──────────────────────
N_COLS, N_ROWS = 16, 12
N_CELLS = N_COLS * N_ROWS            # 192

def cell_index(col, row):
    """(col,row) -> index plat 0..191. À réutiliser partout, jamais recalculer."""
    return col * N_ROWS + row


# ── Bloc 1 : agrégations SQL (le lourd reste dans DuckDB) ─────────────────────
def load_aggregates(con):
    """Rapatrie deux petits agrégats depuis int_xt_actions — jamais les lignes brutes."""
    # (a) stats par case de départ : tirs, buts, déplacements réussis, pertes
    cell_stats = con.sql("""
        SELECT
            col_from AS col,
            row_from AS row,
            COUNT(*) FILTER (WHERE action_kind='shot')                        AS shots,
            COUNT(*) FILTER (WHERE action_kind='shot' AND is_goal)            AS goals,
            COUNT(*) FILTER (WHERE action_kind='move' AND col_to IS NOT NULL) AS moves,
            COUNT(*) FILTER (WHERE action_kind='turnover')                    AS turnover
        FROM intermediate.int_xt_actions
        WHERE col_from IS NOT NULL AND row_from IS NOT NULL
        GROUP BY col_from, row_from
    """).to_df()

    # (b) comptes de transition case_départ -> case_arrivée (déplacements only)
    transitions = con.sql("""
        SELECT
            col_from AS col_o, row_from AS row_o,
            col_to   AS col_d, row_to   AS row_d,
            COUNT(*) AS n
        FROM intermediate.int_xt_actions
        WHERE action_kind = 'move'
          AND col_to   IS NOT NULL AND row_to   IS NOT NULL
          AND col_from IS NOT NULL AND row_from IS NOT NULL
        GROUP BY col_from, row_from, col_to, row_to
    """).to_df()

    return cell_stats, transitions


# ── Bloc 2 : probabilités par case, matrice T, itération de valeur ────────────
def build_probabilities(cell_stats):
    """Vecteurs (192,) : shoot%, goalProb, move%, + comptes bruts.

    Dénominateur = TOUTES les actions de possession de la case :
        actions = tirs + déplacements réussis + pertes de balle
    d'où shoot% + move% < 1 ; le complément (1-shoot%-move%) = proba de perdre le
    ballon (valeur 0) — c'est ce terme qui fait décroître la valeur vers sa cage.
        shoot%   = tirs / actions
        move%    = déplacements réussis / actions
        goalProb = buts / tirs
    """
    shots = np.zeros(N_CELLS)
    goals = np.zeros(N_CELLS)
    moves = np.zeros(N_CELLS)
    turn  = np.zeros(N_CELLS)
    for r in cell_stats.itertuples(index=False):
        i = cell_index(int(r.col), int(r.row))
        shots[i], goals[i], moves[i], turn[i] = r.shots, r.goals, r.moves, r.turnover

    actions = shots + moves + turn
    with np.errstate(divide="ignore", invalid="ignore"):
        shoot_pct = np.where(actions > 0, shots / actions, 0.0)
        move_pct  = np.where(actions > 0, moves / actions, 0.0)
        goal_prob = np.where(shots > 0, goals / shots, 0.0)
    return shoot_pct, goal_prob, move_pct, shots, goals, moves, turn


def build_transition_matrix(transitions):
    """transitions -> T (192,192) row-stochastique.

    T[o,d] = P(le ballon arrive en d | on le déplace depuis o).
    Chaque ligne somme à 1 là où il y a des déplacements, 0 sinon.
    """
    T = np.zeros((N_CELLS, N_CELLS))
    for r in transitions.itertuples(index=False):
        o = cell_index(int(r.col_o), int(r.row_o))
        d = cell_index(int(r.col_d), int(r.row_d))
        T[o, d] += r.n
    row_sums = T.sum(axis=1, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        T = np.where(row_sums > 0, T / row_sums, 0.0)
    return T


def value_iteration(shoot_pct, goal_prob, move_pct, T, tol=1e-9, max_iter=5000):
    """Résout xT par point fixe : v = r + move% ⊙ (T·v).

    r = shoot%·goalProb = récompense immédiate (tirer maintenant). À chaque passe,
    la valeur des zones dangereuses remonte vers les cases qui y mènent. Converge
    car à chaque action une part de la possession est perdue (move% < 1 partout).
    Retourne (xT (192,), n_iter, delta_final).
    """
    r = shoot_pct * goal_prob
    v = np.zeros(N_CELLS)
    for k in range(1, max_iter + 1):
        v_new = r + move_pct * (T @ v)
        delta = np.max(np.abs(v_new - v))
        v = v_new
        if delta < tol:
            return v, k, delta
    return v, max_iter, delta


# ── Bloc 3 : assemblage + écriture ────────────────────────────────────────────
def build_grid_frame(shots, goals, moves, turn, shoot_pct, goal_prob, move_pct, xt):
    """Assemble la grille finale : 192 lignes (une par case), prête à persister."""
    rows = []
    for c in range(N_COLS):
        for r in range(N_ROWS):
            i = cell_index(c, r)
            rows.append({
                "col": c, "row": r,
                "shots": int(shots[i]), "goals": int(goals[i]),
                "moves": int(moves[i]), "turnover": int(turn[i]),
                "shoot_pct": float(shoot_pct[i]), "goal_prob": float(goal_prob[i]),
                "move_pct": float(move_pct[i]), "xt": float(xt[i]),
            })
    return pd.DataFrame(rows)


def write_grid(con, grid_df):
    """Écrit la grille dans machine_learning.xt_grid (CREATE OR REPLACE = idempotent)."""
    con.execute("CREATE SCHEMA IF NOT EXISTS machine_learning")
    con.register("grid_df", grid_df)
    con.execute("CREATE OR REPLACE TABLE machine_learning.xt_grid AS SELECT * FROM grid_df")
    con.unregister("grid_df")
    n = con.sql("SELECT COUNT(*) FROM machine_learning.xt_grid").fetchone()[0]
    logger.info(f"machine_learning.xt_grid écrit : {n} lignes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estime la grille xT (Markov) et l'écrit dans machine_learning.xt_grid.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Calcule et affiche la grille sans l'écrire en base.")
    args = parser.parse_args()

    logger.info(f"Connexion ({'read-only' if args.dry_run else 'écriture'}) : {DB_PATH}")
    con = duckdb.connect(str(DB_PATH), read_only=args.dry_run)
    try:
        cell_stats, transitions = load_aggregates(con)
        logger.info(f"cell_stats : {cell_stats.shape[0]} cases | transitions : {transitions.shape[0]} paires")

        shoot_pct, goal_prob, move_pct, shots, goals, moves, turn = build_probabilities(cell_stats)
        T = build_transition_matrix(transitions)
        xt, n_iter, delta = value_iteration(shoot_pct, goal_prob, move_pct, T)

        logger.info(f"Itération de valeur : convergence en {n_iter} passes (delta={delta:.2e})")
        peak = int(np.argmax(xt))
        logger.info(f"xT min={xt.min():.5f} max={xt.max():.5f} | pic col={peak // N_ROWS} row={peak % N_ROWS}")

        grid_df = build_grid_frame(shots, goals, moves, turn, shoot_pct, goal_prob, move_pct, xt)

        if args.dry_run:
            logger.info("dry-run : grille NON écrite. xT moyen par colonne :")
            grid = xt.reshape(N_COLS, N_ROWS)
            for c in range(N_COLS):
                logger.info(f"  col {c:2d} : {grid[c].mean():.4f}")
        else:
            write_grid(con, grid_df)
    finally:
        con.close()
