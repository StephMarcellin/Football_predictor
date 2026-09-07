"""
poisson_markets.py — Dérivation des marchés depuis les buts attendus (λ).

À partir de λ_home et λ_away (sorties du modèle de buts), construit la matrice des
scores via la loi de Poisson, applique la correction Dixon-Coles (dépendance des
petits scores : deux Poissons indépendants sous-estiment les nuls), et en dérive
1X2, over/under, BTTS.

Fonctions pures — aucune I/O, testables et réutilisables (train comme service).
"""
from math import lgamma

import numpy as np

MAX_GOALS = 10          # tronque la matrice ; P(>10 buts par équipe) ≈ 0


def _poisson_pmf(lam, kmax=MAX_GOALS):
    """P(k) pour k=0..kmax, sans scipy (log-factorielle via lgamma)."""
    k = np.arange(kmax + 1)
    logf = np.array([lgamma(x + 1.0) for x in k])
    return np.exp(-lam + k * np.log(max(lam, 1e-9)) - logf)


def _dixon_coles_tau(i, j, lam_h, lam_a, rho):
    """Correction DC sur les 4 petits scores (0-0, 0-1, 1-0, 1-1).
    rho < 0 gonfle 0-0 et 1-1 (les nuls fermés), rho > 0 l'inverse."""
    if i == 0 and j == 0:
        return 1.0 - lam_h * lam_a * rho
    if i == 0 and j == 1:
        return 1.0 + lam_h * rho
    if i == 1 and j == 0:
        return 1.0 + lam_a * rho
    if i == 1 and j == 1:
        return 1.0 - rho
    return 1.0


def score_matrix(lam_h, lam_a, rho=0.0, max_goals=MAX_GOALS):
    """Matrice (max_goals+1)² : M[i, j] = P(home marque i, away marque j). Normalisée."""
    ph = _poisson_pmf(lam_h, max_goals)
    pa = _poisson_pmf(lam_a, max_goals)
    M = np.outer(ph, pa)
    if rho != 0.0:
        for i in (0, 1):
            for j in (0, 1):
                M[i, j] *= _dixon_coles_tau(i, j, lam_h, lam_a, rho)
    return M / M.sum()


def markets_from_lambdas(lam_h, lam_a, rho=0.0, ou_line=2.5):
    """Probas de marché depuis λ_home, λ_away. 1X2 vu du MATCH (H=domicile gagne)."""
    M = score_matrix(lam_h, lam_a, rho)
    n = M.shape[0]
    total = np.arange(n)[:, None] + np.arange(n)[None, :]
    over = float(M[total > ou_line].sum())
    return {
        "prob_H":     float(np.tril(M, -1).sum()),   # home > away
        "prob_D":     float(np.trace(M)),            # home = away
        "prob_A":     float(np.triu(M, 1).sum()),    # home < away
        "prob_over":  over,
        "prob_under": 1.0 - over,
        "prob_btts":  float(M[1:, 1:].sum()),        # les deux marquent ≥ 1
        "exp_goals":  float(lam_h + lam_a),
    }


def _goal_bucket(marginal, cap=6):
    """Distribution 0,1,...,(cap-1),cap+ à partir d'une marginale (row/col sums)."""
    d = {str(k): float(marginal[k]) for k in range(cap)}
    d[f"{cap}+"] = float(marginal[cap:].sum())
    return d


def full_markets(lam_h, lam_a, rho=0.0,
                 ou_lines=(0.5, 1.5, 2.5, 3.5, 4.5), top_scores=6):
    """Vue DÉTAILLÉE d'un match depuis λ_home, λ_away : distribution de buts par
    équipe (0..6+), 1X2, tous les over/under, clean sheets, BTTS, scores exacts.

    Tout vient d'UNE matrice de scores Poisson (+ Dixon-Coles) — aucune hypothèse
    supplémentaire, chaque marché est juste une somme de cases."""
    M = score_matrix(lam_h, lam_a, rho)
    n = M.shape[0]
    home_marg = M.sum(axis=1)          # buts domicile (marginale)
    away_marg = M.sum(axis=0)          # buts extérieur (marginale)
    total = np.arange(n)[:, None] + np.arange(n)[None, :]

    ou = {}
    for L in ou_lines:
        over = float(M[total > L].sum())
        ou[str(L)] = {"over": over, "under": 1.0 - over}

    flat = sorted(((f"{i}-{j}", float(M[i, j]))
                   for i in range(n) for j in range(n)), key=lambda x: -x[1])

    return {
        "exp_goals_home":  float(lam_h),
        "exp_goals_away":  float(lam_a),
        "exp_goals_total": float(lam_h + lam_a),
        "goals_home":      _goal_bucket(home_marg),      # {'0':.., '1':.., ..., '6+':..}
        "goals_away":      _goal_bucket(away_marg),
        "result":          {"H": float(np.tril(M, -1).sum()),
                            "D": float(np.trace(M)),
                            "A": float(np.triu(M, 1).sum())},
        "over_under":      ou,                            # par ligne : {'2.5': {over, under}, ...}
        "clean_sheet_home": float(M[:, 0].sum()),         # l'extérieur ne marque pas
        "clean_sheet_away": float(M[0, :].sum()),         # le domicile ne marque pas
        "btts":            {"yes": float(M[1:, 1:].sum()),
                            "no":  float(1.0 - M[1:, 1:].sum())},
        "top_scores":      [{"score": s, "prob": p} for s, p in flat[:top_scores]],
    }
