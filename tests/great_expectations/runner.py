"""
tests/great_expectations/runner.py

Runner Great Expectations headless, Windows-friendly, pandas-natif.

Ne dépend PAS d'un DataContext GX (trop lourd pour Windows/CI/paths accentués).
Lit un YAML de suite, exécute la query DuckDB, applique les expectations et
retourne un objet ExpectationSuiteResult avec .success / .report() / .junit_xml().

Chaque expectation a une implémentation pandas native — GE n'est pas requis.
Compatible with pytest via un simple `assert result.success, result.report()`.

Usage direct (CLI) :
    python tests/great_expectations/runner.py tests/great_expectations/gold/equipe_confrontation_zone.yml
"""
from __future__ import annotations
import sys
import json
import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml
import duckdb
import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
# Résultats
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ExpectationResult:
    name: str
    column: str | None
    kwargs: dict[str, Any]
    severity: str
    success: bool
    observed: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def is_blocking(self) -> bool:
        """Un échec 'warn' ne fait pas planter la suite, seul 'error' bloque."""
        return (not self.success) and self.severity == "error"


@dataclass
class SuiteResult:
    suite_name: str
    duckdb_path: str
    n_rows: int
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not any(r.is_blocking() for r in self.results)

    def report(self) -> str:
        lines = [
            "═" * 78,
            f"Suite Great Expectations : {self.suite_name}",
            f"  Source : {self.duckdb_path} — {self.n_rows} lignes chargées",
            "═" * 78,
        ]
        n_pass = sum(1 for r in self.results if r.success)
        n_warn = sum(1 for r in self.results if not r.success and r.severity == "warn")
        n_err = sum(1 for r in self.results if not r.success and r.severity == "error")
        lines.append(f"  ✓ Succès : {n_pass}   ⚠ Warnings : {n_warn}   ✗ Erreurs : {n_err}")
        lines.append("")
        for r in self.results:
            mark = "✓" if r.success else ("⚠" if r.severity == "warn" else "✗")
            col = f"[{r.column}]" if r.column else ""
            observed = ""
            if r.observed:
                observed = "  observed=" + json.dumps(r.observed, default=str)
            err = f"  ERROR={r.error}" if r.error else ""
            lines.append(f"  {mark} {r.name} {col}  kwargs={r.kwargs}{observed}{err}")
        lines.append("═" * 78)
        return "\n".join(lines)

    def junit_xml(self) -> str:
        """Export JUnit XML pour intégration GitHub Actions."""
        import xml.etree.ElementTree as ET
        suite = ET.Element(
            "testsuite",
            name=self.suite_name,
            tests=str(len(self.results)),
            failures=str(sum(1 for r in self.results if not r.success and r.severity == "error")),
        )
        for r in self.results:
            case = ET.SubElement(
                suite,
                "testcase",
                classname=self.suite_name,
                name=f"{r.name}[{r.column or '-'}]",
            )
            if not r.success:
                tag = "failure" if r.severity == "error" else "warning"
                fail = ET.SubElement(case, tag)
                fail.text = json.dumps({"kwargs": r.kwargs, "observed": r.observed, "error": r.error})
        return ET.tostring(suite, encoding="unicode")


# ══════════════════════════════════════════════════════════════════════════════
# Registry des expectations — pandas-natif, extensible
# ══════════════════════════════════════════════════════════════════════════════
_REGISTRY: dict[str, Callable[[pd.DataFrame, dict], tuple[bool, dict, str | None]]] = {}


def register(name: str):
    def _wrap(fn):
        _REGISTRY[name] = fn
        return fn
    return _wrap


@register("expect_table_row_count_to_be_between")
def _expect_table_row_count_to_be_between(df: pd.DataFrame, kw: dict):
    n = len(df)
    ok = kw["min_value"] <= n <= kw["max_value"]
    return ok, {"row_count": n}, None


@register("expect_column_values_to_not_be_null")
def _expect_column_values_to_not_be_null(df: pd.DataFrame, kw: dict):
    col = kw["column"]
    mostly = kw.get("mostly", 1.0)
    if col not in df.columns:
        return False, {}, f"Column '{col}' absente du dataset."
    non_null_ratio = float(df[col].notna().mean())
    ok = non_null_ratio >= mostly
    return ok, {"non_null_ratio": round(non_null_ratio, 4), "mostly_required": mostly}, None


@register("expect_column_value_zero_ratio_to_be_below")
def _expect_column_value_zero_ratio_to_be_below(df: pd.DataFrame, kw: dict):
    col = kw["column"]
    max_ratio = kw["max_ratio"]
    if col not in df.columns:
        return False, {}, f"Column '{col}' absente du dataset."
    s = df[col].dropna()
    if len(s) == 0:
        return False, {"zero_ratio": None}, "Colonne 100 % NULL — impossible d'évaluer les zéros."
    zero_ratio = float((s == 0).mean())
    ok = zero_ratio <= max_ratio
    return ok, {"zero_ratio": round(zero_ratio, 4), "max_allowed": max_ratio}, None


@register("expect_column_mean_to_be_between")
def _expect_column_mean_to_be_between(df: pd.DataFrame, kw: dict):
    col = kw["column"]
    if col not in df.columns:
        return False, {}, f"Column '{col}' absente du dataset."
    s = df[col].dropna()
    if len(s) == 0:
        return False, {"mean": None}, "Colonne vide — impossible de calculer la moyenne."
    mean = float(s.mean())
    ok = kw["min_value"] <= mean <= kw["max_value"]
    return ok, {"mean": round(mean, 4), "bounds": [kw["min_value"], kw["max_value"]]}, None


@register("expect_column_values_to_be_between")
def _expect_column_values_to_be_between(df: pd.DataFrame, kw: dict):
    col = kw["column"]
    if col not in df.columns:
        return False, {}, f"Column '{col}' absente du dataset."
    s = df[col].dropna()
    if len(s) == 0:
        return True, {"n_checked": 0}, None  # vide = trivialement vrai
    out_of_range = ((s < kw["min_value"]) | (s > kw["max_value"])).sum()
    ok = out_of_range == 0
    return ok, {"out_of_range": int(out_of_range), "n_checked": int(len(s))}, None


@register("expect_partition_volume_stability")
def _expect_partition_volume_stability(df: pd.DataFrame, kw: dict):
    """Compare la dernière partition à la moyenne des N précédentes."""
    col = kw["partition_col"]
    warn_dev = kw.get("max_deviation_warn", 0.20)
    err_dev = kw.get("max_deviation_error", 0.40)
    min_hist = kw.get("min_seasons_history", 2)
    if col not in df.columns:
        return False, {}, f"Colonne partition '{col}' absente."
    counts = df.groupby(col).size().sort_index()
    if len(counts) < min_hist + 1:
        return True, {"skipped": True, "n_partitions": int(len(counts))}, None
    last = counts.iloc[-1]
    ref = counts.iloc[-(min_hist + 1):-1].mean()
    dev = abs(last - ref) / ref if ref > 0 else float("inf")
    ok = dev <= err_dev
    obs = {"last_partition": counts.index[-1], "last_count": int(last),
           "ref_mean": round(float(ref), 1), "deviation": round(float(dev), 3),
           "warn_threshold": warn_dev, "error_threshold": err_dev}
    return ok, obs, None


# ══════════════════════════════════════════════════════════════════════════════
# Runner principal
# ══════════════════════════════════════════════════════════════════════════════
def run_suite(yaml_path: str | Path) -> SuiteResult:
    yaml_path = Path(yaml_path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Suite YAML introuvable : {yaml_path}")

    spec = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    suite_name = spec["suite_name"]
    source = spec["source"]

    # Résolution du chemin DuckDB — relatif à la racine du projet (2 niveaux au-dessus du runner)
    project_root = Path(__file__).resolve().parents[2]
    duckdb_path = (project_root / source["duckdb_path"]).resolve()
    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB introuvable : {duckdb_path}")

    con = duckdb.connect(str(duckdb_path), read_only=True)
    df = con.execute(source["query"]).fetch_df()
    con.close()

    result = SuiteResult(suite_name=suite_name, duckdb_path=str(duckdb_path), n_rows=len(df))

    for exp in spec["expectations"]:
        exp_type = exp["type"]
        severity = exp.get("severity", "error")
        kwargs_base = exp.get("kwargs", {})
        # Multiplexage : for_each_column > kwargs.column > table-level (None)
        if exp.get("for_each_column"):
            cols = list(exp["for_each_column"])
        elif "column" in kwargs_base:
            cols = [kwargs_base["column"]]
        else:
            cols = [None]  # expectation table-level

        for col in cols:
            kw = dict(kwargs_base)
            if col is not None:
                kw["column"] = col

            fn = _REGISTRY.get(exp_type)
            if fn is None:
                result.results.append(ExpectationResult(
                    name=exp_type, column=col, kwargs=kw, severity=severity,
                    success=False, error=f"Expectation type inconnue : {exp_type}"))
                continue

            try:
                ok, observed, err = fn(df, kw)
            except Exception as e:  # noqa: BLE001
                ok, observed, err = False, {}, f"Exception: {e!r}"

            result.results.append(ExpectationResult(
                name=exp_type, column=col, kwargs=kw, severity=severity,
                success=ok, observed=observed, error=err))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Runner Great Expectations pandas-natif.")
    parser.add_argument("suite", help="Chemin vers le YAML de la suite.")
    parser.add_argument("--junit", help="Chemin de sortie JUnit XML (optionnel).")
    args = parser.parse_args(argv)

    res = run_suite(args.suite)
    print(res.report())
    if args.junit:
        Path(args.junit).write_text(res.junit_xml(), encoding="utf-8")
        print(f"\nJUnit XML écrit : {args.junit}")
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
