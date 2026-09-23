"""Diagnostic lecture seule des erreurs GE silver (fbref_misc / fbref_shooting).
Ecrit son resultat dans diag1_out.txt (meme dossier). Ne modifie aucune donnee."""
import sys, traceback
from pathlib import Path
import duckdb

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DB = ROOT / "db" / "football_dev.duckdb"
OUT = HERE / "diag1_out.txt"
RAW = ROOT / "data" / "raw" / "fbref" / "parquet"

f = open(OUT, "w", encoding="utf-8")
def w(*a):
    print(*a, file=f); f.flush()

def q(con, title, sql, maxrows=60):
    w(f"\n### {title}")
    try:
        df = con.execute(sql).df()
        with __import__("pandas").option_context("display.width", 250, "display.max_columns", 50, "display.max_colwidth", 60):
            w(df.head(maxrows).to_string(index=False))
        w(f"({len(df)} lignes)")
    except Exception as e:
        w("ERREUR:", repr(e))

w("DB:", DB, "existe:", DB.exists())
try:
    con = duckdb.connect(str(DB), read_only=True)
except Exception as e:
    w("CONNEXION KO:", repr(e)); sys.exit(1)

# ---- 0. schemas -------------------------------------------------------------
q(con, "tables silver / intermediate.match_registry",
  "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('silver') OR table_name='match_registry' ORDER BY 1,2", 100)
q(con, "colonnes silver.fbref_shooting", "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='silver' AND table_name='fbref_shooting' ORDER BY ordinal_position", 100)
q(con, "colonnes silver.fbref_misc", "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='silver' AND table_name='fbref_misc' ORDER BY ordinal_position", 100)
for t in ("stg_whoscored_events_by_match", "stg_whoscored_team_match", "stg_whoscored_match_index", "stg_whoscored_events_physical", "stg_whoscored_events_by_season"):
    q(con, f"colonnes silver.{t}", f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='silver' AND table_name='{t}' ORDER BY ordinal_position", 100)
q(con, "colonnes match_registry", "SELECT table_schema, column_name, data_type FROM information_schema.columns WHERE table_name='match_registry' ORDER BY ordinal_position", 100)

# ---- 1. volumetrie ---------------------------------------------------------
q(con, "fbref_misc / shooting : lignes par saison",
  """SELECT m.season, m.n_misc, s.n_shoot FROM
     (SELECT season, count(*) n_misc FROM silver.fbref_misc GROUP BY 1) m
     FULL JOIN (SELECT season, count(*) n_shoot FROM silver.fbref_shooting GROUP BY 1) s USING (season) ORDER BY 1""", 40)
q(con, "fbref_misc : lignes par saison x comp_category",
  "SELECT season, comp_category, count(*) n, min(date) dmin, max(date) dmax FROM silver.fbref_misc GROUP BY 1,2 ORDER BY 1,2", 120)
q(con, "2026-2027 : ce qui est present", "SELECT league_source, count(*) n, min(date) dmin, max(date) dmax FROM silver.fbref_misc WHERE season='2026-2027' GROUP BY 1 ORDER BY 2 DESC", 40)
q(con, "2025-2026 : ce qui est present", "SELECT comp_category, count(*) n, min(date) dmin, max(date) dmax FROM silver.fbref_misc WHERE season='2025-2026' GROUP BY 1 ORDER BY 2 DESC", 40)

# ---- 2. fbref_misc : int_col hors bornes ------------------------------------
q(con, "misc : int > 55 (les 2 hors bornes)",
  """SELECT date, team, opponent, league_source, season, venue, "int" AS int_col, tklw, fls, fld, crosses, source
     FROM silver.fbref_misc WHERE "int" > 55 ORDER BY "int" DESC""")
q(con, "misc : top 15 valeurs int", """SELECT date, team, opponent, league_source, "int" AS int_col FROM silver.fbref_misc ORDER BY "int" DESC LIMIT 15""")
q(con, "misc : distribution int par comp_category",
  """SELECT comp_category, count(*) n, round(avg("int"),2) mean_int, max("int") max_int, quantile_cont("int",0.999) p999 FROM silver.fbref_misc GROUP BY 1 ORDER BY 1""")

# ---- 3. fbref_shooting : taux de zeros / nulls par comp_category x saison ----
q(con, "shooting : par comp_category",
  """SELECT comp_category, count(*) n,
        sum((standard_sh = 0)::INT) sh_zero,
        round(100.0*sum((standard_sh = 0)::INT)/count(*),1) pct_sh_zero,
        sum((standard_sot_pct IS NULL)::INT) sotpct_null,
        sum((standard_gls > standard_sot)::INT) gls_gt_sot,
        sum((standard_pk > standard_pkatt)::INT) pk_gt_pkatt
     FROM silver.fbref_shooting GROUP BY 1 ORDER BY 1""")
q(con, "shooting : par saison",
  """SELECT season, count(*) n,
        round(100.0*sum((standard_sh = 0)::INT)/count(*),1) pct_sh_zero,
        round(100.0*sum((standard_sot_pct IS NULL)::INT)/count(*),1) pct_sotpct_null,
        sum((standard_gls > standard_sot)::INT) gls_gt_sot,
        sum((standard_pk > standard_pkatt)::INT) pk_gt_pkatt,
        sum((standard_sot > standard_sh)::INT) sot_gt_sh
     FROM silver.fbref_shooting GROUP BY 1 ORDER BY 1""", 40)
q(con, "shooting : gls>sot ventile selon sh=0 ?",
  """SELECT (standard_sh = 0) AS sh_is_zero, (standard_sot = 0) AS sot_is_zero, count(*) n
     FROM silver.fbref_shooting WHERE standard_gls > standard_sot GROUP BY 1,2 ORDER BY 3 DESC""")
q(con, "shooting : gls>sot avec sh>0 (violations 'reelles')",
  """SELECT date, team, opponent, league_source, season, gf, ga, standard_gls, standard_sh, standard_sot, standard_pk, standard_pkatt
     FROM silver.fbref_shooting WHERE standard_gls > standard_sot AND standard_sh > 0 ORDER BY date DESC LIMIT 25""")
q(con, "shooting : sot > sh", """SELECT date, team, opponent, league_source, season, standard_gls, standard_sh, standard_sot, standard_sot_pct FROM silver.fbref_shooting WHERE standard_sot > standard_sh""")
q(con, "shooting : g_sh hors [0,1]", """SELECT date, team, opponent, league_source, season, standard_gls, standard_sh, standard_sot, standard_g_sh, standard_g_sot FROM silver.fbref_shooting WHERE standard_g_sh > 1 OR standard_g_sh < 0 OR standard_g_sot > 2""")
q(con, "shooting : gls > gf", """SELECT date, team, opponent, league_source, season, gf, ga, standard_gls, standard_sh, standard_sot FROM silver.fbref_shooting WHERE standard_gls > gf ORDER BY date""")
q(con, "shooting : pk > pkatt par comp_category x saison (top)",
  """SELECT comp_category, season, count(*) n_viol, sum((standard_pkatt = 0)::INT) pkatt_zero, sum((standard_sh = 0)::INT) sh_zero
     FROM silver.fbref_shooting WHERE standard_pk > standard_pkatt GROUP BY 1,2 ORDER BY 3 DESC""", 30)
q(con, "shooting : exemples pk > pkatt", """SELECT date, team, opponent, league_source, season, standard_gls, standard_sh, standard_sot, standard_pk, standard_pkatt FROM silver.fbref_shooting WHERE standard_pk > standard_pkatt AND standard_sh > 0 ORDER BY date DESC LIMIT 20""")

# ---- 4. Bronze : vide/NULL a la source ? -----------------------------------
sh_glob = str(RAW / "shooting" / "*.parquet").replace("\\", "/")
q(con, "BRONZE shooting : shots vide/NULL par comp (top 25 par nb lignes vides)",
  f"""SELECT comp, count(*) n,
        sum((shots IS NULL OR trim(shots) = '')::INT) shots_empty,
        round(100.0*sum((shots IS NULL OR trim(shots) = '')::INT)/count(*),1) pct_empty
      FROM read_parquet('{sh_glob}', union_by_name=true) GROUP BY 1 ORDER BY 3 DESC""", 25)
q(con, "BRONZE shooting : shots vide par saison",
  f"""SELECT season, count(*) n, round(100.0*sum((shots IS NULL OR trim(shots) = '')::INT)/count(*),1) pct_empty
      FROM read_parquet('{sh_glob}', union_by_name=true) GROUP BY 1 ORDER BY 1""", 40)
q(con, "BRONZE shooting : pens_att vides / pens_made>pens_att bruts",
  f"""SELECT sum((pens_att IS NULL OR trim(pens_att)='')::INT) pkatt_empty,
             sum((pens_made IS NULL OR trim(pens_made)='')::INT) pk_empty,
             sum((try_cast(pens_made AS INT) > try_cast(pens_att AS INT))::INT) pk_gt_pkatt_raw,
             count(*) n
      FROM read_parquet('{sh_glob}', union_by_name=true)""")
w("\nFIN")
