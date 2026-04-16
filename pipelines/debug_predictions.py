import duckdb
import pandas as pd

con = duckdb.connect("db/football.duckdb", read_only=True)

df = con.execute("""
    SELECT *
    FROM gold.features_final
    WHERE result_1n2 IS NULL
      AND comp_category = 'Big5'
      AND (team = 'Paris Saint-Germain' OR opponent = 'Paris Saint-Germain')
    ORDER BY date
""").df()
print(con.execute("""
    SELECT season, team, season_att_rating, season_def_rating, date
    FROM gold.features_final
    WHERE comp_category = 'Big5'
      AND team = 'Paris Saint-Germain'
    ORDER BY date DESC
    LIMIT 20
""").df().to_string())
con.close()
print(df[["date", "team", "opponent", "venue", "np_xg_roll_5", "season_att_rating", "season_def_rating", "xg_net_roll_5"]].to_string())