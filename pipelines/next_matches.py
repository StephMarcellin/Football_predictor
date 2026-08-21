"""
next_matches.py — Produit/rafraîchit le fichier des prochains matchs à prédire.

Pour chaque championnat de config.yaml → predict.leagues, sélectionne la
PROCHAINE JOURNÉE à jouer : le plus proche groupe de matchs sans résultat
(result_1n2 NULL), regroupés par fenêtre de dates (predict.next_matchday_window_days,
défaut 4 j). Indépendant de la date système. Écrit un CSV (match_id, league, date)
— écrase le fichier à chaque run.

Appelé par l'orchestrateur avant predict_1n2 (qui lit --match-ids-file).

Lancement : python pipelines/next_matches.py
"""
import duckdb

import ml_common as mc


def next_matches(cfg):
    """Retourne un df (match_id, league, date) : la prochaine journée non jouée
    de chaque championnat configuré."""
    pcfg = cfg.get("predict", {})
    leagues = pcfg.get("leagues", [])
    window = pcfg.get("next_matchday_window_days", 4)
    if not leagues:
        raise SystemExit("config.yaml → predict.leagues est vide : rien à produire.")

    placeholders = ", ".join("?" * len(leagues))     # IN (?, ?, ...) sûr (pas de f-string sur les valeurs)
    con = duckdb.connect(str(mc.ROOT_DIR / cfg["paths"]["duckdb"]), read_only=True)
    df = con.execute(f"""
        with unplayed as (
            select distinct m.match_id, b.league_source as league, m.date
            from marts.mart_1n2 m
            join intermediate.backbone b using (match_id, team_id)
            where m.result_1n2 is null and b.league_source in ({placeholders})
        ),
        firsts as (select league, min(date) as d0 from unplayed group by league)
        select u.match_id, u.league, u.date
        from unplayed u join firsts f using (league)
        where date_diff('day', f.d0, u.date) between 0 and ?
        order by u.league, u.date, u.match_id
    """, leagues + [window]).df()
    con.close()
    return df


def main():
    cfg, _ = mc.load_configs()
    out_path = mc.ROOT_DIR / cfg["predict"]["next_matches_file"]
    df = next_matches(cfg)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    if df.empty:
        print(f"Aucun match non joué pour les championnats configurés. Fichier vidé : {out_path}")
    else:
        print(f"{len(df)} matchs sur {df['league'].nunique()} championnat(s) → {out_path}")
        print(df.groupby("league")["match_id"].count().to_string())


if __name__ == "__main__":
    main()