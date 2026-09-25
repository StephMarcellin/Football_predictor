"""train_embedding.py — Phase 2 : embedding joueur via autoencoder dense (PyTorch).
Grain (player_id, season). Anti-leakage temporel : pour chaque saison cible N,
entraînement sur les profils source_season <= N-1 uniquement.
Écrit machine_learning.player_embedding_lag.
"""

import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import json
from pathlib import Path

import duckdb
import joblib
import mlflow
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import yaml
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = Path(CFG["paths"]["duckdb"])
MODELS_DIR = ROOT_DIR / "models" / "embedding"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

MIN_APPS = CFG["knn"]["min_apps"]

# Noms refondus (préfixe de type en tête), tels qu'exposés par gold.joueur_saison.
NON_ZONAL_FEATURES = [
    "dec_scorer_xg_per90_lag", "dec_scorer_shots_per90_lag",
    "dec_off_xg_per_shot_lag", "dec_off_chances_created_per90_lag", "dec_off_key_passes_per90_lag",
    "dec_def_aerial_win_rate_lag", "dec_def_actions_per90_lag", "dec_def_errors_per90_lag",
    "dec_player_card_propensity_lag",
    "dec_off_xgchain_per90_lag", "dec_off_xgbuildup_per90_lag",
    "dec_scorer_team_shot_share_lag", "int_scorer_penalty_taker_lag", "int_scorer_freekick_taker_lag",
    "dec_def_threat_conceded_per90_lag", "dec_scorer_xgot_overperformance_lag",
]

# Noms refondus, tels qu'exposés par gold.joueur_zone_saison.
ZONAL_FEATURES = [
    "dec_off_touch_share_by_zone_lag", "dec_off_shot_volume_by_zone_lag",
    "dec_off_danger_by_zone_lag", "dec_off_progressive_actions_by_zone_lag",
    "dec_off_cross_volume_by_zone_lag",
    "dec_def_duel_win_rate_by_zone_lag", "dec_def_actions_by_zone_lag",
]

AE_CFG = {
    "hidden": [128, 64],
    "bottleneck": 32,
    "dropout": 0.3,
    "lr": 1e-3,
    "weight_decay": 1e-4,
    "batch_size": 256,
    "max_epochs": 200,
    "patience": 15,
}

MLFLOW_URI = f"sqlite:///{ROOT_DIR}/mlflow.db"
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("phase2_player_embedding")


def shift_season(season: str, delta: int) -> str:
    y1, y2 = season.split("-")
    return f"{int(y1) + delta}-{int(y2) + delta}"


def build_features_matrix(con, seasons, min_apps=0):
    """Charge le profil (non-zonal + zonal wide) pour une ou plusieurs saisons source."""
    if isinstance(seasons, str):
        seasons = [seasons]

    non_zonal_cols = ",".join(NON_ZONAL_FEATURES)
    nz = con.sql(f"""
        WITH latest AS (
            SELECT *, row_number() OVER (
                PARTITION BY str_player_id, str_season ORDER BY dt_date DESC, str_match_id DESC) rn
            FROM gold.joueur_saison
            WHERE str_season IN ({','.join(['?']*len(seasons))})
        )
        SELECT str_player_id, str_season, int_n_apps_lag, int_minutes_lag, {non_zonal_cols}
        FROM latest WHERE rn = 1
    """, params=seasons).df()

    zonal_cols = ",".join(ZONAL_FEATURES)
    zt = con.sql(f"""
        SELECT str_player_id, str_season, str_zone_5x5, {zonal_cols}
        FROM gold.joueur_zone_saison
        WHERE str_season IN ({','.join(['?']*len(seasons))})
    """, params=seasons).df()

    if zt.empty:
        zw = pd.DataFrame(columns=["str_player_id", "str_season"])
    else:
        zw = zt.pivot(index=["str_player_id", "str_season"], columns="str_zone_5x5", values=ZONAL_FEATURES)
        zw.columns = [f"{feat.replace('_by_zone_lag', '')}_{cell}" for feat, cell in zw.columns]
        zw = zw.reset_index()

    df = nz.merge(zw, on=["str_player_id", "str_season"], how="left")
    if min_apps > 0:
        df = df[df["int_n_apps_lag"] >= min_apps].reset_index(drop=True)

    context_cols = ["str_player_id", "str_season", "int_n_apps_lag", "int_minutes_lag"]
    feature_cols = [c for c in df.columns if c not in context_cols]

    X = df[feature_cols].fillna(0.0).values.astype("float32")
    return df[context_cols], X, feature_cols


class AutoEncoder(nn.Module):
    def __init__(self, input_dim, hidden=[128, 64], bottleneck=32, dropout=0.3):
        super().__init__()
        layers, prev = [], input_dim
        for h in hidden:
            layers += [nn.Linear(prev, h), nn.LeakyReLU(), nn.Dropout(dropout)]
            prev = h
        layers += [nn.Linear(prev, bottleneck)]
        self.encoder = nn.Sequential(*layers)

        layers, prev = [], bottleneck
        for h in reversed(hidden):
            layers += [nn.Linear(prev, h), nn.LeakyReLU()]
            prev = h
        layers += [nn.Linear(prev, input_dim)]
        self.decoder = nn.Sequential(*layers)

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z), z


def train_autoencoder(X_train, X_val, cfg):
    model = AutoEncoder(
        input_dim=X_train.shape[1],
        hidden=cfg["hidden"],
        bottleneck=cfg["bottleneck"],
        dropout=cfg["dropout"],
    )
    opt = torch.optim.Adam(model.parameters(), lr=cfg["lr"], weight_decay=cfg["weight_decay"])
    loss_fn = nn.MSELoss()

    Xt = torch.tensor(X_train, dtype=torch.float32)
    Xv = torch.tensor(X_val, dtype=torch.float32)
    loader = torch.utils.data.DataLoader(
        torch.utils.data.TensorDataset(Xt),
        batch_size=cfg["batch_size"], shuffle=True
    )

    best_val, best_state, patience_left = float("inf"), None, cfg["patience"]
    history = []

    print(f"Début de l'entraînement PyTorch (max {cfg['max_epochs']} époques, patience {cfg['patience']})...")

    for epoch in range(cfg["max_epochs"]):
        model.train()
        losses = []
        for (xb,) in loader:
            opt.zero_grad()
            xh, _ = model(xb)
            loss = loss_fn(xh, xb)
            loss.backward()
            opt.step()
            losses.append(loss.item())
        train_loss = float(np.mean(losses))

        model.eval()
        with torch.no_grad():
            xh_v, _ = model(Xv)
            val_loss = float(loss_fn(xh_v, Xv).item())
        history.append({"epoch": epoch, "train_loss": train_loss, "val_loss": val_loss})

        # Affichage de la convergence toutes les 5 époques
        if (epoch + 1) % 5 == 0 or epoch == 0:
            print(f"  [Epoch {epoch+1:03d}/{cfg['max_epochs']}] Train Loss: {train_loss:.6f} | Val Loss: {val_loss:.6f}")

        if val_loss < best_val - 1e-6:
            best_val = val_loss
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
            patience_left = cfg["patience"]
        else:
            patience_left -= 1
            if patience_left <= 0:
                print(f"  Early stopping déclenché à l'époque {epoch+1}.")
                break

    model.load_state_dict(best_state)
    print(f"-> Entraînement terminé | Meilleure Val Loss: {best_val:.6f}\n")
    return model, {"best_val_loss": best_val, "epochs_run": epoch + 1, "history": history}

def encode_profiles(model, scaler, X):
    model.eval()
    with torch.no_grad():
        Xs = scaler.transform(X)
        z = model.encoder(torch.tensor(Xs, dtype=torch.float32)).numpy()
    return z


def write_embeddings(con, df, table="machine_learning.player_embedding_lag"):
    con.register("tmp_emb", df)
    con.execute("CREATE SCHEMA IF NOT EXISTS machine_learning")
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM tmp_emb")
    con.unregister("tmp_emb")
    print(f"Écrit : {table} ({len(df):,} lignes)")


def validate_embedding(con, table="machine_learning.player_embedding_lag"):
    df = con.sql(f"""
        SELECT e.*, r.str_role_fin_lag AS str_role
        FROM {table} e
        LEFT JOIN intermediate.int_player_role_lag r
          ON r.str_player_id = e.str_player_id AND r.str_season = e.str_season
        WHERE r.str_role_fin_lag IS NOT NULL
    """).df()
    emb_cols = [c for c in df.columns if c.startswith("dec_emb_")]
    global_var = float(df[emb_cols].var().mean())
    intra_var = float(df.groupby("str_role")[emb_cols].var().mean().mean())
    inter_var = float(df.groupby("str_role")[emb_cols].mean().var().mean())
    ratio = inter_var / intra_var if intra_var > 0 else float("nan")
    print(f"\n=== Contrôle qualité embedding (par role_fin_lag) ===")
    print(f"Variance globale        : {global_var:.4f}")
    print(f"Variance intra-classe   : {intra_var:.4f}")
    print(f"Variance inter-classe   : {inter_var:.4f}")
    print(f"Ratio inter/intra       : {ratio:.2f}  (>= 1 attendu)")
    return {"global_var": global_var, "intra_var": intra_var, "inter_var": inter_var, "ratio": ratio}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Écrit en base DuckDB")
    parser.add_argument("--validate", action="store_true", help="Exécute le contrôle qualité")
    parser.add_argument(
        "--seasons", 
        type=str, 
        default=None, 
        help="Saisons cibles filtrées séparées par des virgules (ex: 2023-2024,2024-2025)"
    )
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))
    all_seasons = sorted([r[0] for r in con.sql("SELECT DISTINCT str_season FROM gold.joueur_saison").fetchall()])
    
    # Filtrage éventuel via --seasons
    if args.seasons:
        selected = [s.strip() for s in args.seasons.split(",")]
        # On garde les saisons cibles demandées qui existent dans la base
        target_seasons = [s for s in all_seasons if s in selected]
    else:
        # Par défaut, toutes les saisons à partir de la 2ème
        target_seasons = all_seasons[1:]

    all_dfs = []

    for target_season in target_seasons:
        i = all_seasons.index(target_season)
        if i == 0:
            print(f"Saison {target_season} ignorée (première saison du dataset, pas d'historique <= N-1).")
            continue

        source_seasons_train = all_seasons[:i]
        source_season_infer = shift_season(target_season, -1)

        print(f"\n--- Traitement Saison Cible : {target_season} (Train sur <= {source_season_infer}) ---")

        # 1. Train set (Fiables uniquement, saisons <= N-1)
        _, X_raw, feature_cols = build_features_matrix(con, source_seasons_train, min_apps=MIN_APPS)
        if len(X_raw) == 0:
            print(f"Pas de données d'entraînement pour {target_season}.")
            continue

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_raw)

        X_tr, X_val = train_test_split(X_scaled, test_size=0.15, random_state=42)

        # 2. Fit AutoEncoder
        with mlflow.start_run(run_name=f"embedding_lag_{target_season}"):
            model, metrics = train_autoencoder(X_tr, X_val, AE_CFG)
            
            mlflow.log_params(AE_CFG)
            mlflow.log_params({"target_season": target_season, "n_train": len(X_tr)})
            mlflow.log_metrics({"best_val_loss": metrics["best_val_loss"], "epochs_run": metrics["epochs_run"]})

        # 3. Inférence pour target_season (Tous joueurs sur profil N-1)
        df_ids_infer, X_infer_raw, _ = build_features_matrix(con, source_season_infer, min_apps=0)
        embeddings = encode_profiles(model, scaler, X_infer_raw)

        emb_df = pd.DataFrame(embeddings, columns=[f"dec_emb_{k}" for k in range(AE_CFG["bottleneck"])])
        df_res = pd.concat([df_ids_infer, emb_df], axis=1)
        df_res["str_season"] = target_season
        df_res["str_source_season"] = source_season_infer
        
        all_dfs.append(df_res)

    if all_dfs:
        df_final = pd.concat(all_dfs, ignore_index=True)
        if args.write:
            write_embeddings(con, df_final)
        if args.validate and args.write:
            validate_embedding(con)

    con.close()


if __name__ == "__main__":
    main()