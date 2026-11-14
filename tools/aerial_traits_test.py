"""Duels aériens : types par traits contre z-scores continus, et interaction attaquant x défenseur par type.

Compare, sur les mêmes duels (attaquant et défenseur typés), LightGBM additif (contraintes d'interaction entre les deux
joueurs) avec : contexte seul, z-scores continus, types des 5 familles, types aérien + sacrifice, z + types, et le modèle
libre sur les types (écart libre - additif = interaction). Profils et types de la saison s-1. Un pli par appel :
python tools/aerial_traits_test.py ANNEE_DE_TEST (2019 à 2025), sorties dans SCRATCH_DIR/tt ; bilan : tools/agg_traits_test.py.
Entrées dans SCRATCH_DIR (défaut ~/scratch) : aerial_20*.parquet (duels), behaviour_all.parquet, trait_types.parquet,
aerial_profile.parquet (tailles).
"""
import sys, os, numpy as np, pandas as pd, duckdb, lightgbm as lgb
H=os.environ.get('SCRATCH_DIR', os.environ['HOME']+'/scratch'); os.makedirs(H+'/tt', exist_ok=True); ty=int(sys.argv[1]); out=f'{H}/tt/aerial_traits_{ty}.parquet'
FAM=['aerien','sacrifice','duel_sol','creation','distribution']
ZA=['aerial_win_rate','aerial_duels','a_woe_net_off','a_head_shots_per_match','a_duels_per_match','shots','clearances','touches']
ZD=['aerial_win_rate','aerial_duels','a_woe_net_def','a_duels_per_match','clearances','interceptions','tackles']
CTX=['dec_x','dec_y','ball_cross','ball_corner','ball_freekick','ball_longball','ball_headpass','ball_goalkick','ball_throwin']
c=duckdb.connect()
lag="CAST(CAST(SUBSTR(d.str_season,1,4) AS INTEGER)-1 AS VARCHAR)||'-'||SUBSTR(d.str_season,1,4)"
c.execute(f"create table tt as select * from read_parquet('{H}/trait_types.parquet')")
c.execute(f"create table bh as select * from read_parquet('{H}/behaviour_all.parquet')")
c.execute(f"create table hh as select str_player_id, max(int_height) h from read_parquet('{H}/aerial_profile.parquet') group by 1")
tcols=lambda p,t: ', '.join(f"{p}.int_type_{f} AS {t}_t_{f}" for f in FAM)
z=lambda p,t,names: ', '.join(f"{p}.dec_z_{n} AS {t}_{n}" for n in names)
ctx=', '.join(f"CAST(d.bool_{x} AS INTEGER) AS {x}" if x.startswith('ball_') else f"d.{x}" for x in CTX)
sql=f"""select d.str_match_id,d.str_season,d.str_league_source,CAST(d.bool_is_won AS INTEGER) won,{ctx},
 {tcols('ta','a')},{tcols('tf','f')},{z('ba','a',ZA)},{z('bf','f',ZD)},ha.h a_height,hf.h f_height
from read_parquet('{H}/aerial_20*.parquet') d
join tt ta on ta.str_player_id=d.str_player_id and ta.str_season={lag}
join tt tf on tf.str_player_id=d.str_opp_player_id and tf.str_season={lag}
join bh ba on ba.str_player_id=d.str_player_id and ba.str_season={lag}
join bh bf on bf.str_player_id=d.str_opp_player_id and bf.str_season={lag}
left join hh ha on ha.str_player_id=d.str_player_id left join hh hf on hf.str_player_id=d.str_opp_player_id
where d.str_role='offensif' and d.str_season>='2016-2017'"""
df=c.sql(sql).df(); df['y']=df.str_season.str[:4].astype(int)
tr=df[df.y<ty]; te=df[(df.y==ty)&df.str_league_source.isin(['Premier League','La Liga','Serie A','Bundesliga','Ligue 1'])].copy()
if len(tr)>500000: tr=tr.sample(500000,random_state=0)
def fit(cols,cons=None,cat=None):
    p=dict(n_estimators=400,learning_rate=0.03,num_leaves=15,min_child_samples=200,subsample=0.8,subsample_freq=1,colsample_bytree=0.8,reg_lambda=5.0,random_state=0,verbose=-1,n_jobs=2)
    if cons is not None: p['interaction_constraints']=cons
    X=tr[cols].copy(); Xt=te[cols].copy()
    if cat:
        for k in cat: X[k]=X[k].astype('category'); Xt[k]=pd.Categorical(Xt[k],categories=X[k].cat.categories)
    return lgb.LGBMClassifier(**p).fit(X,tr.won).predict_proba(Xt)[:,1]
ctxc=CTX
aT=[f'a_t_{f}' for f in FAM]; fT=[f'f_t_{f}' for f in FAM]; aZ=[f'a_{n}' for n in ZA]; fZ=[f'f_{n}' for n in ZD]
def cons(cols,a,f,x): i={k:j for j,k in enumerate(cols)}; return [[i[k] for k in a+x],[i[k] for k in f+x]]
te['p_ctx']=fit(ctxc)
cols=aZ+['a_height']+fZ+['f_height']+ctxc; te['p_z_add']=fit(cols,cons(cols,aZ+['a_height'],fZ+['f_height'],ctxc))
cols=aT+['a_height']+fT+['f_height']+ctxc; cat=aT+fT
te['p_t_add']=fit(cols,cons(cols,aT+['a_height'],fT+['f_height'],ctxc),cat)
te['p_t_free']=fit(cols,None,cat)
aA=['a_t_aerien','a_t_sacrifice']; fA=['f_t_aerien','f_t_sacrifice']
cols=aA+['a_height']+fA+['f_height']+ctxc; te['p_tA_add']=fit(cols,cons(cols,aA+['a_height'],fA+['f_height'],ctxc),aA+fA)
te['p_tA_free']=fit(cols,None,aA+fA)
cols=aZ+aT+['a_height']+fZ+fT+['f_height']+ctxc; te['p_zt_add']=fit(cols,cons(cols,aZ+aT+['a_height'],fZ+fT+['f_height'],ctxc),aT+fT)
te.to_parquet(out); print(ty,len(tr),len(te))
