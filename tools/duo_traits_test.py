"""Duos et face-à-face d'attaquants avec les TYPES par traits (au lieu des z-scores) : voir tools/duo_matchup_test.py.

Variables : types de l'attaquant (5 familles) ; part de chaque type parmi ses coéquipiers du onze de départ (hors lui) ;
part de chaque type parmi les défenseurs adverses (FB + CB, familles aérien, sacrifice, duel au sol). Mêmes variantes
(base, add, Aopp, Ateam, free), mêmes cibles (xt, shots, xg). Un pli par appel :
python tools/duo_traits_test.py CIBLE ANNEE_DE_TEST ; bilan : tools/agg_traits_test.py. Lit la base DuckDB du dépôt (lecture
seule) et SCRATCH_DIR (défaut ~/scratch) : behaviour_all.parquet, trait_types.parquet, passes_20*.parquet.
"""
import sys, os, importlib.util, numpy as np, pandas as pd, duckdb, lightgbm as lgb
from pathlib import Path
ROOT=str(Path(__file__).resolve().parents[1]); S=os.environ.get('SCRATCH_DIR', os.environ['HOME']+'/scratch'); os.makedirs(S+'/tt', exist_ok=True)
spec=importlib.util.spec_from_file_location('duo',ROOT+'/tools/duo_matchup_test.py'); duo=importlib.util.module_from_spec(spec); spec.loader.exec_module(duo)
FAM=['aerien','sacrifice','duel_sol','creation','distribution']
cache=S+'/duo_types_table.parquet'
if not os.path.exists(cache):
    con=duckdb.connect(ROOT+'/db/football.duckdb',read_only=True)
    sql=f"""
    WITH mc AS (SELECT str_match_id,str_team_id,str_opponent_id,int_is_home,str_league_source,CAST(SUBSTR(str_season,1,4) AS INTEGER) y FROM gold.gold_team_match),
    xi AS (SELECT DISTINCT l.str_match_id,l.str_team_id,l.str_player_id,m.str_opponent_id,m.int_is_home,m.str_league_source,m.y
           FROM intermediate.intermediate_whoscored_lineup_period l JOIN mc m ON m.str_match_id=l.str_match_id AND m.str_team_id=l.str_team_id
           WHERE l.int_start_minute=0 AND l.str_match_id IS NOT NULL AND m.y>=2016),
    prof AS (SELECT str_player_id,str_season,str_macro_role,CAST(SUBSTR(str_season,1,4) AS INTEGER) py,
             (dec_sec_def_right+dec_sec_mid_right+dec_sec_att_right-dec_sec_def_left-dec_sec_mid_left-dec_sec_att_left) lat
             FROM read_parquet('{S}/behaviour_all.parquet'))
    SELECT x.str_match_id,x.str_team_id,x.str_opponent_id,x.str_player_id,x.int_is_home,x.str_league_source,x.y,p.str_macro_role AS macro,
      CASE WHEN p.lat>0.10 THEN 'R' WHEN p.lat<-0.10 THEN 'L' ELSE 'C' END AS side,
      {', '.join(f't.int_type_{f} AS t_{f}' for f in FAM)},
      m.int_minutes_played mins, COALESCE(e.int_n_shots,0) int_n_shots, COALESCE(e.dec_xg_contribution,0) dec_xg_contribution,
      COALESCE(ps.dec_xt_created,0) dec_xt_created
    FROM xi x JOIN prof p ON p.str_player_id=x.str_player_id AND p.py=x.y-1
    LEFT JOIN read_parquet('{S}/trait_types.parquet') t ON t.str_player_id=p.str_player_id AND t.str_season=p.str_season
    LEFT JOIN intermediate.intermediate_player_match_minutes m ON m.str_match_id=x.str_match_id AND m.str_team_id=x.str_team_id AND m.str_player_id=x.str_player_id
    LEFT JOIN intermediate.intermediate_player_match_event_stats e ON e.str_match_id=x.str_match_id AND e.str_team_id=x.str_team_id AND e.str_player_id=x.str_player_id
    LEFT JOIN read_parquet('{S}/passes_20*.parquet') ps ON ps.str_match_id=x.str_match_id AND ps.str_team_id=x.str_team_id AND ps.str_player_id=x.str_player_id"""
    con.sql(sql).df().to_parquet(cache); con.close()
df=pd.read_parquet(cache)
key=['str_match_id','str_team_id']
def shares(sub,fams,prefix,keys):
    out=None
    for f in fams:
        for k in range(3):
            s=(sub[f't_{f}']==k).where(sub[f't_{f}'].notna()).groupby([sub[c] for c in keys]).mean().rename(f'{prefix}_{f}_{k}')
            out=s.to_frame() if out is None else out.join(s)
    return out.reset_index()
a=df[df.macro.isin(['WIDE','ST'])&(df.mins>=60)&df.t_aerien.notna()].copy()
out=a[key+['str_opponent_id','str_player_id','y','str_league_source','int_is_home','side','macro','mins','int_n_shots','dec_xg_contribution','dec_xt_created']+[f't_{f}' for f in FAM]].copy()
out=out.rename(columns={f't_{f}':f'A_t_{f}' for f in FAM})
# coéquipiers : onze moins l'attaquant lui-même, gardiens exclus (types absents)
TF=['aerien','sacrifice','duel_sol','creation','distribution']
oc=df[df.macro!='GK']
tm=shares(oc,TF,'team',key).merge(oc.groupby(key).size().rename('n_tm').reset_index(),on=key)
out=out.merge(tm,on=key,how='left')
# retire la contribution de l'attaquant : (part*n - 1[A de ce type]) / (n-1) sur ses propres types
n=out['n_tm'].values
for f in TF:
    for k in range(3):
        mine=(out[f'A_t_{f}']==k).astype(float).values
        out[f'team_{f}_{k}']=(out[f'team_{f}_{k}'].values*n-mine)/np.maximum(n-1,1)
# adversaires : défenseurs (FB+CB) de l'équipe en face
od=df[df.macro.isin(['FB','CB'])]
op=shares(od,['aerien','sacrifice','duel_sol'],'opp',key).rename(columns={'str_team_id':'str_opponent_id'})
out=out.merge(op,on=['str_match_id','str_opponent_id'],how='left')
out['side_c']=out.side.map({'L':0,'C':1,'R':2}); out['macro_c']=(out.macro=='ST').astype(int)
Acols=[f'A_t_{f}' for f in FAM]; Tcols=[c for c in out.columns if c.startswith('team_')]; Ocols=[c for c in out.columns if c.startswith('opp_')]
Ctx=['int_is_home','side_c','macro_c']
feats=Acols+Tcols+Ocols+Ctx; ix={c:i for i,c in enumerate(feats)}; I=lambda *g:[ix[c] for grp in g for c in grp]
V={'add':[I(Acols,Ctx),I(Tcols,Ctx),I(Ocols,Ctx)],'Aopp':[I(Acols,Ocols,Ctx),I(Tcols,Ctx)],'Ateam':[I(Acols,Tcols,Ctx),I(Ocols,Ctx)],'free':None}
tg=sys.argv[1]; ty=int(sys.argv[2]); part=f'{S}/tt/duo_types_{tg}_{ty}.parquet'
y=out[duo.TARGETS[tg]]*90/out.mins
tr=out[out.y<ty]; te=out[(out.y==ty)&out.str_league_source.isin(duo.BIG5)].copy(); te['y_true']=y.loc[te.index]
def fp(cols,cons=None):
    p=dict(n_estimators=300,learning_rate=0.03,num_leaves=15,min_child_samples=100,subsample=0.8,subsample_freq=1,colsample_bytree=0.8,reg_lambda=5.0,random_state=0,verbose=-1,n_jobs=2)
    if cons is not None: p['interaction_constraints']=cons
    X=tr[cols].copy(); Xt=te[cols].copy()
    for k in Acols:
        if k in cols: X[k]=X[k].astype('category'); Xt[k]=pd.Categorical(Xt[k],categories=X[k].cat.categories)
    return lgb.LGBMRegressor(**p).fit(X,y.loc[tr.index]).predict(Xt)
base=Acols+Ctx; te['p_base']=fp(base)
for k,c in V.items(): te['p_'+k]=fp(feats,c)
te[['str_match_id','str_team_id','str_player_id','y','y_true']+[c for c in te.columns if c.startswith('p_')]].to_parquet(part); print(tg,ty,len(tr),len(te))
