"""Bilan des tests par types : python tools/agg_traits_test.py [aerial|duo]. Lit SCRATCH_DIR/tt."""
import glob, os, sys
import numpy as np
import pandas as pd
os.chdir(os.environ.get("SCRATCH_DIR", os.environ["HOME"] + "/scratch"))
mode = sys.argv[1] if len(sys.argv) > 1 else "aerial"
if mode == "aerial":
    r=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob('tt/aerial_traits_*.parquet'))]); r['y']=r.str_season.str[:4].astype(int)
    ll=lambda p:-(r.won*np.log(np.clip(p,1e-6,1-1e-6))+(1-r.won)*np.log(1-np.clip(p,1e-6,1-1e-6)))
    P=[c for c in r.columns if c.startswith('p_')]
    for c in P: r['ll_'+c[2:]]=ll(r[c])
    print(len(r),' | '.join(f"{c[3:]} {r[c].mean():.5f}" for c in r.columns if c.startswith('ll_')))
    def cmp(nm,a,b):
        g=(r['ll_'+a]-r['ll_'+b]); gm=g.groupby(r.str_match_id).sum().values; rng=np.random.default_rng(0)
        bs=[gm[rng.integers(0,len(gm),len(gm))].sum()/len(r) for _ in range(1000)]; by=g.groupby(r.y).mean()
        print(f"{nm:55s} {g.mean():+.5f} IC95 [{np.percentile(bs,2.5):+.5f};{np.percentile(bs,97.5):+.5f}] saisons {(by>0).sum()}/{len(by)}")
    cmp('types additifs vs contexte','ctx','t_add'); cmp('z continus additifs vs contexte','ctx','z_add')
    cmp('z continus vs types (z mieux si >0)','t_add','z_add')
    cmp('z + types vs z seuls','z_add','zt_add')
    cmp('INTERACTION types (libre vs additif)','t_add','t_free')
    cmp('INTERACTION types aérien+sacrifice','tA_add','tA_free')
    cmp('types aérien+sacrifice vs tous types (additif)','t_add','tA_add')
else:
    for tg in ['xt','shots','xg']:
        r=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f'tt/duo_types_{tg}_*.parquet'))])
        var=((r.y_true-r.y_true.mean())**2).mean(); se={k:(r.y_true-r['p_'+k])**2 for k in ('base','add','Aopp','Ateam','free')}
        print(f"[{tg}] {len(r):,} | R2 "+' '.join(f"{k} {1-v.mean()/var:.4f}" for k,v in se.items()))
        for nm,a,b in (('équipe+adversaires (types) vs A seul','base','add'),('A x adversaires','add','Aopp'),('A x coéquipiers (duo)','add','Ateam'),('tout libre','add','free')):
            g=se[a]-se[b]; per=g.groupby(r.str_match_id).sum().values; rng=np.random.default_rng(0)
            bs=np.array([per[rng.integers(0,len(per),len(per))].sum()/len(r) for _ in range(1000)]); by=g.groupby(r.y).mean(); m=se[a].mean()
            print(f"  {nm:40s} {g.mean()/m*100:+.2f}% IC95 [{np.percentile(bs,2.5)/m*100:+.2f};{np.percentile(bs,97.5)/m*100:+.2f}] saisons {(by>0).sum()}/{len(by)}")
