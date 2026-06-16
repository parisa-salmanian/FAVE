"""
Find (selection, feature) pairs where the selection MEAN ~ city mean (|d| small)
but KS distance is large => same map color, different distribution.
Selections tested: RegSO districts (natural map units) and UMAP/KMeans regimes.
Also report per-selection 'internal bimodality' of overall fairness.
"""
import os,json,numpy as np
from sklearn.cluster import KMeans
from matplotlib.path import Path
HERE=os.path.dirname(__file__);ROOT=os.path.join(HERE,"..","..")
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS]);overall=d["overall"];coords=d["coords"]
allf=CATS+["overall"]
M=np.column_stack([X,overall])
def ks(a,b):
    a=np.sort(a);b=np.sort(b);i=j=0;ca=cb=0;dm=0
    while i<len(a) and j<len(b):
        if a[i]<=b[j]:i+=1;ca=i/len(a)
        else:j+=1;cb=j/len(b)
        dm=max(dm,abs(ca-cb))
    return dm
# bimodality coefficient (Sarle): (skew^2+1)/kurt ; >0.555 suggests bimodal/flat
def bimod(x):
    x=x[np.isfinite(x)];n=len(x)
    if n<20:return np.nan
    m=x.mean();s=x.std()
    if s==0:return np.nan
    g1=((x-m)**3).mean()/s**3
    g2=((x-m)**4).mean()/s**4-3
    return (g1**2+1)/(g2+3*(n-1)**2/((n-2)*(n-3)))

# build district selections
g=json.load(open(os.path.join(ROOT,"frontend/assets/data/Vaxjo_regso.geojson")))
dist_paths={}
for f in g["features"]:
    nm=f["properties"].get("regsonamn");geom=f["geometry"]
    rings=[geom["coordinates"][0]] if geom["type"]=="Polygon" else [r[0] for r in geom["coordinates"]]
    dist_paths.setdefault(nm,[]).extend(Path(np.array(r)) for r in rings)
# assign each building to a district (subsample buildings for speed)
N=len(coords)
sub=np.random.RandomState(0).choice(N,20000,replace=False)
assign={}
for i in sub:
    pt=coords[i]
    for nm,ps in dist_paths.items():
        if any(p.contains_point(pt) for p in ps):assign.setdefault(nm,[]).append(i);break

print("=== Districts: features where |d|<0.15 (same avg color) but KS>0.30 (different distribution) ===")
cityM=M.mean(0);cityS=M.std(0)
hits=[]
for nm,ids in assign.items():
    if len(ids)<150:continue
    ids=np.array(ids)
    for j,fn in enumerate(allf):
        se=M[ids,j];dd=(se.mean()-cityM[j])/(cityS[j] or 1);k=ks(M[:,j],se)
        if abs(dd)<0.15 and k>0.30:
            hits.append((nm,fn,len(ids),dd,k,bimod(se)))
for nm,fn,n,dd,k,bm in sorted(hits,key=lambda r:-r[4])[:12]:
    print(f"  {nm:24s} {fn:18s} n={n:4d} d={dd:+.2f} KS={k:.2f} bimod={bm:.2f}")

print("\n=== Per district: overall fairness mean vs internal bimodality ===")
print("  (high bimod = district mixes very-fair & very-unfair buildings -> macro color misleads)")
for nm,ids in sorted(assign.items(),key=lambda kv:-len(kv[1])):
    if len(ids)<150:continue
    ids=np.array(ids);ov=overall[ids]
    print(f"  {nm:24s} n={len(ids):4d} overallμ={ov.mean():.2f} median={np.median(ov):.2f} "
          f"std={ov.std():.2f} bimod={bimod(ov):.2f}")
