"""
Hunt for facts that are invisible on EVERY single-variable map:
 (1) structure among 'unremarkable everywhere' buildings (overall medium AND
     every category in [0.3,0.7]) -> if a separable cluster exists there, no
     single-variable or overall map could ever flag it.
 (2) correlation-breaking: pairs strongly +correlated city-wide but inverted in
     a sizeable subgroup -> a trade-off invisible to single maps.
"""
import os,numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import umap
HERE=os.path.dirname(__file__)
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS]);overall=d["overall"];coords=d["coords"]
lon,lat=coords[:,0],coords[:,1]
N=len(X)
med=np.median(overall)
zc=lambda M:np.apply_along_axis(lambda c:(c-c.mean())/(c.std() or 1),0,M)

# ---- (1) unremarkable-everywhere population ----
unre = (overall>med-0.12)&(overall<med+0.12)
for j in range(6): unre &= (X[:,j]>=0.30)&(X[:,j]<=0.70)
print(f"[1] 'unremarkable on every map' buildings: n={unre.sum()} ({100*unre.mean():.1f}%)")
Xu=X[unre]
if unre.sum()>300:
    emb=umap.UMAP(n_neighbors=15,min_dist=0.1,random_state=42).fit_transform(zc(Xu))
    best=None
    for k in [2,3,4]:
        lab=KMeans(k,n_init=10,random_state=42).fit(zc(Xu)).labels_
        s=silhouette_score(zc(Xu),lab)
        print(f"    k={k} silhouette(feature-space)={s:.3f}")
        if best is None or s>best[0]: best=(s,k,lab)
    s,k,lab=best
    print(f"    -> best k={k} (sil={s:.3f}); cluster profiles (means, all within map-invisible band):")
    print("    "+" ".join(f"{c[:5]:>5s}" for c in CATS)+"   n   geo-spread")
    for cl in range(k):
        m=lab==cl;prof=Xu[m].mean(0)
        sl=lon[unre][m];sa=lat[unre][m]
        dx=(sl-sl.mean())*np.cos(np.radians(sa.mean()))*111.32;dy=(sa-sa.mean())*110.57
        spread=np.sqrt(dx*dx+dy*dy).mean()
        print(f"  c{cl} "+" ".join(f"{p:5.2f}" for p in prof)+f"  {m.sum():4d}  {spread:4.1f}km")

# ---- (2) correlation breaking ----
print("\n[2] citywide correlations vs subgroup inversion")
C=np.corrcoef(X.T)
pairs=[(i,j) for i in range(6) for j in range(i+1,6)]
for i,j in pairs:
    r=C[i,j]
    if r<0.5: continue
    # subgroup where one is high and the other low (violates +corr)
    hi_i = (X[:,i]>np.quantile(X[:,i],0.6)) & (X[:,j]<np.quantile(X[:,j],0.4))
    hi_j = (X[:,j]>np.quantile(X[:,j],0.6)) & (X[:,i]<np.quantile(X[:,i],0.4))
    viol = hi_i|hi_j
    # how 'map-visible' are violators? fraction with BOTH vars mid-range
    midboth = viol & (X[:,i]>0.3)&(X[:,i]<0.7)&(X[:,j]>0.3)&(X[:,j]<0.7)
    print(f"  {CATS[i]:16s}~{CATS[j]:16s} r={r:.2f}  violators={viol.sum():5d}  (mid-range-both={midboth.sum()})")
