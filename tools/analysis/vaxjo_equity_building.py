"""
Building-level prototype: attach district income to each building, then test
whether access+income forms the supply-vs-need quadrants as lassoable clusters,
and whether 'double disadvantage' is separable ONLY when income is added.
"""
import json,numpy as np
from matplotlib.path import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
import umap

d=np.load("tools/analysis/vaxjo_features.npz")
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS]);overall=d["overall"];coords=d["coords"]
N=len(X)

# ---- income per district ----
inc=json.load(open("frontend/assets/data/gender-district-income-vaxjo.json"))
income={}
for row in inc["data"]:
    code=row["key"][0].split("_")[0];g=row["key"][2]
    income.setdefault(code,{})[g]=(float(row["values"][0]),int(row["values"][1]))

# ---- assign each building to a district (vectorized point-in-polygon) ----
g=json.load(open("frontend/assets/data/Vaxjo_regso.geojson"))
bld_inc=np.full(N,np.nan);bld_gap=np.full(N,np.nan)
assigned=np.zeros(N,bool)
for f in g["features"]:
    pr=f["properties"];code=str(pr.get("regsokod"))
    ic=income.get(code[:8]) or income.get(code)
    if not ic or '1+2' not in ic: continue
    geom=f["geometry"];rings=[geom["coordinates"][0]] if geom["type"]=="Polygon" else [r[0] for r in geom["coordinates"]]
    inside=np.zeros(N,bool)
    for r in rings: inside |= Path(np.array(r)).contains_points(coords)
    inside &= ~assigned
    bld_inc[inside]=ic['1+2'][0]
    bld_gap[inside]=ic.get('1',(np.nan,0))[0]-ic.get('2',(np.nan,0))[0]
    assigned|=inside
ok=assigned & np.isfinite(bld_inc)
print(f"buildings with income attached: {ok.sum()} / {N}")
Xa=X[ok];ova=overall[ok];inca=bld_inc[ok];gapa=bld_gap[ok];co=coords[ok]
z=lambda a:(a-a.mean())/a.std()

ACC=np.column_stack([z(Xa[:,j]) for j in range(6)])                 # access-only
ACCI=np.column_stack([ACC, z(inca), z(gapa)])                       # access + income + gap

# ---- double-disadvantage label (low access AND low income) ----
dd=(ova<np.median(ova))&(inca<np.median(inca))
print(f"double-disadvantage buildings: {dd.sum()} ({100*dd.mean():.0f}%)")

# ---- KEY TEST: is dd separable WITHOUT income vs WITH income? ----
rng=np.random.RandomState(0);samp=rng.choice(len(Xa),min(6000,len(Xa)),replace=False)
sil_acc =silhouette_score(ACC[samp], dd[samp].astype(int))
sil_acci=silhouette_score(ACCI[samp],dd[samp].astype(int))
print(f"\nseparability of 'double disadvantage' group:")
print(f"  access-only feature space   silhouette = {sil_acc:+.3f}")
print(f"  access+income feature space silhouette = {sil_acci:+.3f}   <- higher => income makes it a real cluster")

# ---- contrastive: dd vs rest (exact effect-size + KS) ----
def ks(a,b):
    a=np.sort(a);b=np.sort(b);i=j=0;ca=cb=0;dm=0
    while i<len(a) and j<len(b):
        if a[i]<=b[j]:i+=1;ca=i/len(a)
        else:j+=1;cb=j/len(b)
        dm=max(dm,abs(ca-cb))
    return dm
feats={"overall fairness":ova,**{f"{CATS[j]}":Xa[:,j] for j in range(6)},"income":inca,"gender gap":gapa}
print("\nContrastive (double-disadvantage vs rest):")
for k,e,kk in sorted([(k,(v[dd].mean()-v.mean())/v.std(),ks(v,v[dd])) for k,v in feats.items()],key=lambda r:-abs(r[1])):
    print(f"  {k:18s} d={e:+.2f} KS={kk:.2f} {'higher' if e>0 else 'lower'}")

# ---- UMAP figure: access+income, colored by quadrant ----
emb=umap.UMAP(n_neighbors=15,min_dist=0.1,random_state=42).fit_transform(ACCI[samp])
om=np.median(ova);im=np.median(inca)
q=np.where((ova<om)&(inca<im),0,np.where((ova<om)&(inca>=im),1,np.where((ova>=om)&(inca<im),2,3)))[samp]
cols=["#d62728","#ff7f0e","#2ca02c","#1f77b4"];labs=["double disadvantage","access-poor affluent","well-served low-income","advantaged"]
fig,ax=plt.subplots(1,2,figsize=(15,6))
for k in range(4):
    m=q==k;ax[0].scatter(emb[m,0],emb[m,1],s=6,c=cols[k],label=labs[k],alpha=.6)
ax[0].legend(markerscale=3,fontsize=8);ax[0].set_title("UMAP of ACCESS + INCOME (buildings)\ncolored by supply-vs-need quadrant")
# compare: UMAP access-only
emb2=umap.UMAP(n_neighbors=15,min_dist=0.1,random_state=42).fit_transform(ACC[samp])
for k in range(4):
    m=q==k;ax[1].scatter(emb2[m,0],emb2[m,1],s=6,c=cols[k],alpha=.6)
ax[1].set_title("UMAP of ACCESS ONLY (same points)\nquadrants NOT separable")
fig.tight_layout();out="/Users/pasaaa/Downloads/vaxjo_equity_umap.png";fig.savefig(out,dpi=130);print("\nsaved:",out)
