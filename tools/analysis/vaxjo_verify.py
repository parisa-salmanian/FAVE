import os, json, math, numpy as np

HERE=os.path.dirname(__file__)
ROOT=os.path.join(HERE,"..","..")
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
LABELS={"grocery":"Grocery fairness","hospital":"Hospital fairness",
        "healthcare_center":"Healthcare center fairness","pharmacy":"Pharmacy fairness",
        "kindergarten":"Kindergarten fairness","school_primary":"Primary school fairness"}
X=np.column_stack([d[f"score_{c}"] for c in CATS])
overall=d["overall"]; coords=d["coords"]
emb=np.load(os.path.join(HERE,"vaxjo_umap.npy"))
lab=np.load(os.path.join(HERE,"vaxjo_km5.npy"))
SEL=0  # cluster of interest
sel=(lab==SEL)
print("selection cluster %d: n=%d (%.1f%% of city)"%(SEL,sel.sum(),100*sel.mean()))

# --- spatial: distance to the single hospital ---
hosp=json.load(open(os.path.join(ROOT,"frontend/assets/data/cities/vaxjo/pois/hospital.geojson")))
hlon,hlat=hosp["features"][0]["geometry"]["coordinates"]
def hav(lon,lat):
    R=6371000.0;p1=math.radians(lat);p2=math.radians(hlat)
    dp=math.radians(hlat-lat);dl=math.radians(hlon-lon)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))
dh=np.array([hav(lo,la) for lo,la in coords])
print("dist-to-hospital km: selection median=%.2f city median=%.2f"%(np.median(dh[sel])/1000,np.median(dh)/1000))
print("centroid selection lon/lat=%.4f,%.4f  city=%.4f,%.4f"%(coords[sel,0].mean(),coords[sel,1].mean(),coords[:,0].mean(),coords[:,1].mean()))

# --- UMAP separability: silhouette of this cluster vs rest in embedding ---
from sklearn.metrics import silhouette_score
sil=silhouette_score(emb,(sel).astype(int))
# bbox spread of selection in embedding
print("UMAP emb: selection bbox x[%.1f,%.1f] y[%.1f,%.1f]; binary silhouette=%.3f"%(
    emb[sel,0].min(),emb[sel,0].max(),emb[sel,1].min(),emb[sel,1].max(),sil))

# ===== EBM: selection vs background (same interpret lib the app calls) =====
# Mirrors api/ebm_service.run_ebm_ranking: downsample 2000/class, interactions=0
from interpret.glassbox import ExplainableBoostingClassifier
import pandas as pd
rng=np.random.RandomState(42)
y=sel.astype(int)
idx_pos=np.where(y==1)[0]; idx_neg=np.where(y==0)[0]
if len(idx_pos)>2000: idx_pos=rng.choice(idx_pos,2000,replace=False)
if len(idx_neg)>2000: idx_neg=rng.choice(idx_neg,2000,replace=False)
ii=np.concatenate([idx_pos,idx_neg])
feat_names=[LABELS[c] for c in CATS]
df=pd.DataFrame(X[ii],columns=feat_names)
ebm=ExplainableBoostingClassifier(random_state=42,interactions=0)
ebm.fit(df,y[ii])
imp=list(zip(ebm.term_names_,ebm.term_importances()))
imp=[(n,abs(s)) for n,s in imp]
imp.sort(key=lambda t:t[1],reverse=True)
print("\n=== EBM ranked importance (selection vs city background) ===")
for n,s in imp: print(f"  {n:30s} {s:.3f}")

# ===== Contrastive distribution: EXACT formula from drView.js =====
def ksdist(a,b):
    a=np.sort(a);b=np.sort(b);i=j=0;ca=cb=0;dmax=0
    while i<len(a) and j<len(b):
        if a[i]<=b[j]: i+=1;ca=i/len(a)
        else: j+=1;cb=j/len(b)
        dmax=max(dmax,abs(ca-cb))
    return dmax
print("\n=== Contrastive (effect d = (meanSel-meanCity)/stdCity ; KS) ===")
print("feature                         medSel medCity   d     KS   dir")
rows=[]
metrics={**{f"fair{i}":X[:,j] for i,j in []}}  # noop
city_overall=overall
for j,c in enumerate(CATS+["overall"]):
    arr = overall if c=="overall" else X[:,j]
    cityv=arr; selv=arr[sel]
    mc=cityv.mean();ms=selv.mean();sc=cityv.std()
    eff=(ms-mc)/sc if sc>0 else 0
    ks=ksdist(cityv,selv)
    lab_=("Overall fairness (0-1)" if c=="overall" else LABELS[c])
    rows.append((lab_,np.median(selv),np.median(cityv),eff,ks))
rows.sort(key=lambda r:abs(r[3]),reverse=True)
for lab_,ms,mc,eff,ks in rows:
    print(f"{lab_:30s} {ms:6.2f} {mc:6.2f} {eff:6.2f} {ks:6.2f}  {'higher' if eff>0 else 'lower'}")
