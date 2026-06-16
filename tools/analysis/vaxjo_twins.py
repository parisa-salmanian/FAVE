"""
Verify the 'same overall, opposite bottleneck' regimes within a narrow overall band.
EBM + exact contrastive + spatial interleaving + district names + KS-vs-d (shape) check.
"""
import os,json,numpy as np
from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from matplotlib.path import Path
HERE=os.path.dirname(__file__); ROOT=os.path.join(HERE,"..","..")
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
LAB={"grocery":"Grocery fairness","hospital":"Hospital fairness","healthcare_center":"Healthcare center fairness",
     "pharmacy":"Pharmacy fairness","kindergarten":"Kindergarten fairness","school_primary":"Primary school fairness"}
X=np.column_stack([d[f"score_{c}"] for c in CATS]); overall=d["overall"]; coords=d["coords"]
lon,lat=coords[:,0],coords[:,1]
LO,HI=0.55,0.65
band=(overall>=LO)&(overall<HI); idxb=np.where(band)[0]
Xb=X[band]
zc=lambda M:np.apply_along_axis(lambda c:(c-c.mean())/(c.std() or 1),0,M)
k2=KMeans(n_clusters=2,n_init=10,random_state=42).fit(zc(Xb)).labels_
# order so R_groc = grocery-rich regime
if Xb[k2==0,0].mean() > Xb[k2==1,0].mean(): k2=1-k2  # make regime1 = grocery-rich
R0=idxb[k2==0]; R1=idxb[k2==1]   # R0 kindergarten-rich, R1 grocery-rich
print(f"band overall in [{LO},{HI}): n={band.sum()}, overall mean R0={overall[R0].mean():.3f} R1={overall[R1].mean():.3f}")
print(f"R0 (kindergarten-rich) n={len(R0)} ; R1 (grocery-rich) n={len(R1)}")
print("profiles:")
print("    "+" ".join(f"{c[:5]:>6s}" for c in CATS))
print("R0  "+" ".join(f"{X[R0,j].mean():6.2f}" for j in range(6)))
print("R1  "+" ".join(f"{X[R1,j].mean():6.2f}" for j in range(6)))

# ---- districts ----
g=json.load(open(os.path.join(ROOT,"frontend/assets/data/Vaxjo_regso.geojson")))
paths=[]
for f in g["features"]:
    nm=f["properties"].get("regsonamn")
    geom=f["geometry"]
    rings=[geom["coordinates"][0]] if geom["type"]=="Polygon" else [r[0] for r in geom["coordinates"]]
    for ring in rings: paths.append((nm,Path(np.array(ring))))
from collections import Counter
def districts(ids,k=1500):
    sub=np.random.RandomState(0).choice(ids,min(k,len(ids)),replace=False);c=Counter()
    for i in sub:
        for nm,pa in paths:
            if pa.contains_point(coords[i]):c[nm]+=1;break
    return [(nm,round(100*v/len(sub))) for nm,v in c.most_common(4)]
print("\nR0 districts:",districts(R0))
print("R1 districts:",districts(R1))

# ---- spatial interleaving: of each building's 10 nearest neighbors (geo), how many are the OTHER regime? ----
both=np.concatenate([R0,R1]); ll=coords[both]
nn=NearestNeighbors(n_neighbors=11).fit(ll)
_,ind=nn.kneighbors(ll)
reg=np.array([0]*len(R0)+[1]*len(R1))
mix=[]
for a in range(len(both)):
    neigh=reg[ind[a,1:]]; mix.append((neigh!=reg[a]).mean())
mix=np.array(mix)
print(f"\nspatial interleaving: mean fraction of 10 geo-nearest neighbors in the OPPOSITE regime = {mix.mean():.2f}")
print(f"  (0=fully separated areas, 0.5=fully interleaved)  R0={mix[reg==0].mean():.2f} R1={mix[reg==1].mean():.2f}")

# ---- EBM R0 vs R1 ----
from interpret.glassbox import ExplainableBoostingClassifier
import pandas as pd
rng=np.random.RandomState(42)
def samp(a,n=2000): return a if len(a)<=n else rng.choice(a,n,replace=False)
ii=np.concatenate([samp(R0),samp(R1)]); y=np.array([0]*min(len(R0),2000)+[1]*min(len(R1),2000))
# rebuild y to match ii ordering
y=np.array([0]*len(samp(R0))+[1]*len(samp(R1)))
df=pd.DataFrame(X[ii],columns=[LAB[c] for c in CATS])
ebm=ExplainableBoostingClassifier(random_state=42,interactions=0).fit(df,y)
imp=sorted([(n,abs(s)) for n,s in zip(ebm.term_names_,ebm.term_importances())],key=lambda t:-t[1])
print("\nEBM (R0 vs R1) ranked importance:")
for n,s in imp: print(f"  {n:28s} {s:.3f}")

# ---- contrastive R1 vs R0-as-background (exact drView formula) + KS-vs-d shape check vs CITY ----
def ks(a,b):
    a=np.sort(a);b=np.sort(b);i=j=0;ca=cb=0;dm=0
    while i<len(a) and j<len(b):
        if a[i]<=b[j]:i+=1;ca=i/len(a)
        else:j+=1;cb=j/len(b)
        dm=max(dm,abs(ca-cb))
    return dm
print("\nContrastive R1(grocery-rich) vs R0(kindergarten-rich) background:")
print("feature                       medR1 medR0   d    KS")
rows=[]
for j,c in enumerate(CATS):
    bg=X[R0,j];se=X[R1,j];sd=bg.std()
    rows.append((LAB[c],np.median(se),np.median(bg),(se.mean()-bg.mean())/(sd or 1),ks(bg,se)))
for l,a,b,e,k in sorted(rows,key=lambda r:-abs(r[3])):
    print(f"{l:28s} {a:5.2f} {b:5.2f} {e:6.2f} {k:5.2f}  {'higher' if e>0 else 'lower'}")
