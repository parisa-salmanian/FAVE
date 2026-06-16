"""
MACRO scale: district-level mean profiles. Find district pairs with nearly EQUAL
overall fairness (same macro map color) but DIVERGENT 6-service signatures
(opposite bottlenecks) -> only DR/EBM/contrastive can separate them.
"""
import os,json,numpy as np
from matplotlib.path import Path
HERE=os.path.dirname(__file__);ROOT=os.path.join(HERE,"..","..")
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS]);overall=d["overall"];coords=d["coords"]
g=json.load(open(os.path.join(ROOT,"frontend/assets/data/Vaxjo_regso.geojson")))
dist_paths={}
for f in g["features"]:
    nm=f["properties"].get("regsonamn");geom=f["geometry"]
    rings=[geom["coordinates"][0]] if geom["type"]=="Polygon" else [r[0] for r in geom["coordinates"]]
    dist_paths.setdefault(nm,[]).extend(Path(np.array(r)) for r in rings)
N=len(coords);sub=np.random.RandomState(0).choice(N,25000,replace=False)
members={}
for i in sub:
    pt=coords[i]
    for nm,ps in dist_paths.items():
        if any(p.contains_point(pt) for p in ps):members.setdefault(nm,[]).append(i);break
# district profile = MEAN per service (macro map shows pop-weighted overall; use mean here)
names=[];P=[];OV=[]
for nm,ids in members.items():
    if len(ids)<150:continue
    ids=np.array(ids);names.append(nm);P.append(X[ids].mean(0));OV.append(overall[ids].mean())
P=np.array(P);OV=np.array(OV);names=np.array(names)
print("District macro profiles (mean per service), sorted by overall:")
print(f"{'district':28s} {'over':>5s}  "+" ".join(f"{c[:5]:>5s}" for c in CATS))
for k in np.argsort(-OV):
    print(f"{names[k]:28s} {OV[k]:5.2f}  "+" ".join(f"{P[k,j]:5.2f}" for j in range(6)))

# find pairs: |overall diff|<=0.04 but profile L2 distance large
print("\n=== District pairs: ~equal overall (<=0.04) but most divergent profiles ===")
pairs=[]
for a in range(len(names)):
    for b in range(a+1,len(names)):
        if abs(OV[a]-OV[b])<=0.04:
            dist=np.linalg.norm(P[a]-P[b])
            pairs.append((dist,a,b))
for dist,a,b in sorted(pairs,reverse=True)[:6]:
    print(f"\n {names[a]} (overall {OV[a]:.2f})  vs  {names[b]} (overall {OV[b]:.2f})   profileL2={dist:.2f}")
    diff=P[a]-P[b]
    for j in np.argsort(-np.abs(diff)):
        print(f"     {CATS[j]:18s} {P[a,j]:.2f} vs {P[b,j]:.2f}   ({diff[j]:+.2f})")
