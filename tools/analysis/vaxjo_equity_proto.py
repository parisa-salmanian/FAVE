"""
Prototype: does adding income/gender-gap to the fairness feature set create
structure (supply-vs-need) that pure accessibility cannot?
Outputs: quadrant figure, contrastive of double-disadvantage districts,
and an access-only vs access+income nearest-neighbour test.
"""
import json,numpy as np
from matplotlib.path import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.neighbors import NearestNeighbors

d=np.load("tools/analysis/vaxjo_features.npz")
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS]);overall=d["overall"];coords=d["coords"]

inc=json.load(open("frontend/assets/data/gender-district-income-vaxjo.json"))
income={}
for row in inc["data"]:
    code=row["key"][0].split("_")[0];g=row["key"][2]
    income.setdefault(code,{})[g]=(float(row["values"][0]),int(row["values"][1]))

g=json.load(open("frontend/assets/data/Vaxjo_regso.geojson"))
nm=[];prof=[];ov=[];inctot=[];gap=[];pop=[]
for f in g["features"]:
    pr=f["properties"];code=str(pr.get("regsokod"));name=pr.get("regsonamn")
    geom=f["geometry"];rings=[geom["coordinates"][0]] if geom["type"]=="Polygon" else [r[0] for r in geom["coordinates"]]
    paths=[Path(np.array(r)) for r in rings]
    mask=np.array([any(p.contains_point(c) for p in paths) for c in coords])
    if mask.sum()<30:continue
    ic=income.get(code[:8]) or income.get(code)
    if not ic or '1+2' not in ic:continue
    nm.append(name);prof.append(X[mask].mean(0));ov.append(overall[mask].mean())
    inctot.append(ic['1+2'][0]);pop.append(ic['1+2'][1])
    gap.append(ic.get('1',(np.nan,0))[0]-ic.get('2',(np.nan,0))[0])
nm=np.array(nm);prof=np.array(prof);ov=np.array(ov);inctot=np.array(inctot);gap=np.array(gap);pop=np.array(pop)
N=len(nm);print("districts:",N)

z=lambda a:(a-a.mean())/a.std()
A=np.column_stack([z(prof[:,j]) for j in range(6)]+[z(ov)])          # access-only
AI=np.column_stack([A, z(inctot), z(gap)])                           # access + income + gap

# ---- test: nearest neighbour changes when income added ----
print("\nNearest-district shifts when income is added (does income re-group the right ones?):")
for target in ["Växjö öster","Teleborg"]:
    i=list(nm).index(target)
    for label,M in [("access-only",A),("access+income",AI)]:
        nn=NearestNeighbors(n_neighbors=2).fit(M);_,ind=nn.kneighbors(M[i:i+1])
        print(f"  {target:14s} [{label:13s}] nearest = {nm[ind[0,1]]}")

# ---- double-disadvantage contrastive (low access & low income vs rest) ----
omed=np.median(ov);imed=np.median(inctot)
dd=(ov<omed)&(inctot<imed)
print(f"\nDouble-disadvantage districts: {list(nm[dd])}")
def ks(a,b):
    a=np.sort(a);b=np.sort(b);i=j=0;ca=cb=0;dm=0
    while i<len(a) and j<len(b):
        if a[i]<=b[j]:i+=1;ca=i/len(a)
        else:j+=1;cb=j/len(b)
        dm=max(dm,abs(ca-cb))
    return dm
feats={"overall fairness":ov,**{f"{CATS[j]} fairness":prof[:,j] for j in range(6)},
       "income (tkr)":inctot,"gender income gap":gap}
print("\nContrastive: double-disadvantage vs rest (d = std mean diff):")
rows=[]
for k,v in feats.items():
    bg=v;se=v[dd];rows.append((k,(se.mean()-bg.mean())/bg.std(),ks(bg,se)))
for k,e,kk in sorted(rows,key=lambda r:-abs(r[1])):
    print(f"  {k:22s} d={e:+.2f} KS={kk:.2f} {'higher' if e>0 else 'lower'}")

# ---- figure: quadrant scatter ----
fig,ax=plt.subplots(figsize=(11,8))
col=np.where(dd,"#d62728",np.where((ov<omed)&(inctot>=imed),"#ff7f0e",
       np.where((ov>=omed)&(inctot<imed),"#2ca02c","#1f77b4")))
ax.scatter(ov,inctot,c=col,s=np.clip(pop/30,40,500),alpha=.85,edgecolor="k",linewidth=.5)
ax.axvline(omed,ls="--",c="gray");ax.axhline(imed,ls="--",c="gray")
for i in range(N):
    ax.annotate(nm[i],(ov[i],inctot[i]),fontsize=8,xytext=(4,4),textcoords="offset points")
ax.set_xlabel("Overall accessibility fairness  (low <-> high)")
ax.set_ylabel("Mean income (tkr)")
ax.set_title("Växjö districts: accessibility (supply) vs income (need)\n"
             "red = double disadvantage · green = well-served low-income · "
             "orange = access-poor affluent · blue = advantaged")
ax.text(0.02,0.02,f"corr(access,income) = {np.corrcoef(ov,inctot)[0,1]:+.2f}",
        transform=ax.transAxes,fontsize=9,style="italic")
fig.tight_layout();out="/Users/pasaaa/Downloads/vaxjo_equity_quadrants.png"
fig.savefig(out,dpi=130);print("\nsaved figure:",out)
