"""
Look for patterns the MAP cannot show:
 (1) attribute-space regimes that are spatially dispersed
 (2) same overall-fairness band, different service profile (different bottleneck)
"""
import os, numpy as np
from sklearn.cluster import KMeans
HERE=os.path.dirname(__file__)
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS])
overall=d["overall"]; coords=d["coords"]
lon,lat=coords[:,0],coords[:,1]
N=len(X)

def zc(M):
    return np.apply_along_axis(lambda c:(c-c.mean())/(c.std() if c.std()>0 else 1),0,M)

# ---------- spatial dispersion of attribute clusters ----------
def dispersion(mask):
    # mean great-circle distance (km) of members to their own geo-centroid
    clo,cla=lon[mask].mean(),lat[mask].mean()
    dx=(lon[mask]-clo)*np.cos(np.radians(cla))*111.32
    dy=(lat[mask]-cla)*110.57
    r=np.sqrt(dx*dx+dy*dy)
    return r.mean(), np.median(r)

print("=== (1) Are attribute regimes spatially compact or dispersed? ===")
Xz=zc(X)
km=KMeans(n_clusters=5,n_init=10,random_state=42).fit(Xz)
for cl in range(5):
    m=km.labels_==cl
    mr,md=dispersion(m)
    prof=X[m].mean(0)
    print(f"clu{cl} n={m.sum():5d} overallμ={overall[m].mean():.2f} "
          f"geo-spread mean={mr:4.1f}km med={md:4.1f}km  "
          f"prof["+",".join(f"{p:.2f}" for p in prof)+"]")
# reference: whole-city spread
print("city geo-spread:", [round(x,1) for x in dispersion(np.ones(N,bool))],"km")

# ---------- (2) same overall band, different profile ----------
print("\n=== (2) Within a NARROW overall band, are there opposite-bottleneck regimes? ===")
for lo,hi in [(0.45,0.55),(0.55,0.65),(0.35,0.45)]:
    band=(overall>=lo)&(overall<hi)
    nb=band.sum()
    if nb<200:
        print(f"band [{lo},{hi}) n={nb} (skip)"); continue
    Xb=X[band]; Xbz=zc(Xb)
    k2=KMeans(n_clusters=2,n_init=10,random_state=42).fit(Xbz)
    print(f"\nband overall in [{lo},{hi})  n={nb}  (all SAME color on overall map, μ={overall[band].mean():.2f})")
    print("    "+ " ".join(f"{c[:5]:>5s}" for c in CATS)+"   n   overallμ  geo-spread")
    idxb=np.where(band)[0]
    for cl in range(2):
        mm=k2.labels_==cl
        prof=Xb[mm].mean(0)
        sub=idxb[mm]
        clo,cla=lon[sub].mean(),lat[sub].mean()
        dx=(lon[sub]-clo)*np.cos(np.radians(cla))*111.32; dy=(lat[sub]-cla)*110.57
        spread=np.sqrt(dx*dx+dy*dy).mean()
        print(f"  R{cl} "+" ".join(f"{p:5.2f}" for p in prof)+
              f"  {mm.sum():5d}  {overall[sub].mean():.2f}    {spread:4.1f}km  cen=({clo:.3f},{cla:.3f})")
    # which services flip between the two regimes
    p0=Xb[k2.labels_==0].mean(0); p1=Xb[k2.labels_==1].mean(0)
    diff=p1-p0
    order=np.argsort(-np.abs(diff))
    print("  biggest profile differences (R1-R0): "+
          ", ".join(f"{CATS[j]}:{diff[j]:+.2f}" for j in order[:4]))
