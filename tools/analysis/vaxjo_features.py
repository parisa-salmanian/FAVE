"""
Reproduce FAVE's per-building gravity (IF-City) fairness scores for Växjö,
walking mode, building (micro) level, for the 6 case-study categories.

Pipeline mirrors:
  - ifCityAccessibilityForBuilding()  (fairness.js): nearest-3 POIs,
        access_c = sum_{top3} rho_c * v_j(=1) * exp(-kappa_c * impedance)
  - ifCityDistanceForMode(km,'walking') = km   (detour=1, behavior=1, speed=5)
  - normalizeBenefitsToScores()  (fairness.js): log1p, floor=p10, span=p95-p10, clamp[0,1]

Constants taken verbatim from frontend/assets/js/lib/config.js.
Building coords: routing key list. POI coords: pois/<cat>.geojson.
"""
import json, math, os, numpy as np

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
DATA = os.path.join(ROOT, "frontend/assets/data/cities/vaxjo")

CATS = ["grocery", "hospital", "healthcare_center", "pharmacy",
        "kindergarten", "school_primary"]

# config.js  IF_CITY_KAPPA_BY_CAT / IF_CITY_PRIORITY_WEIGHTS
KAPPA = {"grocery":1.2,"hospital":0.6,"pharmacy":1.0,"dentistry":1.0,
         "healthcare_center":0.8,"veterinary":0.8,"university":0.5,
         "kindergarten":1.1,"school_primary":0.9,"school_high":0.7}
RHO   = {"grocery":1.0,"hospital":1.4,"pharmacy":1.2,"dentistry":1.1,
         "healthcare_center":1.2,"veterinary":0.9,"university":0.8,
         "kindergarten":1.3,"school_primary":1.2,"school_high":1.1}

def haversine_m(lon1,lat1,lon2,lat2):
    R=6371000.0
    p1=math.radians(lat1); p2=math.radians(lat2)
    dp=math.radians(lat2-lat1); dl=math.radians(lon2-lon1)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def load_building_coords():
    d=json.load(open(os.path.join(DATA,"routing/walking/grocery.json")))
    coords=[]
    for k in d["key"]:
        lon,lat=k.split(",")
        coords.append((float(lon),float(lat)))
    return np.array(coords)  # (N,2) lon,lat

def load_pois(cat):
    d=json.load(open(os.path.join(DATA,f"pois/{cat}.geojson")))
    pts=[]
    for f in d["features"]:
        g=f["geometry"]
        if g["type"]=="Point":
            pts.append(tuple(g["coordinates"]))
        else:
            # centroid fallback (none expected for these cats)
            cs=np.array(g["coordinates"]).reshape(-1,2)
            pts.append((cs[:,0].mean(),cs[:,1].mean()))
    return np.array(pts)

def access_for_cat(bcoords, pois, cat):
    kappa=KAPPA[cat]; rho=RHO[cat]
    N=len(bcoords); out=np.empty(N)
    plon=pois[:,0]; plat=pois[:,1]
    for i in range(N):
        lon,lat=bcoords[i]
        # vectorized haversine to all POIs
        p1=math.radians(lat)
        dp=np.radians(plat-lat); dl=np.radians(plon-lon)
        a=np.sin(dp/2)**2+math.cos(p1)*np.cos(np.radians(plat))*np.sin(dl/2)**2
        d=2*6371000.0*np.arcsin(np.sqrt(a))
        # nearest 3 (walking impedance = km)
        k=min(3,len(d))
        nd=np.partition(d,k-1)[:k]/1000.0
        out[i]=np.sum(rho*np.exp(-kappa*nd))
    return out

def normalize_scores(access):
    v=np.log1p(access)
    pos=v[access>0]
    if pos.size==0: return np.zeros_like(access)
    s=np.sort(pos)
    def q(p):
        if s.size==1: return s[0]
        idx=(s.size-1)*p; lo=int(math.floor(idx)); hi=int(math.ceil(idx))
        if lo==hi: return s[lo]
        t=idx-lo; return s[lo]*(1-t)+s[hi]*t
    floor=q(0.10); p95=q(0.95); span=max(1e-9,p95-floor)
    sc=(v-floor)/span
    sc=np.clip(sc,0,1)
    sc[access<=0]=0
    return sc

def main():
    b=load_building_coords()
    print("buildings:",len(b))
    access={}; score={}
    for c in CATS:
        p=load_pois(c)
        ac=access_for_cat(b,p,c)
        access[c]=ac; score[c]=normalize_scores(ac)
        print(f"{c:18s} POIs={len(p):3d}  score mean={score[c].mean():.3f} "
              f"median={np.median(score[c]):.3f} std={score[c].std():.3f}")
    # demand-free overall = normalized sum of access (equity=demand=1)
    util=np.sum([access[c] for c in CATS],axis=0)
    overall=normalize_scores(util)
    np.savez(os.path.join(os.path.dirname(__file__),"vaxjo_features.npz"),
             coords=b, overall=overall,
             **{f"score_{c}":score[c] for c in CATS},
             **{f"access_{c}":access[c] for c in CATS})
    print("overall (demand-free) mean=%.3f median=%.3f"%(overall.mean(),np.median(overall)))
    print("saved vaxjo_features.npz")

if __name__=="__main__":
    main()
