"""
City-general: compute 6-category gravity fairness, then test whether attribute
(accessibility-profile) space DECOUPLES from geography. If it does, UMAP/EBM/
contrastive can reveal structure the map cannot. If profile~geography, they can't.

Usage: python city_pipeline.py <city_key> [cat1,cat2,...]
"""
import os,sys,json,math,numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import PCA

ROOT=os.path.join(os.path.dirname(__file__),"..","..")
KAPPA={"grocery":1.2,"hospital":0.6,"pharmacy":1.0,"dentistry":1.0,"healthcare_center":0.8,
       "veterinary":0.8,"university":0.5,"kindergarten":1.1,"school_primary":0.9,"school_high":0.7}
RHO={"grocery":1.0,"hospital":1.4,"pharmacy":1.2,"dentistry":1.1,"healthcare_center":1.2,
     "veterinary":0.9,"university":0.8,"kindergarten":1.3,"school_primary":1.2,"school_high":1.1}

def data(city): return os.path.join(ROOT,f"frontend/assets/data/cities/{city}")
def load_bldg(city):
    d=json.load(open(os.path.join(data(city),"routing/walking/grocery.json")))
    return np.array([[float(x) for x in k.split(",")] for k in d["key"]])
def load_pois(city,cat):
    d=json.load(open(os.path.join(data(city),f"pois/{cat}.geojson")))
    pts=[]
    for f in d["features"]:
        g=f["geometry"]
        if g["type"]=="Point": pts.append(g["coordinates"])
        else:
            cs=np.array(g["coordinates"]).reshape(-1,2); pts.append([cs[:,0].mean(),cs[:,1].mean()])
    return np.array(pts)
def access(b,pois,cat):
    kappa=KAPPA[cat];rho=RHO[cat];out=np.empty(len(b));plon=pois[:,0];plat=pois[:,1]
    for i in range(len(b)):
        lon,lat=b[i];p1=math.radians(lat)
        a=np.sin(np.radians(plat-lat)/2)**2+math.cos(p1)*np.cos(np.radians(plat))*np.sin(np.radians(plon-lon)/2)**2
        dd=2*6371000.0*np.arcsin(np.sqrt(a));k=min(3,len(dd))
        nd=np.partition(dd,k-1)[:k]/1000.0
        out[i]=np.sum(rho*np.exp(-kappa*nd))
    return out
def norm(acc):
    v=np.log1p(acc);pos=v[acc>0]
    if pos.size==0:return np.zeros_like(acc)
    s=np.sort(pos)
    def q(p):
        i=(s.size-1)*p;lo=int(math.floor(i));hi=int(math.ceil(i))
        return s[lo] if lo==hi else s[lo]*(1-(i-lo))+s[hi]*(i-lo)
    floor=q(.10);span=max(1e-9,q(.95)-floor);sc=np.clip((v-floor)/span,0,1);sc[acc<=0]=0;return sc

def main():
    city=sys.argv[1]
    cats=sys.argv[2].split(",") if len(sys.argv)>2 else ["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
    b=load_bldg(city)
    CAP=40000
    if len(b)>CAP:
        sub=np.random.RandomState(1).choice(len(b),CAP,replace=False);b=b[sub]
        print(f"{city}: {len(b)} buildings (subsampled from larger); cats={cats}")
    else:
        print(f"{city}: {len(b)} buildings; cats={cats}")
    X=[]
    for c in cats:
        p=load_pois(city,c);ac=access(b,p,c);X.append(norm(ac))
        print(f"  {c:18s} POIs={len(p):4d} scoreμ={X[-1].mean():.2f}")
    X=np.column_stack(X);overall=norm(np.sum([access(b,load_pois(city,c),c) for c in cats],axis=0))
    np.savez(os.path.join(os.path.dirname(__file__),f"{city}_feat.npz"),coords=b,X=X,overall=overall,cats=np.array(cats))
    Xz=(X-X.mean(0))/X.std(0)
    # PCA structure
    pca=PCA().fit(Xz);ev=pca.explained_variance_ratio_
    sc=pca.transform(Xz)
    print(f"\nPCA var: {np.round(ev,3)}")
    print(f"corr(PC2,overall)={np.corrcoef(sc[:,1],overall)[0,1]:.2f}  corr(PC3,overall)={np.corrcoef(sc[:,2],overall)[0,1]:.2f}")
    # ---- DECOUPLING TEST ----
    # sample buildings; for each, profile-nearest neighbor's GEOGRAPHIC distance.
    # If profile-twins are geographically FAR, geography != profile -> DR is useful.
    rng=np.random.RandomState(0);idx=rng.choice(len(b),min(4000,len(b)),replace=False)
    nn=NearestNeighbors(n_neighbors=6).fit(Xz[idx])  # profile space
    _,pi=nn.kneighbors(Xz[idx])
    def geo_km(i,j):
        la=(b[idx[i],1]+b[idx[j],1])/2
        dx=(b[idx[i],0]-b[idx[j],0])*math.cos(math.radians(la))*111.32
        dy=(b[idx[i],1]-b[idx[j],1])*110.57
        return math.hypot(dx,dy)
    twin_geo=np.array([np.mean([geo_km(i,pi[i,k]) for k in range(1,6)]) for i in range(len(idx))])
    # baseline: geo distance to random buildings
    base=np.array([geo_km(i,rng.randint(len(idx))) for i in range(len(idx))])
    print(f"\nDECOUPLING: geographic km between PROFILE-twins: median={np.median(twin_geo):.2f} mean={twin_geo.mean():.2f}")
    print(f"            geographic km between RANDOM pairs:  median={np.median(base):.2f} mean={base.mean():.2f}")
    print(f"            ratio twin/random = {twin_geo.mean()/base.mean():.2f}  (near 1.0 => profile-twins are NOT local => DR useful)")
    far=(twin_geo> np.median(base)).mean()
    print(f"            fraction of buildings whose profile-twins are FARTHER than the median random pair = {far:.2f}")

if __name__=="__main__":
    main()
