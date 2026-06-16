import os, numpy as np
import umap
from sklearn.cluster import KMeans
from scipy.stats import zscore

HERE=os.path.dirname(__file__)
d=np.load(os.path.join(HERE,"vaxjo_features.npz"))
CATS=["grocery","hospital","healthcare_center","pharmacy","kindergarten","school_primary"]
X=np.column_stack([d[f"score_{c}"] for c in CATS])
overall=d["overall"]; coords=d["coords"]
N=len(X)
print("N=",N)

print("\n=== Pearson correlation between category scores ===")
C=np.corrcoef(X.T)
hdr="           "+" ".join(f"{c[:6]:>7s}" for c in CATS)
print(hdr)
for i,c in enumerate(CATS):
    print(f"{c[:10]:10s} "+" ".join(f"{C[i,j]:7.2f}" for j in range(len(CATS))))

# z-score (matches collectDRData normalize=true)
Xz=np.apply_along_axis(lambda col: (col-col.mean())/ (col.std() if col.std()>0 else 1),0,X)

print("\n=== UMAP (n_neighbors=15, min_dist=0.1, metric=euclidean, seed=42) ===")
emb=umap.UMAP(n_neighbors=15,min_dist=0.1,random_state=42).fit_transform(Xz)
np.save(os.path.join(HERE,"vaxjo_umap.npy"),emb)

# KMeans on the z-scored feature space (clusters = regimes), try k=4..6
for k in [4,5,6]:
    km=KMeans(n_clusters=k,n_init=10,random_state=42).fit(Xz)
    lab=km.labels_
    print(f"\n--- k={k} cluster profiles (mean per-category score; n) ---")
    print("clu   n    "+" ".join(f"{c[:6]:>6s}" for c in CATS)+"  overall")
    for cl in range(k):
        m=X[lab==cl].mean(0); ov=overall[lab==cl].mean()
        print(f"{cl:3d} {np.sum(lab==cl):5d}  "+" ".join(f"{m[j]:6.2f}" for j in range(len(CATS)))+f"   {ov:5.2f}")
    np.save(os.path.join(HERE,f"vaxjo_km{k}.npy"),lab)

if __name__=="__main__":
    pass
