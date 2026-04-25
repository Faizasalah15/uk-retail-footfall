"""
UK Retail Footfall Intelligence Platform — Regional Segmentation
=================================================================
K-Means clustering, PCA dimensionality reduction, cluster profiling.
"""
import os, warnings
import numpy as np
import pandas as pd
from scipy import stats
from tabulate import tabulate
from typing import Dict, List, Optional
warnings.filterwarnings("ignore")

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)
REGIONS = ["UK_total","England","Northern_Ireland","Scotland","Wales",
    "East_of_England","East_Midlands","London","North_East","North_West",
    "South_East","South_West","West_Midlands","Yorkshire_and_The_Humber"]
REGION_LABELS = {"UK_total":"UK Total","England":"England","Northern_Ireland":"Northern Ireland",
    "Scotland":"Scotland","Wales":"Wales","East_of_England":"East of England",
    "East_Midlands":"East Midlands","London":"London","North_East":"North East",
    "North_West":"North West","South_East":"South East","South_West":"South West",
    "West_Midlands":"West Midlands","Yorkshire_and_The_Humber":"Yorkshire & Humber"}


class RegionalSegmenter:
    """Regional segmentation via clustering and PCA."""

    def __init__(self, data_dir: str = "data"):
        path = os.path.join(data_dir, "cleaned_footfall.csv")
        self.df = pd.read_csv(path, parse_dates=["week_ending"])
        self.region_cols = [c for c in REGIONS if c in self.df.columns]
        self.feature_matrix: Optional[pd.DataFrame] = None
        self.features_norm: Optional[np.ndarray] = None
        self.cluster_labels: Optional[np.ndarray] = None
        self.pca_coords: Optional[np.ndarray] = None

    # ── BUILD FEATURE MATRIX ─────────────────────────────────────────
    def build_feature_matrix(self) -> pd.DataFrame:
        """Build feature matrix with key metrics per region."""
        print(f"\n{'='*60}")
        print("  BUILDING FEATURE MATRIX")
        print(f"{'='*60}")
        rows = []
        x = np.arange(len(self.df))
        mn_col = self.df["month"].values if "month" in self.df.columns else np.ones(len(self.df))
        for col in self.region_cols:
            v = self.df[col].dropna().values
            sl = stats.linregress(x[:len(v)], v).slope
            dec = v[mn_col[:len(v)] == 12]
            jan = v[mn_col[:len(v)] == 1]
            trough = np.min(v)
            recovery = (v[-1] - trough) / (np.max(v) - trough + 0.01)
            yoy = v[-1] - v[max(0, len(v)-53)] if len(v) > 52 else 0
            rows.append({
                "region": REGION_LABELS.get(col, col),
                "region_key": col,
                "mean_index": round(np.mean(v), 2),
                "std_index": round(np.std(v), 2),
                "trend_slope": round(sl, 4),
                "seasonal_amplitude": round(np.max(v) - np.min(v), 2),
                "christmas_peak": round(np.mean(dec), 2) if len(dec) > 0 else np.mean(v),
                "january_trough": round(np.mean(jan), 2) if len(jan) > 0 else np.mean(v),
                "recovery_rate": round(recovery, 3),
                "current_index": round(v[-1], 2),
                "yoy_change": round(yoy, 2),
            })
        self.feature_matrix = pd.DataFrame(rows)
        print(tabulate(self.feature_matrix, headers="keys", tablefmt="rounded_grid", showindex=False))
        # Normalise
        feat_cols = ["mean_index", "std_index", "trend_slope", "seasonal_amplitude",
                     "christmas_peak", "january_trough", "recovery_rate", "current_index"]
        data = self.feature_matrix[feat_cols].values.astype(float)
        self.feat_means = data.mean(axis=0)
        self.feat_stds = data.std(axis=0) + 1e-8
        self.features_norm = (data - self.feat_means) / self.feat_stds
        print(f"\n  Feature matrix: {self.features_norm.shape[0]} regions × {self.features_norm.shape[1]} features (normalised)")
        return self.feature_matrix

    # ── K-MEANS CLUSTERING ───────────────────────────────────────────
    def kmeans_clustering(self, k: int = 3) -> pd.DataFrame:
        """K-Means clustering with silhouette score selection."""
        print(f"\n{'='*60}")
        print("  K-MEANS CLUSTERING")
        print(f"{'='*60}")
        if self.features_norm is None:
            self.build_feature_matrix()
        X = self.features_norm
        n = X.shape[0]

        def _kmeans(X, k, max_iter=100):
            np.random.seed(42)
            idx = np.random.choice(n, k, replace=False)
            centroids = X[idx].copy()
            for _ in range(max_iter):
                dists = np.array([[np.linalg.norm(x - c) for c in centroids] for x in X])
                labels = np.argmin(dists, axis=1)
                new_centroids = np.array([X[labels == i].mean(axis=0) if (labels == i).sum() > 0 else centroids[i] for i in range(k)])
                if np.allclose(centroids, new_centroids): break
                centroids = new_centroids
            return labels, centroids

        def _silhouette(X, labels):
            n = len(X)
            unique = np.unique(labels)
            if len(unique) < 2: return -1
            scores = []
            for i in range(n):
                same = X[labels == labels[i]]
                a = np.mean([np.linalg.norm(X[i] - s) for s in same if not np.array_equal(s, X[i])]) if len(same) > 1 else 0
                b_vals = []
                for c in unique:
                    if c != labels[i]:
                        other = X[labels == c]
                        b_vals.append(np.mean([np.linalg.norm(X[i] - o) for o in other]))
                b = min(b_vals) if b_vals else 0
                scores.append((b - a) / max(a, b, 1e-8))
            return np.mean(scores)

        # Try k=2,3,4
        best_k, best_score, best_labels = k, -1, None
        print("  Silhouette scores:")
        for kk in [2, 3, 4]:
            if kk >= n: continue
            labels, _ = _kmeans(X, kk)
            score = _silhouette(X, labels)
            marker = " ★" if score > best_score else ""
            print(f"    k={kk}: {score:.3f}{marker}")
            if score > best_score:
                best_score = score
                best_k = kk
                best_labels = labels

        print(f"\n  Selected k={best_k} (silhouette={best_score:.3f})")
        self.cluster_labels = best_labels

        # Assign meaningful cluster names based on mean index
        cluster_means = {}
        for c in range(best_k):
            mask = best_labels == c
            cluster_means[c] = np.mean(self.feature_matrix.loc[mask, "mean_index"])
        sorted_clusters = sorted(cluster_means.keys(), key=lambda c: cluster_means[c], reverse=True)
        names = ["High-Performance Metros", "Stable Mid-Tier", "Recovering Regions", "Emerging Regions"]
        name_map = {sorted_clusters[i]: names[i] for i in range(len(sorted_clusters))}

        self.feature_matrix["cluster_id"] = best_labels
        self.feature_matrix["cluster_name"] = [name_map[l] for l in best_labels]

        result = self.feature_matrix[["region", "cluster_id", "cluster_name", "mean_index", "current_index"]].copy()
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        self.feature_matrix.to_csv(os.path.join(REPORTS_DIR, "clusters.csv"), index=False)
        print(f"  → Saved reports/clusters.csv")
        return result

    # ── PROFILE CLUSTERS ─────────────────────────────────────────────
    def profile_clusters(self) -> pd.DataFrame:
        """Profile each cluster by feature means."""
        print(f"\n{'='*60}")
        print("  CLUSTER PROFILES")
        print(f"{'='*60}")
        if self.cluster_labels is None:
            self.kmeans_clustering()
        feat_cols = ["mean_index", "std_index", "trend_slope", "seasonal_amplitude",
                     "christmas_peak", "january_trough", "recovery_rate", "current_index"]
        profiles = self.feature_matrix.groupby("cluster_name")[feat_cols].mean().round(2)
        print(tabulate(profiles.reset_index(), headers="keys", tablefmt="rounded_grid", showindex=False))
        # Defining characteristic
        for name, row in profiles.iterrows():
            best_feat = row.idxmax()
            print(f"  {name}: Highest in '{best_feat}' ({row[best_feat]:.2f})")
        return profiles

    # ── PCA REDUCTION ────────────────────────────────────────────────
    def pca_reduction(self) -> pd.DataFrame:
        """PCA to 2 components for visualisation."""
        print(f"\n{'='*60}")
        print("  PCA DIMENSIONALITY REDUCTION")
        print(f"{'='*60}")
        if self.features_norm is None:
            self.build_feature_matrix()
        X = self.features_norm
        # Manual PCA via SVD
        X_centered = X - X.mean(axis=0)
        U, S, Vt = np.linalg.svd(X_centered, full_matrices=False)
        components = X_centered @ Vt[:2].T
        total_var = np.sum(S**2)
        var_explained = [(S[i]**2 / total_var) * 100 for i in range(min(2, len(S)))]
        self.pca_coords = components
        result = pd.DataFrame({
            "region": self.feature_matrix["region"],
            "PC1": np.round(components[:, 0], 4),
            "PC2": np.round(components[:, 1], 4) if components.shape[1] > 1 else 0,
            "cluster": self.feature_matrix.get("cluster_name", "Unknown"),
        })
        result.to_csv(os.path.join(REPORTS_DIR, "pca_coords.csv"), index=False)
        print(f"  PC1 variance explained: {var_explained[0]:.1f}%")
        if len(var_explained) > 1:
            print(f"  PC2 variance explained: {var_explained[1]:.1f}%")
            print(f"  Total (2 components):   {sum(var_explained):.1f}%")
        print(f"  → Saved reports/pca_coords.csv")
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        return result

    def run_all(self):
        """Execute full segmentation pipeline."""
        self.build_feature_matrix()
        self.kmeans_clustering()
        self.profile_clusters()
        self.pca_reduction()
        print("\n✓ Segmentation complete. Results saved to reports/")


if __name__ == "__main__":
    segmenter = RegionalSegmenter()
    segmenter.run_all()
