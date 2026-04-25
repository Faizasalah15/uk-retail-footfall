"""
UK Retail Footfall Intelligence Platform — Visualisations
==========================================================
8 professional matplotlib/seaborn charts saved as 300dpi PNG.
Dark theme consistent with Antigravity design system.
"""
import os, warnings
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial import ConvexHull
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
warnings.filterwarnings("ignore")

FIGURES_DIR = "figures"
os.makedirs(FIGURES_DIR, exist_ok=True)
plt.style.use("dark_background")
PALETTE = ["#00FFFF","#00E5CC","#7B61FF","#FF6B6B","#FFD93D",
           "#06D6A0","#F72585","#4ECDC4","#8338EC","#FF8C42",
           "#FB5607","#C77DFF","#6BCB77","#3A86FF"]
BG = "#0A0E1A"
REGIONS = ["UK_total","England","Northern_Ireland","Scotland","Wales",
    "East_of_England","East_Midlands","London","North_East","North_West",
    "South_East","South_West","West_Midlands","Yorkshire_and_The_Humber"]
RLABELS = {"UK_total":"UK Total","England":"England","Northern_Ireland":"N. Ireland",
    "Scotland":"Scotland","Wales":"Wales","East_of_England":"East of England",
    "East_Midlands":"East Midlands","London":"London","North_East":"North East",
    "North_West":"North West","South_East":"South East","South_West":"South West",
    "West_Midlands":"West Midlands","Yorkshire_and_The_Humber":"Yorkshire"}
SITES = ["District_and_Local_Centres","Retail_Parks","Town_and_City_Centres"]
SLABELS = {"District_and_Local_Centres":"District & Local","Retail_Parks":"Retail Parks",
    "Town_and_City_Centres":"Town & City Centres"}


def _setup_fig(figsize=(14, 7)):
    fig, ax = plt.subplots(figsize=figsize, facecolor=BG)
    ax.set_facecolor(BG)
    return fig, ax

def _save(fig, name):
    path = os.path.join(FIGURES_DIR, name)
    fig.savefig(path, dpi=300, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  → Saved {path}")


def chart_regional_trends(df):
    """CHART 1: All 14 regions weekly trend lines."""
    print("\n  Creating regional_trends.png …")
    fig, ax = _setup_fig((16, 8))
    cols = [c for c in REGIONS if c in df.columns]
    for i, col in enumerate(cols):
        lw = 2.5 if col == "UK_total" else 1.0
        alpha = 1.0 if col == "UK_total" else 0.65
        ax.plot(df["week_ending"], df[col], color=PALETTE[i % len(PALETTE)],
                linewidth=lw, alpha=alpha, label=RLABELS.get(col, col))
    ax.axhline(y=100, color="#00FFFF", linestyle="--", linewidth=0.8, alpha=0.4, label="Baseline (100)")
    ax.set_xlabel("Week Ending", fontsize=11, color="#8892A4")
    ax.set_ylabel("Footfall Index (2023 = 100)", fontsize=11, color="#8892A4")
    ax.set_title("UK Retail Footfall by Region (Jul 2024 – Apr 2026)",
                 fontsize=15, color="#00FFFF", fontweight="bold", pad=15)
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=7,
              framealpha=0.3, edgecolor="#00FFFF")
    ax.tick_params(colors="#8892A4")
    ax.grid(alpha=0.08)
    plt.xticks(rotation=45)
    _save(fig, "regional_trends.png")


def chart_seasonal_heatmap(df):
    """CHART 2: Region × Month heatmap."""
    print("  Creating seasonal_heatmap.png …")
    cols = [c for c in REGIONS if c in df.columns and c != "UK_total"]
    if "month" not in df.columns:
        df["month"] = df["week_ending"].dt.month
    pivot = df.groupby("month")[cols].mean()
    pivot.columns = [RLABELS.get(c, c) for c in pivot.columns]
    pivot.index = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][:len(pivot)]
    fig, ax = plt.subplots(figsize=(14, 8), facecolor=BG)
    ax.set_facecolor(BG)
    cmap = mcolors.LinearSegmentedColormap.from_list("cyber", ["#0A0E1A", "#003344", "#006666", "#00CCCC", "#00FFFF"])
    sns.heatmap(pivot.T, annot=True, fmt=".0f", cmap=cmap, linewidths=0.5,
                linecolor="#1a1a2e", ax=ax, cbar_kws={"label": "Index"},
                annot_kws={"size": 8, "color": "#E0E6F0"})
    ax.set_title("Monthly Footfall Index Heatmap", fontsize=15, color="#00FFFF",
                 fontweight="bold", pad=15)
    ax.tick_params(colors="#8892A4")
    _save(fig, "seasonal_heatmap.png")


def chart_site_type_comparison(df_site):
    """CHART 3: 3-panel site type comparison."""
    print("  Creating site_type_comparison.png …")
    if df_site is None:
        print("    ⚠ No site data"); return
    fig, axes = plt.subplots(1, 3, figsize=(18, 6), facecolor=BG, sharey=True)
    colors = ["#00FFFF", "#00E5CC", "#7B61FF"]
    site_cols = [c for c in SITES if c in df_site.columns]
    for i, col in enumerate(site_cols):
        ax = axes[i]
        ax.set_facecolor(BG)
        ax.plot(df_site["week_ending"], df_site[col], color=colors[i], linewidth=0.8, alpha=0.5)
        r4 = col + "_rolling_4w"
        if r4 in df_site.columns:
            ax.plot(df_site["week_ending"], df_site[r4], color=colors[i], linewidth=2, label="4-week avg")
        ax.axhline(y=100, color="#00FFFF", linestyle="--", linewidth=0.6, alpha=0.3)
        ax.set_title(SLABELS.get(col, col), fontsize=12, color=colors[i], fontweight="bold")
        ax.tick_params(colors="#8892A4", labelsize=8)
        ax.grid(alpha=0.06)
        ax.legend(fontsize=7, framealpha=0.3)
        plt.sca(ax); plt.xticks(rotation=45)
    fig.suptitle("Site Type Performance Comparison", fontsize=15, color="#00FFFF",
                 fontweight="bold", y=1.02)
    _save(fig, "site_type_comparison.png")


def chart_regional_ranking(rankings_path="reports/regional_rankings.csv"):
    """CHART 4: Horizontal bar chart of composite scores."""
    print("  Creating regional_ranking_bars.png …")
    if not os.path.exists(rankings_path):
        print("    ⚠ Rankings file not found"); return
    rk = pd.read_csv(rankings_path)
    rk = rk.sort_values("Composite", ascending=True)
    fig, ax = _setup_fig((12, 8))
    colors = []
    for i, score in enumerate(rk["Composite"]):
        if i == len(rk) - 1: colors.append("#FFD700")
        elif i < 3: colors.append("#FF4444")
        else: colors.append("#00FFFF")
    bars = ax.barh(rk["Region"], rk["Composite"], color=colors, height=0.6, edgecolor="none")
    for bar, score in zip(bars, rk["Composite"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                f"{score:.0f}", va="center", fontsize=9, color="#E0E6F0")
    ax.set_xlabel("Composite Score", fontsize=11, color="#8892A4")
    ax.set_title("Regional Composite Score Ranking", fontsize=15, color="#00FFFF",
                 fontweight="bold", pad=15)
    ax.set_xlim(0, 100)
    ax.tick_params(colors="#8892A4")
    ax.grid(axis="x", alpha=0.06)
    _save(fig, "regional_ranking_bars.png")


def chart_correlation_heatmap(corr_path="reports/correlation_matrix.csv"):
    """CHART 5: Correlation heatmap with masked upper triangle."""
    print("  Creating correlation_heatmap.png …")
    if not os.path.exists(corr_path):
        print("    ⚠ Correlation file not found"); return
    corr = pd.read_csv(corr_path, index_col=0)
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    fig, ax = plt.subplots(figsize=(12, 10), facecolor=BG)
    ax.set_facecolor(BG)
    cmap = mcolors.LinearSegmentedColormap.from_list("corr", ["#FF6B6B", "#0A0E1A", "#00FFFF"])
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap=cmap,
                linewidths=0.5, linecolor="#1a1a2e", ax=ax,
                annot_kws={"size": 7, "color": "#E0E6F0"},
                vmin=0.5, vmax=1.0)
    ax.set_title("Inter-Regional Footfall Correlation", fontsize=15, color="#00FFFF",
                 fontweight="bold", pad=15)
    ax.tick_params(colors="#8892A4", labelsize=8)
    _save(fig, "correlation_heatmap.png")


def chart_forecast(df, forecasts_path="reports/forecasts.csv"):
    """CHART 6: Actual + forecast with confidence band."""
    print("  Creating forecast_chart.png …")
    fig, ax = _setup_fig((14, 7))
    uk = df[df.columns[df.columns.str.contains("UK_total")]].iloc[:, 0] if "UK_total" in df.columns else df.iloc[:, 1]
    last20 = uk.iloc[-20:]
    dates_actual = df["week_ending"].iloc[-20:]
    ax.plot(dates_actual, last20, color="#00FFFF", linewidth=2.5, label="Actual", zorder=5)
    if os.path.exists(forecasts_path):
        fc = pd.read_csv(forecasts_path)
        fc_uk = fc[fc["Region"] == "UK Total"]
        if len(fc_uk) > 0:
            last_date = dates_actual.iloc[-1]
            fc_dates = pd.date_range(last_date + pd.Timedelta(weeks=1), periods=len(fc_uk), freq="7D")
            ax.plot(fc_dates, fc_uk["Forecast"].values, color="#00E5CC", linewidth=2,
                    linestyle="--", label="Forecast", zorder=5)
            if "Upper_95" in fc_uk.columns:
                ax.fill_between(fc_dates, fc_uk["Lower_95"].values, fc_uk["Upper_95"].values,
                                color="#00FFFF", alpha=0.08, label="95% CI")
            ax.axvline(x=last_date, color="#7B61FF", linestyle=":", linewidth=1, alpha=0.6, label="Forecast start")
    ax.axhline(y=100, color="#00FFFF", linestyle="--", linewidth=0.6, alpha=0.3)
    ax.set_xlabel("Week", fontsize=11, color="#8892A4")
    ax.set_ylabel("Footfall Index", fontsize=11, color="#8892A4")
    ax.set_title("UK Total Footfall Forecast (8 Weeks)", fontsize=15, color="#00FFFF",
                 fontweight="bold", pad=15)
    ax.legend(fontsize=9, framealpha=0.3, edgecolor="#00FFFF")
    ax.tick_params(colors="#8892A4")
    ax.grid(alpha=0.06)
    plt.xticks(rotation=45)
    _save(fig, "forecast_chart.png")


def chart_cluster_scatter(pca_path="reports/pca_coords.csv"):
    """CHART 7: PCA scatter with cluster colours and convex hulls."""
    print("  Creating cluster_scatter.png …")
    if not os.path.exists(pca_path):
        print("    ⚠ PCA file not found"); return
    pca = pd.read_csv(pca_path)
    fig, ax = _setup_fig((12, 8))
    cluster_colors = {"High-Performance Metros": "#00FFFF", "Stable Mid-Tier": "#00E5CC",
                      "Recovering Regions": "#7B61FF", "Emerging Regions": "#FFD93D"}
    for cluster in pca["cluster"].unique():
        mask = pca["cluster"] == cluster
        color = cluster_colors.get(cluster, "#FF6B6B")
        subset = pca[mask]
        ax.scatter(subset["PC1"], subset["PC2"], c=color, s=120, label=cluster,
                   edgecolors="white", linewidths=0.5, zorder=5, alpha=0.9)
        # Convex hull
        if mask.sum() >= 3:
            points = subset[["PC1", "PC2"]].values
            try:
                hull = ConvexHull(points)
                hull_pts = np.append(hull.vertices, hull.vertices[0])
                ax.plot(points[hull_pts, 0], points[hull_pts, 1], color=color,
                        linewidth=1, alpha=0.4, linestyle="--")
                ax.fill(points[hull_pts, 0], points[hull_pts, 1], color=color, alpha=0.06)
            except Exception:
                pass
    for _, row in pca.iterrows():
        ax.annotate(row["region"], (row["PC1"], row["PC2"]), fontsize=7,
                    color="#E0E6F0", textcoords="offset points", xytext=(5, 5))
    ax.set_xlabel("Principal Component 1", fontsize=11, color="#8892A4")
    ax.set_ylabel("Principal Component 2", fontsize=11, color="#8892A4")
    ax.set_title("Regional Clustering (PCA 2D Projection)", fontsize=15, color="#00FFFF",
                 fontweight="bold", pad=15)
    ax.legend(fontsize=9, framealpha=0.3, edgecolor="#00FFFF")
    ax.tick_params(colors="#8892A4")
    ax.grid(alpha=0.06)
    _save(fig, "cluster_scatter.png")


def chart_anomaly_timeline(df, anomalies_path="reports/anomalies.csv"):
    """CHART 8: Timeline with anomaly points highlighted."""
    print("  Creating anomaly_timeline.png …")
    fig, ax = _setup_fig((14, 7))
    if "UK_total" not in df.columns:
        print("    ⚠ UK_total not found"); return
    ax.plot(df["week_ending"], df["UK_total"], color="#00FFFF", linewidth=1.5, label="UK Total", zorder=3)
    if os.path.exists(anomalies_path):
        anom = pd.read_csv(anomalies_path, parse_dates=["week_ending"])
        if len(anom) > 0:
            ax.scatter(anom["week_ending"], anom["index"], color="#FF4444", s=60,
                       zorder=5, label=f"Anomalies ({len(anom)})", edgecolors="white", linewidths=0.5)
            top5 = anom.nlargest(3, "index")
            bot2 = anom.nsmallest(2, "index")
            for _, row in pd.concat([top5, bot2]).iterrows():
                ax.annotate(row["likely_cause"], (row["week_ending"], row["index"]),
                            fontsize=6, color="#FFD93D", textcoords="offset points",
                            xytext=(5, 10), arrowprops=dict(arrowstyle="->", color="#FFD93D", lw=0.5))
    ax.axhline(y=100, color="#00FFFF", linestyle="--", linewidth=0.6, alpha=0.3)
    ax.set_xlabel("Week", fontsize=11, color="#8892A4")
    ax.set_ylabel("Footfall Index", fontsize=11, color="#8892A4")
    ax.set_title("Footfall Anomaly Detection", fontsize=15, color="#00FFFF",
                 fontweight="bold", pad=15)
    ax.legend(fontsize=9, framealpha=0.3, edgecolor="#00FFFF")
    ax.tick_params(colors="#8892A4")
    ax.grid(alpha=0.06)
    plt.xticks(rotation=45)
    _save(fig, "anomaly_timeline.png")


def generate_all_charts(data_dir="data"):
    """Generate all 8 charts."""
    print("\n" + "="*60)
    print("  GENERATING VISUALISATIONS")
    print("="*60)
    df = pd.read_csv(os.path.join(data_dir, "cleaned_footfall.csv"), parse_dates=["week_ending"])
    df_site = None
    site_path = os.path.join(data_dir, "cleaned_footfall_sites.csv")
    if os.path.exists(site_path):
        df_site = pd.read_csv(site_path, parse_dates=["week_ending"])

    chart_regional_trends(df)
    chart_seasonal_heatmap(df)
    chart_site_type_comparison(df_site)
    chart_regional_ranking()
    chart_correlation_heatmap()
    chart_forecast(df)
    chart_cluster_scatter()
    chart_anomaly_timeline(df)
    print(f"\n✓ All 8 charts saved to {FIGURES_DIR}/")


if __name__ == "__main__":
    generate_all_charts()
