"""
UK Retail Footfall Intelligence Platform — Statistical Analysis
================================================================
Comprehensive analysis: descriptive stats, trends, seasonal decomposition,
correlation, anomaly detection, site type comparison, regional ranking.
"""
import os, json, warnings
import numpy as np
import pandas as pd
from scipy import stats
from tabulate import tabulate
from typing import Dict, List, Optional, Tuple
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
SITE_TYPES = ["District_and_Local_Centres","Retail_Parks","Town_and_City_Centres"]
SITE_LABELS = {"District_and_Local_Centres":"District & Local","Retail_Parks":"Retail Parks",
    "Town_and_City_Centres":"Town & City Centres"}

class FootfallAnalyser:
    """Comprehensive statistical analysis of UK retail footfall data."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.df = self._load("cleaned_footfall.csv")
        self.df_site = self._load("cleaned_footfall_sites.csv")
        self.region_cols = [c for c in REGIONS if c in self.df.columns]
        self.results: Dict = {}

    def _load(self, fname: str) -> Optional[pd.DataFrame]:
        path = os.path.join(self.data_dir, fname)
        if os.path.exists(path):
            df = pd.read_csv(path, parse_dates=["week_ending"])
            return df
        return None

    def _save_csv(self, df: pd.DataFrame, name: str):
        path = os.path.join(REPORTS_DIR, name)
        df.to_csv(path, index=False)
        print(f"  → Saved {path}")

    # ── DESCRIPTIVE STATS ────────────────────────────────────────────
    def descriptive_stats(self) -> pd.DataFrame:
        print("\n" + "="*60)
        print("  DESCRIPTIVE STATISTICS")
        print("="*60)
        rows = []
        for col in self.region_cols:
            v = self.df[col].dropna()
            rows.append({
                "Region": REGION_LABELS.get(col, col),
                "Mean": round(v.mean(), 2), "Median": round(v.median(), 2),
                "Std": round(v.std(), 2), "Min": round(v.min(), 2),
                "Max": round(v.max(), 2), "P25": round(v.quantile(0.25), 2),
                "P75": round(v.quantile(0.75), 2),
            })
        result = pd.DataFrame(rows)
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        self._save_csv(result, "descriptive_stats.csv")
        self.results["descriptive"] = result
        return result

    # ── TREND ANALYSIS ───────────────────────────────────────────────
    def trend_analysis(self) -> pd.DataFrame:
        print("\n" + "="*60)
        print("  TREND ANALYSIS (Linear Regression)")
        print("="*60)
        rows = []
        x = np.arange(len(self.df))
        for col in self.region_cols:
            y = self.df[col].values
            mask = ~np.isnan(y)
            sl, ic, r, p, se = stats.linregress(x[mask], y[mask])
            if sl > 0.1: direction = "STRONG_UP"
            elif sl > 0.03: direction = "MODERATE_UP"
            elif sl > -0.03: direction = "FLAT"
            elif sl > -0.1: direction = "MODERATE_DOWN"
            else: direction = "STRONG_DOWN"
            rows.append({
                "Region": REGION_LABELS.get(col, col),
                "Slope": round(sl, 4), "Intercept": round(ic, 2),
                "R_squared": round(r**2, 4), "P_value": round(p, 6),
                "Std_Error": round(se, 4), "Direction": direction,
            })
        result = pd.DataFrame(rows)
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        self._save_csv(result, "trend_analysis.csv")
        self.results["trends"] = result
        return result

    # ── SEASONAL DECOMPOSITION ───────────────────────────────────────
    def seasonal_decomposition(self, region: str = "UK_total") -> pd.DataFrame:
        print("\n" + "="*60)
        print(f"  SEASONAL DECOMPOSITION — {REGION_LABELS.get(region, region)}")
        print("="*60)
        y = self.df[region].values.copy()
        n = len(y)
        # Trend: centred moving average (use smaller window if < 52 weeks)
        window = min(13, n // 3) if n < 52 else 52
        trend = pd.Series(y).rolling(window=window, center=True, min_periods=1).mean().values
        # Detrended
        detrended = y - trend
        # Seasonal indices by week-of-year
        wn = self.df["week_number"].values if "week_number" in self.df.columns else np.arange(n) % 52 + 1
        seasonal = np.zeros(n)
        for w in range(1, 54):
            mask = wn == w
            if mask.sum() > 0:
                seasonal[mask] = np.nanmean(detrended[mask])
        # Residual
        residual = y - trend - seasonal
        result = pd.DataFrame({
            "week_ending": self.df["week_ending"],
            "observed": y, "trend": np.round(trend, 2),
            "seasonal": np.round(seasonal, 2), "residual": np.round(residual, 2),
        })
        peak_w = int(wn[np.argmax(seasonal)])
        trough_w = int(wn[np.argmin(seasonal)])
        amplitude = round(float(np.max(seasonal) - np.min(seasonal)), 2)
        print(f"  Peak week-of-year:    {peak_w}")
        print(f"  Trough week-of-year:  {trough_w}")
        print(f"  Seasonal amplitude:   {amplitude}")
        self._save_csv(result, "seasonal_components.csv")
        self.results["seasonal"] = {"peak_week": peak_w, "trough_week": trough_w, "amplitude": amplitude}
        return result

    # ── REGIONAL CORRELATION ─────────────────────────────────────────
    def regional_correlation(self) -> pd.DataFrame:
        print("\n" + "="*60)
        print("  REGIONAL CORRELATION MATRIX")
        print("="*60)
        sub = self.df[self.region_cols].rename(columns=REGION_LABELS)
        corr = sub.corr(method="pearson").round(3)
        # Most and least correlated pairs
        pairs = []
        cols = corr.columns.tolist()
        for i in range(len(cols)):
            for j in range(i+1, len(cols)):
                pairs.append((cols[i], cols[j], corr.iloc[i, j]))
        pairs.sort(key=lambda x: x[2])
        print(f"  Most correlated:  {pairs[-1][0]} ↔ {pairs[-1][1]} (r={pairs[-1][2]:.3f})")
        print(f"  Least correlated: {pairs[0][0]} ↔ {pairs[0][1]} (r={pairs[0][2]:.3f})")
        self._save_csv(corr.reset_index(), "correlation_matrix.csv")
        self.results["correlation"] = {"most": pairs[-1], "least": pairs[0]}
        return corr

    # ── ANOMALY DETECTION ────────────────────────────────────────────
    def anomaly_detection(self, region: str = "UK_total") -> pd.DataFrame:
        print("\n" + "="*60)
        print(f"  ANOMALY DETECTION — {REGION_LABELS.get(region, region)}")
        print("="*60)
        v = self.df[region].values
        mn, sd = np.nanmean(v), np.nanstd(v)
        q1, q3 = np.nanpercentile(v, 25), np.nanpercentile(v, 75)
        iqr = q3 - q1
        rows = []
        wn = self.df["week_number"].values if "week_number" in self.df.columns else np.arange(len(v)) % 52 + 1
        mn_col = self.df["month"].values if "month" in self.df.columns else np.ones(len(v))
        for i in range(len(v)):
            z = (v[i] - mn) / sd if sd > 0 else 0
            is_zscore = abs(z) > 2.5
            is_iqr = v[i] < (q1 - 1.5*iqr) or v[i] > (q3 + 1.5*iqr)
            if is_zscore or is_iqr:
                w, m = int(wn[i]), int(mn_col[i])
                if m == 12 and v[i] > mn: cause = "Christmas peak"
                elif m == 1 and v[i] < mn: cause = "January slump"
                elif m in (7, 8) and v[i] > mn: cause = "Summer peak"
                else: cause = "Anomaly — investigate"
                rows.append({
                    "week_ending": self.df["week_ending"].iloc[i],
                    "index": round(v[i], 2), "z_score": round(z, 2),
                    "method": "Z-score" if is_zscore else "IQR",
                    "likely_cause": cause,
                })
        result = pd.DataFrame(rows)
        print(f"  Anomalies detected: {len(result)}")
        if len(result) > 0:
            print(tabulate(result.head(10), headers="keys", tablefmt="rounded_grid", showindex=False))
        self._save_csv(result, "anomalies.csv")
        self.results["anomalies"] = len(result)
        return result

    # ── SITE TYPE ANALYSIS ───────────────────────────────────────────
    def site_type_analysis(self) -> pd.DataFrame:
        print("\n" + "="*60)
        print("  SITE TYPE ANALYSIS")
        print("="*60)
        if self.df_site is None:
            print("  ⚠ No site type data available"); return pd.DataFrame()
        rows = []
        site_cols = [c for c in SITE_TYPES if c in self.df_site.columns]
        for col in site_cols:
            v = self.df_site[col].dropna()
            mn_col = self.df_site["month"].values if "month" in self.df_site.columns else np.ones(len(v))
            dec = v[mn_col == 12]; jan = v[mn_col == 1]
            rows.append({
                "Site_Type": SITE_LABELS.get(col, col),
                "Mean": round(v.mean(), 2), "Std": round(v.std(), 2),
                "Min": round(v.min(), 2), "Max": round(v.max(), 2),
                "Xmas_Peak": round(dec.mean(), 2) if len(dec) > 0 else np.nan,
                "Jan_Trough": round(jan.mean(), 2) if len(jan) > 0 else np.nan,
            })
        result = pd.DataFrame(rows)
        # Pairwise t-tests
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        comparisons = []
        for i in range(len(site_cols)):
            for j in range(i+1, len(site_cols)):
                a = self.df_site[site_cols[i]].dropna().values
                b = self.df_site[site_cols[j]].dropna().values
                t_stat, p_val = stats.ttest_ind(a, b, equal_var=False)
                pooled_std = np.sqrt((np.var(a) + np.var(b)) / 2)
                cohens_d = (np.mean(a) - np.mean(b)) / pooled_std if pooled_std > 0 else 0
                comparisons.append({
                    "Pair": f"{SITE_LABELS.get(site_cols[i],'?')} vs {SITE_LABELS.get(site_cols[j],'?')}",
                    "t_stat": round(t_stat, 3), "p_value": round(p_val, 6),
                    "Cohens_d": round(cohens_d, 3),
                    "Significant": "Yes" if p_val < 0.05 else "No",
                })
        comp_df = pd.DataFrame(comparisons)
        print("\n  Pairwise T-Tests:")
        print(tabulate(comp_df, headers="keys", tablefmt="rounded_grid", showindex=False))
        self._save_csv(result, "site_type_comparison.csv")
        self.results["site_types"] = result.to_dict("records")
        return result

    # ── REGIONAL RANKING ─────────────────────────────────────────────
    def regional_ranking(self, weights: Dict[str, float] = None) -> pd.DataFrame:
        print("\n" + "="*60)
        print("  REGIONAL RANKING (Composite Score)")
        print("="*60)
        if weights is None:
            weights = {"current": 0.20, "trend": 0.20, "stability": 0.15,
                       "recovery": 0.15, "peak": 0.15, "consistency": 0.15}
        rows = []
        x = np.arange(len(self.df))
        for col in self.region_cols:
            v = self.df[col].dropna().values
            mn_col = self.df["month"].values if "month" in self.df.columns else np.ones(len(v))
            sl = stats.linregress(x[:len(v)], v).slope
            dec = v[mn_col[:len(v)] == 12]; jan = v[mn_col[:len(v)] == 1]
            current = min(100, v[-1] / 1.4) if len(v) > 0 else 0
            trend_norm = min(100, max(0, 50 + sl * 200))
            amplitude = np.max(v) - np.min(v)
            stability = max(0, 100 - amplitude)
            recovery = min(100, (v[-1] - np.min(v)) / (np.max(v) - np.min(v) + 0.01) * 100) if len(v) > 1 else 0
            peak = min(100, np.max(dec) / 1.5) if len(dec) > 0 else 50
            consistency = max(0, 100 - np.std(v) * 5)
            composite = round(
                current * weights["current"] + trend_norm * weights["trend"] +
                stability * weights["stability"] + recovery * weights["recovery"] +
                peak * weights["peak"] + consistency * weights["consistency"], 1
            )
            rows.append({
                "Region": REGION_LABELS.get(col, col), "Current": round(current, 1),
                "Trend": round(trend_norm, 1), "Stability": round(stability, 1),
                "Recovery": round(recovery, 1), "Peak": round(peak, 1),
                "Consistency": round(consistency, 1), "Composite": composite,
            })
        result = pd.DataFrame(rows).sort_values("Composite", ascending=False).reset_index(drop=True)
        result.insert(0, "Rank", range(1, len(result) + 1))
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        self._save_csv(result, "regional_rankings.csv")
        self.results["rankings"] = result.to_dict("records")
        return result

    # ── EXECUTIVE REPORT ─────────────────────────────────────────────
    def generate_report(self):
        print("\n" + "="*60)
        print("  EXECUTIVE SUMMARY REPORT")
        print("="*60)
        lines = []
        lines.append("UK RETAIL FOOTFALL INTELLIGENCE — EXECUTIVE SUMMARY")
        lines.append("=" * 55)
        lines.append(f"Report generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"Data source: ONS / BT Active Intelligence")
        lines.append(f"Coverage: {self.df['week_ending'].min().date()} to {self.df['week_ending'].max().date()}")
        lines.append(f"Observations: {len(self.df)} weeks × {len(self.region_cols)} regions")
        lines.append("")
        lines.append("EXECUTIVE SUMMARY")
        lines.append("-" * 40)
        uk = self.df["UK_total"]
        lines.append(f"The UK retail footfall index currently stands at {uk.iloc[-1]:.1f},")
        if uk.iloc[-1] > 100:
            lines.append(f"which is {uk.iloc[-1]-100:.1f} points above the 2023 baseline.")
        else:
            lines.append(f"which is {100-uk.iloc[-1]:.1f} points below the 2023 baseline.")
        lines.append("")
        lines.append("KEY FINDINGS")
        lines.append("-" * 40)
        if "rankings" in self.results and self.results["rankings"]:
            top = self.results["rankings"][0]
            lines.append(f"1. Top-performing region: {top['Region']} (score: {top['Composite']})")
        if "London" in self.df.columns:
            lon_diff = self.df["London"].iloc[-1] - self.df["UK_total"].iloc[-1]
            lines.append(f"2. London premium: {lon_diff:+.1f} points above UK average")
        lines.append(f"3. UK footfall range: {uk.min():.1f} (trough) to {uk.max():.1f} (peak)")
        lines.append(f"4. Current trend: {'upward' if uk.iloc[-1] > uk.iloc[-13] else 'downward'} over last 12 weeks")
        if "anomalies" in self.results:
            lines.append(f"5. Anomalies detected: {self.results['anomalies']} data points flagged")
        lines.append("")
        lines.append("REGIONAL ANALYSIS")
        lines.append("-" * 40)
        for col in self.region_cols[:5]:
            v = self.df[col]
            lines.append(f"  {REGION_LABELS.get(col,col):25s}: Latest={v.iloc[-1]:.1f}  Mean={v.mean():.1f}  Trend={'↑' if v.iloc[-1]>v.iloc[-13] else '↓'}")
        lines.append("  ...")
        lines.append("")
        lines.append("SITE TYPE ANALYSIS")
        lines.append("-" * 40)
        if self.df_site is not None:
            for col in SITE_TYPES:
                if col in self.df_site.columns:
                    v = self.df_site[col]
                    lines.append(f"  {SITE_LABELS.get(col,col):25s}: Latest={v.iloc[-1]:.1f}  Mean={v.mean():.1f}")
        lines.append("")
        lines.append("SEASONAL PATTERNS")
        lines.append("-" * 40)
        if "seasonal" in self.results:
            s = self.results["seasonal"]
            lines.append(f"  Peak week:      Week {s['peak_week']}")
            lines.append(f"  Trough week:    Week {s['trough_week']}")
            lines.append(f"  Amplitude:      {s['amplitude']:.1f} index points")
        lines.append("")
        lines.append("RECOMMENDATIONS")
        lines.append("-" * 40)
        lines.append("1. Focus investment on top-ranked regions showing strong momentum.")
        lines.append("2. Plan inventory and staffing around December peak (weeks 49-52).")
        lines.append("3. Develop January recovery strategies for underperforming regions.")
        lines.append("4. Monitor District & Local Centres — consistently highest baseline.")
        lines.append("5. Use forecast projections for 8-week operational planning horizon.")
        report = "\n".join(lines)
        print(report)
        path = os.path.join(REPORTS_DIR, "executive_summary.txt")
        with open(path, "w") as f:
            f.write(report)
        print(f"\n  → Saved {path}")
        return report

    def run_all(self):
        """Execute all analysis methods in sequence."""
        self.descriptive_stats()
        self.trend_analysis()
        self.seasonal_decomposition()
        self.regional_correlation()
        self.anomaly_detection()
        self.site_type_analysis()
        self.regional_ranking()
        self.generate_report()
        print("\n✓ All analyses complete. Reports saved to reports/")

if __name__ == "__main__":
    analyser = FootfallAnalyser()
    analyser.run_all()
