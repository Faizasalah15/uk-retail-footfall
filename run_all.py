"""
UK Retail Footfall Intelligence Platform — Master Runner
=========================================================
Executes all pipeline components in correct order with progress
tracking, error handling, and execution time reporting.
"""
import os
import sys
import time
from datetime import datetime


def progress_bar(current, total, label="", width=40):
    pct = current / total
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    sys.stdout.write(f"\r  [{bar}] {pct*100:.0f}% — {label}")
    sys.stdout.flush()
    if current == total:
        print()


def ensure_dirs():
    for d in ["data", "reports", "figures"]:
        os.makedirs(d, exist_ok=True)
        print(f"  ✓ Directory: {d}/")


def run_component(name, func, step, total):
    progress_bar(step, total, f"Running {name}...")
    t0 = time.time()
    try:
        func()
        elapsed = time.time() - t0
        print(f"  ✓ {name} completed in {elapsed:.2f}s")
        return elapsed, True
    except Exception as e:
        elapsed = time.time() - t0
        print(f"\n  ✗ {name} FAILED after {elapsed:.2f}s: {e}")
        return elapsed, False


def main():
    print()
    print("=" * 60)
    print("  UK RETAIL FOOTFALL INTELLIGENCE PLATFORM")
    print("  Master Pipeline Runner")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    ensure_dirs()

    components = []
    files_generated = []

    # ── Component 1: ETL Pipeline ──
    def run_etl():
        from data_pipeline import FootfallETL
        pipeline = FootfallETL(output_dir="data")
        pipeline.run()
        files_generated.extend([
            "data/cleaned_footfall.csv",
            "data/footfall_long.csv",
            "data/cleaned_footfall_sites.csv",
            "data/footfall_summary.json",
        ])
    components.append(("Data Pipeline (ETL)", run_etl))

    # ── Component 2: Statistical Analysis ──
    def run_analysis():
        from analysis import FootfallAnalyser
        analyser = FootfallAnalyser()
        analyser.run_all()
        files_generated.extend([
            "reports/descriptive_stats.csv",
            "reports/trend_analysis.csv",
            "reports/seasonal_components.csv",
            "reports/correlation_matrix.csv",
            "reports/anomalies.csv",
            "reports/site_type_comparison.csv",
            "reports/regional_rankings.csv",
            "reports/executive_summary.txt",
        ])
    components.append(("Statistical Analysis", run_analysis))

    # ── Component 3: Forecasting ──
    def run_forecast():
        from forecasting import FootfallForecaster
        forecaster = FootfallForecaster()
        forecaster.run_all()
        files_generated.append("reports/forecasts.csv")
    components.append(("Forecasting Models", run_forecast))

    # ── Component 4: Segmentation ──
    def run_segment():
        from segmentation import RegionalSegmenter
        segmenter = RegionalSegmenter()
        segmenter.run_all()
        files_generated.extend([
            "reports/clusters.csv",
            "reports/pca_coords.csv",
        ])
    components.append(("Regional Segmentation", run_segment))

    # ── Component 5: Visualisations ──
    def run_viz():
        from visualisations import generate_all_charts
        generate_all_charts()
        files_generated.extend([
            "figures/regional_trends.png",
            "figures/seasonal_heatmap.png",
            "figures/site_type_comparison.png",
            "figures/regional_ranking_bars.png",
            "figures/correlation_heatmap.png",
            "figures/forecast_chart.png",
            "figures/cluster_scatter.png",
            "figures/anomaly_timeline.png",
        ])
    components.append(("Visualisations", run_viz))

    # ── Execute all ──
    total = len(components)
    timings = []
    successes = 0

    print(f"\n  Running {total} components...\n")

    for i, (name, func) in enumerate(components):
        elapsed, ok = run_component(name, func, i + 1, total)
        timings.append((name, elapsed, ok))
        if ok:
            successes += 1
        print()

    # ── Final Summary ──
    total_time = sum(t[1] for t in timings)
    existing_files = [f for f in files_generated if os.path.exists(f)]

    print("=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Components run:    {total}")
    print(f"  Succeeded:         {successes}/{total}")
    print(f"  Files generated:   {len(existing_files)}")
    print(f"  Total time:        {total_time:.2f}s")
    print()
    print("  Timing breakdown:")
    for name, elapsed, ok in timings:
        status = "✓" if ok else "✗"
        print(f"    {status} {name:30s} {elapsed:6.2f}s")
    print()
    print("  Output directories:")
    print(f"    data/     — Cleaned datasets ({len([f for f in existing_files if f.startswith('data')])} files)")
    print(f"    reports/  — Analysis reports ({len([f for f in existing_files if f.startswith('reports')])} files)")
    print(f"    figures/  — Visualisations   ({len([f for f in existing_files if f.startswith('figures')])} files)")
    print()
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
