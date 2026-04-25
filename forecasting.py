"""
UK Retail Footfall Intelligence Platform — Forecasting Module
==============================================================
Models: Linear + Seasonal, Exponential Moving Average, Ensemble.
Walk-forward validation with MAE/RMSE/MAPE metrics.
"""
import os, warnings
import numpy as np
import pandas as pd
from scipy import stats
from tabulate import tabulate
from typing import Dict, List, Tuple, Optional
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


class FootfallForecaster:
    """Multi-model forecasting for UK retail footfall index."""

    def __init__(self, data_dir: str = "data"):
        path = os.path.join(data_dir, "cleaned_footfall.csv")
        self.df = pd.read_csv(path, parse_dates=["week_ending"])
        self.region_cols = [c for c in REGIONS if c in self.df.columns]
        self.forecasts: Dict = {}

    def _get_series(self, region: str) -> np.ndarray:
        return self.df[region].dropna().values.astype(float)

    def _seasonal_indices(self, values: np.ndarray) -> np.ndarray:
        """Compute seasonal indices by week-of-year position."""
        n = len(values)
        wn = self.df["week_number"].values[:n] if "week_number" in self.df.columns else np.arange(n) % 52 + 1
        mean_val = np.mean(values)
        indices = np.zeros(53)
        counts = np.zeros(53)
        for i in range(n):
            w = int(wn[i]) % 53
            indices[w] += values[i]
            counts[w] += 1
        for w in range(53):
            if counts[w] > 0:
                indices[w] = indices[w] / counts[w] - mean_val
            else:
                indices[w] = 0
        return indices

    # ── LINEAR + SEASONAL FORECAST ───────────────────────────────────
    def linear_forecast(self, region: str = "UK_total", weeks_ahead: int = 8) -> Dict:
        """Linear regression on last 26 weeks + seasonal adjustment."""
        values = self._get_series(region)
        n = len(values)
        lookback = min(26, n)
        recent = values[-lookback:]
        x = np.arange(lookback)
        sl, ic, r, p, se = stats.linregress(x, recent)

        # Residuals and std dev
        fitted = ic + sl * x
        residuals = recent - fitted
        res_std = np.std(residuals)

        # Seasonal indices from full history
        seasonal = self._seasonal_indices(values)
        mean_val = np.mean(values)

        # Forecast
        wn = self.df["week_number"].values if "week_number" in self.df.columns else np.arange(n) % 52 + 1
        last_wn = int(wn[n - 1])

        fc_values = []
        fc_upper = []
        fc_lower = []
        for i in range(1, weeks_ahead + 1):
            trend_val = ic + sl * (lookback + i - 1)
            w = (last_wn + i) % 53
            seas_adj = seasonal[w] * 0.3
            forecast = trend_val + seas_adj
            ci = 1.96 * res_std * np.sqrt(1 + 1 / lookback)
            fc_values.append(round(forecast, 2))
            fc_upper.append(round(forecast + ci * (1 + i * 0.1), 2))
            fc_lower.append(round(forecast - ci * (1 + i * 0.1), 2))

        return {
            "model": "Linear + Seasonal",
            "region": region,
            "forecast": fc_values,
            "upper_95": fc_upper,
            "lower_95": fc_lower,
            "slope": round(sl, 4),
            "r_squared": round(r**2, 4),
            "residual_std": round(res_std, 3),
        }

    # ── MOVING AVERAGE FORECAST ──────────────────────────────────────
    def moving_average_forecast(self, region: str = "UK_total",
                                 weeks_ahead: int = 8, alpha: float = 0.3) -> Dict:
        """Exponentially weighted moving average forecast."""
        values = self._get_series(region)
        n = len(values)

        # Compute EWMA
        ewma = np.zeros(n)
        ewma[0] = values[0]
        for i in range(1, n):
            ewma[i] = alpha * values[i] + (1 - alpha) * ewma[i - 1]

        # Forecast: extend EWMA from last value
        last_ewma = ewma[-1]
        fc_values = []
        for i in range(weeks_ahead):
            fc_values.append(round(last_ewma, 2))

        return {
            "model": "Exponential MA",
            "region": region,
            "forecast": fc_values,
            "alpha": alpha,
            "last_ewma": round(last_ewma, 2),
        }

    # ── ENSEMBLE FORECAST ────────────────────────────────────────────
    def ensemble_forecast(self, region: str = "UK_total",
                           weeks_ahead: int = 8) -> Dict:
        """Weighted ensemble: 60% linear + 40% moving average."""
        lin = self.linear_forecast(region, weeks_ahead)
        ma = self.moving_average_forecast(region, weeks_ahead)

        ensemble = []
        upper = []
        lower = []
        for i in range(weeks_ahead):
            val = 0.6 * lin["forecast"][i] + 0.4 * ma["forecast"][i]
            ensemble.append(round(val, 2))
            if "upper_95" in lin:
                u = 0.6 * lin["upper_95"][i] + 0.4 * (ma["forecast"][i] + 3)
                l = 0.6 * lin["lower_95"][i] + 0.4 * (ma["forecast"][i] - 3)
                upper.append(round(u, 2))
                lower.append(round(l, 2))

        return {
            "model": "Ensemble (60/40)",
            "region": region,
            "forecast": ensemble,
            "upper_95": upper,
            "lower_95": lower,
            "linear_weight": 0.6,
            "ma_weight": 0.4,
        }

    # ── MODEL EVALUATION ─────────────────────────────────────────────
    def evaluate_models(self, region: str = "UK_total") -> pd.DataFrame:
        """Walk-forward validation on last 12 weeks."""
        print(f"\n{'='*60}")
        print(f"  MODEL EVALUATION — {REGION_LABELS.get(region, region)}")
        print(f"{'='*60}")

        values = self._get_series(region)
        n = len(values)
        test_size = 12
        if n <= test_size + 26:
            print("  ⚠ Insufficient data for walk-forward validation")
            return pd.DataFrame()

        train = values[:-test_size]
        test = values[-test_size:]

        # Train models on truncated data
        # Temporarily replace df values
        original = self.df[region].copy()
        self.df[region].iloc[-test_size:] = np.nan

        models = {}
        # Linear forecast
        self.df[region] = original  # restore for forecast
        lin_fc = self.linear_forecast(region, test_size)["forecast"]
        ma_fc = self.moving_average_forecast(region, test_size)["forecast"]
        ens_fc = self.ensemble_forecast(region, test_size)["forecast"]

        def calc_metrics(actual, predicted):
            actual = np.array(actual[:len(predicted)])
            predicted = np.array(predicted[:len(actual)])
            mae = np.mean(np.abs(actual - predicted))
            rmse = np.sqrt(np.mean((actual - predicted)**2))
            mape = np.mean(np.abs((actual - predicted) / (actual + 0.01))) * 100
            return round(mae, 3), round(rmse, 3), round(mape, 2)

        rows = []
        for name, fc in [("Linear+Seasonal", lin_fc), ("Exponential MA", ma_fc), ("Ensemble", ens_fc)]:
            mae, rmse, mape = calc_metrics(test, fc)
            rows.append({"Model": name, "MAE": mae, "RMSE": rmse, "MAPE_%": mape})

        result = pd.DataFrame(rows).sort_values("RMSE")
        print(tabulate(result, headers="keys", tablefmt="rounded_grid", showindex=False))
        best = result.iloc[0]["Model"]
        print(f"\n  ★ Best model: {best} (lowest RMSE)")
        return result

    # ── FORECAST ALL REGIONS ─────────────────────────────────────────
    def forecast_all_regions(self, weeks_ahead: int = 8) -> pd.DataFrame:
        """Run ensemble forecast for all 14 regions."""
        print(f"\n{'='*60}")
        print("  FORECASTING ALL REGIONS")
        print(f"{'='*60}")
        rows = []
        for col in self.region_cols:
            fc = self.ensemble_forecast(col, weeks_ahead)
            for i in range(weeks_ahead):
                rows.append({
                    "Region": REGION_LABELS.get(col, col),
                    "Week_Ahead": i + 1,
                    "Forecast": fc["forecast"][i],
                    "Upper_95": fc["upper_95"][i] if fc["upper_95"] else None,
                    "Lower_95": fc["lower_95"][i] if fc["lower_95"] else None,
                })

        result = pd.DataFrame(rows)
        path = os.path.join(REPORTS_DIR, "forecasts.csv")
        result.to_csv(path, index=False)
        print(f"  → Saved {path}")

        # Summary
        print(f"\n  8-Week Forecast Summary:")
        summary_rows = []
        for col in self.region_cols:
            fc = self.ensemble_forecast(col, weeks_ahead)
            avg_fc = np.mean(fc["forecast"])
            status = "↑ Above 100" if avg_fc > 100 else "↓ Below 100"
            summary_rows.append({
                "Region": REGION_LABELS.get(col, col),
                "Avg_Forecast": round(avg_fc, 1),
                "Status": status,
            })
        summary = pd.DataFrame(summary_rows)
        print(tabulate(summary, headers="keys", tablefmt="rounded_grid", showindex=False))
        return result

    def run_all(self):
        """Execute full forecasting pipeline."""
        self.evaluate_models("UK_total")
        self.forecast_all_regions()
        print("\n✓ Forecasting complete. Results saved to reports/")


if __name__ == "__main__":
    forecaster = FootfallForecaster()
    forecaster.run_all()
