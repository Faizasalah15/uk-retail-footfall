"""
UK Retail Footfall Intelligence Platform — ETL Pipeline
========================================================
Author:  Ashima Faiza Salahudeen Alimajasmin
Data:    ONS / BT Active Intelligence (Jul 2024 – Apr 2026)
Purpose: Extract, validate, clean, transform, and export
         UK retail footfall index data for downstream analytics.

Usage:
    python data_pipeline.py
    python data_pipeline.py --input data/raw_footfall.xlsx --output data/
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ── Logging Configuration ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("FootfallETL")

# ── Constants ─────────────────────────────────────────────────────────
REGIONS = [
    "UK_total", "England", "Northern_Ireland", "Scotland", "Wales",
    "East_of_England", "East_Midlands", "London", "North_East",
    "North_West", "South_East", "South_West", "West_Midlands",
    "Yorkshire_and_The_Humber",
]

REGION_LABELS = {
    "UK_total": "UK Total", "England": "England",
    "Northern_Ireland": "Northern Ireland", "Scotland": "Scotland",
    "Wales": "Wales", "East_of_England": "East of England",
    "East_Midlands": "East Midlands", "London": "London",
    "North_East": "North East", "North_West": "North West",
    "South_East": "South East", "South_West": "South West",
    "West_Midlands": "West Midlands",
    "Yorkshire_and_The_Humber": "Yorkshire & The Humber",
}

SITE_TYPES = ["District_and_Local_Centres", "Retail_Parks", "Town_and_City_Centres"]

SITE_LABELS = {
    "District_and_Local_Centres": "District & Local Centres",
    "Retail_Parks": "Retail Parks",
    "Town_and_City_Centres": "Town & City Centres",
}

SEASON_MAP = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Autumn", 10: "Autumn", 11: "Autumn",
}

INDEX_MIN, INDEX_MAX = 50, 200  # Sanity bounds for footfall index


class FootfallETL:
    """
    End-to-end ETL pipeline for ONS / BT Active Intelligence
    retail footfall data.

    Pipeline stages:
        1. load_raw()    – ingest from CSV/Excel or generate synthetic data
        2. validate()    – quality checks and anomaly flagging
        3. clean()       – handle nulls, duplicates, type enforcement
        4. transform()   – derive columns, reshape, rolling averages
        5. export()      – write cleaned outputs to disk
    """

    def __init__(self, input_path: Optional[str] = None, output_dir: str = "data"):
        self.input_path = input_path
        self.output_dir = output_dir
        self.df_weekly_region: Optional[pd.DataFrame] = None
        self.df_weekly_site: Optional[pd.DataFrame] = None
        self.df_long: Optional[pd.DataFrame] = None
        self.validation_log: List[str] = []
        self.stats = {
            "rows_raw": 0,
            "rows_final": 0,
            "nulls_filled": 0,
            "anomalies_flagged": 0,
            "duplicates_removed": 0,
        }

    # ── 1. LOAD ──────────────────────────────────────────────────────
    def load_raw(self) -> "FootfallETL":
        """
        Load raw data from file or generate synthetic data mirroring
        official ONS/BT Active Intelligence characteristics.
        """
        logger.info("STAGE 1: Loading raw data …")

        if self.input_path and os.path.exists(self.input_path):
            ext = os.path.splitext(self.input_path)[1].lower()
            if ext in (".xlsx", ".xls"):
                logger.info(f"Reading Excel file: {self.input_path}")
                self.df_weekly_region = pd.read_excel(
                    self.input_path, sheet_name=0, engine="openpyxl"
                )
                try:
                    self.df_weekly_site = pd.read_excel(
                        self.input_path, sheet_name=1, engine="openpyxl"
                    )
                except Exception:
                    logger.warning("No second sheet found — generating site type data")
                    self.df_weekly_site = None
            elif ext == ".csv":
                logger.info(f"Reading CSV file: {self.input_path}")
                self.df_weekly_region = pd.read_csv(self.input_path)
                self.df_weekly_site = None
            else:
                raise ValueError(f"Unsupported file format: {ext}")
        else:
            logger.info("No input file found — generating synthetic dataset")
            self._generate_synthetic_data()

        self.stats["rows_raw"] = len(self.df_weekly_region)
        logger.info(
            f"  Loaded {self.stats['rows_raw']} weekly rows, "
            f"{len(self.df_weekly_region.columns)} columns"
        )
        return self

    def _generate_synthetic_data(self):
        """
        Generate realistic synthetic footfall data mirroring
        ONS / BT Active Intelligence patterns.
        Index = 100 (2023 average baseline).
        """
        np.random.seed(42)
        start = datetime(2024, 7, 7)
        dates = [start + timedelta(weeks=i) for i in range(90)]

        region_offsets = {
            "UK_total": 0, "England": 1, "Northern_Ireland": -3,
            "Scotland": -1, "Wales": -2, "East_of_England": 3,
            "East_Midlands": -1, "London": 7, "North_East": -4,
            "North_West": 0, "South_East": 3, "South_West": 1,
            "West_Midlands": -1, "Yorkshire_and_The_Humber": -3,
        }
        region_volatility = {
            "UK_total": 1.0, "England": 1.0, "Northern_Ireland": 1.7,
            "Scotland": 1.5, "Wales": 1.2, "East_of_England": 1.0,
            "East_Midlands": 1.1, "London": 1.0, "North_East": 1.2,
            "North_West": 1.1, "South_East": 1.0, "South_West": 1.1,
            "West_Midlands": 1.1, "Yorkshire_and_The_Humber": 1.2,
        }
        site_offsets = {
            "District_and_Local_Centres": 4,
            "Retail_Parks": -2,
            "Town_and_City_Centres": -4,
        }
        site_xmas = {
            "District_and_Local_Centres": 1.15,
            "Retail_Parks": 0.70,
            "Town_and_City_Centres": 1.30,
        }
        site_jan = {
            "District_and_Local_Centres": 0.90,
            "Retail_Parks": 1.30,
            "Town_and_City_Centres": 0.80,
        }

        def seasonal_component(month: int, day: int) -> float:
            w = ((month - 1) * 30 + day) // 7 % 52
            xmas = 38 * np.exp(-0.5 * ((w - 50) / 2.5) ** 2)
            summer = 18 * np.exp(-0.5 * ((w - 31) / 5) ** 2)
            jan_dip = -16 * np.exp(-0.5 * ((w - 1) / 2.2) ** 2)
            return xmas + summer + jan_dip

        # Weekly region data
        rows_region = []
        for d in dates:
            s = seasonal_component(d.month, d.day)
            row = {"week_ending": d.strftime("%Y-%m-%d")}
            for r in REGIONS:
                base = 103 + region_offsets[r]
                vol = region_volatility[r]
                noise = np.random.uniform(-2, 2)
                row[r] = round(base + s * vol * 0.85 + noise, 1)
            rows_region.append(row)

        self.df_weekly_region = pd.DataFrame(rows_region)

        # Weekly site type data
        rows_site = []
        for d in dates:
            s = seasonal_component(d.month, d.day)
            row = {"week_ending": d.strftime("%Y-%m-%d")}
            for st in SITE_TYPES:
                base = 103 + site_offsets[st]
                sm = s
                if d.month == 12:
                    sm *= site_xmas[st]
                if d.month == 1:
                    sm *= site_jan[st]
                noise = np.random.uniform(-1.8, 1.8)
                row[st] = round(base + sm * 0.82 + noise, 1)
            rows_site.append(row)

        self.df_weekly_site = pd.DataFrame(rows_site)
        logger.info("  Synthetic data generated: 90 weeks × 14 regions + 3 site types")

    # ── 2. VALIDATE ──────────────────────────────────────────────────
    def validate(self) -> "FootfallETL":
        """
        Run data quality checks and log issues.
        """
        logger.info("STAGE 2: Validating data quality …")
        df = self.df_weekly_region

        # 2a. Null percentage per column
        null_pcts = df.isnull().mean() * 100
        for col, pct in null_pcts.items():
            if pct > 0:
                msg = f"  ⚠ Column '{col}': {pct:.1f}% null"
                logger.warning(msg)
                self.validation_log.append(msg)
        if null_pcts.sum() == 0:
            logger.info("  ✓ No null values detected")

        # 2b. Index range sanity check
        numeric_cols = [c for c in df.columns if c != "week_ending"]
        for col in numeric_cols:
            vals = pd.to_numeric(df[col], errors="coerce")
            out_of_range = vals[(vals < INDEX_MIN) | (vals > INDEX_MAX)]
            if len(out_of_range) > 0:
                msg = f"  ⚠ Column '{col}': {len(out_of_range)} values outside [{INDEX_MIN}, {INDEX_MAX}]"
                logger.warning(msg)
                self.validation_log.append(msg)
                self.stats["anomalies_flagged"] += len(out_of_range)

        # 2c. Date continuity check
        df["_date_check"] = pd.to_datetime(df["week_ending"], errors="coerce")
        if df["_date_check"].isnull().any():
            self._try_parse_serial_dates(df)

        date_sorted = df["_date_check"].sort_values().reset_index(drop=True)
        expected_gap = timedelta(weeks=1)
        missing_weeks = 0
        for i in range(1, len(date_sorted)):
            gap = date_sorted[i] - date_sorted[i - 1]
            if gap > expected_gap + timedelta(days=2):
                missing_weeks += int(gap.days / 7) - 1
                msg = f"  ⚠ Missing {int(gap.days / 7) - 1} week(s) between {date_sorted[i-1].date()} and {date_sorted[i].date()}"
                logger.warning(msg)
                self.validation_log.append(msg)
        if missing_weeks == 0:
            logger.info("  ✓ Date continuity — no gaps detected")
        df.drop(columns=["_date_check"], inplace=True)

        # 2d. Duplicate detection
        dupes = df.duplicated(subset=["week_ending"]).sum()
        if dupes > 0:
            msg = f"  ⚠ {dupes} duplicate rows detected"
            logger.warning(msg)
            self.validation_log.append(msg)
            self.stats["duplicates_removed"] = dupes
        else:
            logger.info("  ✓ No duplicate rows")

        # 2e. Data type enforcement
        for col in numeric_cols:
            if not pd.api.types.is_numeric_dtype(df[col]):
                logger.warning(f"  ⚠ Column '{col}' is not numeric — will coerce")
                self.validation_log.append(f"Type issue: {col}")

        logger.info(
            f"  Validation complete: {len(self.validation_log)} issue(s) logged"
        )
        return self

    def _try_parse_serial_dates(self, df: pd.DataFrame):
        """Attempt to convert Excel serial dates to datetime."""
        try:
            serial = pd.to_numeric(df["week_ending"], errors="coerce")
            if serial.notna().sum() > len(df) * 0.5:
                origin = datetime(1899, 12, 30)
                df["_date_check"] = serial.apply(
                    lambda x: origin + timedelta(days=int(x)) if pd.notna(x) else pd.NaT
                )
                logger.info("  Parsed Excel serial dates to datetime")
        except Exception as e:
            logger.warning(f"  Serial date parse failed: {e}")

    # ── 3. CLEAN ─────────────────────────────────────────────────────
    def clean(self) -> "FootfallETL":
        """
        Handle nulls, remove duplicates, enforce data types.
        """
        logger.info("STAGE 3: Cleaning data …")
        df = self.df_weekly_region

        # Remove duplicates
        before = len(df)
        df = df.drop_duplicates(subset=["week_ending"], keep="first")
        removed = before - len(df)
        if removed:
            logger.info(f"  Removed {removed} duplicate rows")

        # Parse dates
        df["week_ending"] = pd.to_datetime(df["week_ending"], errors="coerce")

        # Handle Excel serial dates
        if df["week_ending"].isnull().any():
            serial = pd.to_numeric(
                self.df_weekly_region["week_ending"], errors="coerce"
            )
            origin = datetime(1899, 12, 30)
            mask = df["week_ending"].isnull() & serial.notna()
            df.loc[mask, "week_ending"] = serial[mask].apply(
                lambda x: origin + timedelta(days=int(x))
            )

        # Coerce numeric columns
        numeric_cols = [c for c in df.columns if c != "week_ending"]
        null_count_before = df[numeric_cols].isnull().sum().sum()

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Fill remaining nulls with linear interpolation
        null_count_after_coerce = df[numeric_cols].isnull().sum().sum()
        df[numeric_cols] = df[numeric_cols].interpolate(method="linear", limit_direction="both")
        null_count_after_fill = df[numeric_cols].isnull().sum().sum()

        self.stats["nulls_filled"] = int(null_count_after_coerce - null_count_after_fill)

        # Sort by date
        df = df.sort_values("week_ending").reset_index(drop=True)
        self.df_weekly_region = df

        # Clean site type data similarly
        if self.df_weekly_site is not None:
            st = self.df_weekly_site
            st["week_ending"] = pd.to_datetime(st["week_ending"], errors="coerce")
            for col in SITE_TYPES:
                if col in st.columns:
                    st[col] = pd.to_numeric(st[col], errors="coerce")
            st = st.sort_values("week_ending").reset_index(drop=True)
            st[SITE_TYPES] = st[SITE_TYPES].interpolate(
                method="linear", limit_direction="both"
            )
            self.df_weekly_site = st

        logger.info(
            f"  Clean complete: {self.stats['nulls_filled']} nulls interpolated, "
            f"{removed} duplicates removed"
        )
        return self

    # ── 4. TRANSFORM ─────────────────────────────────────────────────
    def transform(self) -> "FootfallETL":
        """
        Add derived columns, rolling averages, and reshape to long format.
        """
        logger.info("STAGE 4: Transforming data …")
        df = self.df_weekly_region

        # ── Temporal columns ──
        df["year"] = df["week_ending"].dt.year
        df["month"] = df["week_ending"].dt.month
        df["week_number"] = df["week_ending"].dt.isocalendar().week.astype(int)
        df["quarter"] = df["week_ending"].dt.quarter
        df["season"] = df["month"].map(SEASON_MAP)
        df["is_holiday_week"] = df["week_number"].isin([49, 50, 51, 52, 1])

        # ── Performance columns per region ──
        region_cols = [c for c in REGIONS if c in df.columns]
        for col in region_cols:
            df[f"{col}_above_baseline"] = df[col] > 100
            if "UK_total" in df.columns and col != "UK_total":
                df[f"{col}_vs_uk"] = round(df[col] - df["UK_total"], 2)

            # Rolling averages
            df[f"{col}_rolling_4w"] = (
                df[col].rolling(window=4, min_periods=1).mean().round(2)
            )
            df[f"{col}_rolling_12w"] = (
                df[col].rolling(window=12, min_periods=1).mean().round(2)
            )

            # Year-over-year change (52-week lag)
            if len(df) > 52:
                df[f"{col}_yoy"] = round(df[col] - df[col].shift(52), 2)
            else:
                df[f"{col}_yoy"] = np.nan

        # ── Transform site type data ──
        if self.df_weekly_site is not None:
            st = self.df_weekly_site
            st["year"] = st["week_ending"].dt.year
            st["month"] = st["week_ending"].dt.month
            st["week_number"] = st["week_ending"].dt.isocalendar().week.astype(int)
            st["quarter"] = st["week_ending"].dt.quarter
            st["season"] = st["month"].map(SEASON_MAP)
            st["is_holiday_week"] = st["week_number"].isin([49, 50, 51, 52, 1])
            for col in SITE_TYPES:
                if col in st.columns:
                    st[f"{col}_above_baseline"] = st[col] > 100
                    st[f"{col}_rolling_4w"] = (
                        st[col].rolling(window=4, min_periods=1).mean().round(2)
                    )
                    st[f"{col}_rolling_12w"] = (
                        st[col].rolling(window=12, min_periods=1).mean().round(2)
                    )
            self.df_weekly_site = st

        # ── Melt to long (tidy) format ──
        id_cols = ["week_ending", "year", "month", "week_number", "quarter",
                   "season", "is_holiday_week"]
        value_cols = [c for c in region_cols]
        self.df_long = df.melt(
            id_vars=id_cols,
            value_vars=value_cols,
            var_name="region",
            value_name="footfall_index",
        )
        self.df_long["region_label"] = self.df_long["region"].map(REGION_LABELS)
        self.df_long["above_baseline"] = self.df_long["footfall_index"] > 100

        self.stats["rows_final"] = len(df)
        logger.info(
            f"  Transform complete: {len(df.columns)} columns in wide format, "
            f"{len(self.df_long)} rows in long format"
        )
        return self

    # ── 5. EXPORT ────────────────────────────────────────────────────
    def export(self) -> "FootfallETL":
        """
        Export cleaned data to CSV and JSON.
        """
        logger.info("STAGE 5: Exporting data …")
        os.makedirs(self.output_dir, exist_ok=True)

        # Wide format
        path_wide = os.path.join(self.output_dir, "cleaned_footfall.csv")
        self.df_weekly_region.to_csv(path_wide, index=False)
        logger.info(f"  → {path_wide}")

        # Long format
        path_long = os.path.join(self.output_dir, "footfall_long.csv")
        self.df_long.to_csv(path_long, index=False)
        logger.info(f"  → {path_long}")

        # Site type data
        if self.df_weekly_site is not None:
            path_site = os.path.join(self.output_dir, "cleaned_footfall_sites.csv")
            self.df_weekly_site.to_csv(path_site, index=False)
            logger.info(f"  → {path_site}")

        # Summary JSON
        summary = self._build_summary()
        path_json = os.path.join(self.output_dir, "footfall_summary.json")
        with open(path_json, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"  → {path_json}")

        # Validation log
        if self.validation_log:
            path_log = os.path.join(self.output_dir, "validation_log.txt")
            with open(path_log, "w") as f:
                f.write("DATA VALIDATION LOG\n")
                f.write(f"Generated: {datetime.now().isoformat()}\n")
                f.write("=" * 50 + "\n")
                for entry in self.validation_log:
                    f.write(entry + "\n")
            logger.info(f"  → {path_log}")

        return self

    def _build_summary(self) -> Dict:
        """Build summary statistics dictionary for JSON export."""
        df = self.df_weekly_region
        region_cols = [c for c in REGIONS if c in df.columns]

        region_stats = {}
        for col in region_cols:
            vals = df[col].dropna()
            region_stats[REGION_LABELS.get(col, col)] = {
                "mean": round(float(vals.mean()), 2),
                "median": round(float(vals.median()), 2),
                "std": round(float(vals.std()), 2),
                "min": round(float(vals.min()), 2),
                "max": round(float(vals.max()), 2),
                "latest": round(float(vals.iloc[-1]), 2),
                "above_baseline_pct": round(
                    float((vals > 100).mean() * 100), 1
                ),
            }

        site_stats = {}
        if self.df_weekly_site is not None:
            for col in SITE_TYPES:
                if col in self.df_weekly_site.columns:
                    vals = self.df_weekly_site[col].dropna()
                    site_stats[SITE_LABELS.get(col, col)] = {
                        "mean": round(float(vals.mean()), 2),
                        "median": round(float(vals.median()), 2),
                        "std": round(float(vals.std()), 2),
                        "min": round(float(vals.min()), 2),
                        "max": round(float(vals.max()), 2),
                        "latest": round(float(vals.iloc[-1]), 2),
                    }

        return {
            "generated_at": datetime.now().isoformat(),
            "data_source": "ONS / BT Active Intelligence",
            "coverage": {
                "start": str(df["week_ending"].min().date()),
                "end": str(df["week_ending"].max().date()),
                "weeks": len(df),
                "regions": len(region_cols),
                "site_types": len(SITE_TYPES),
            },
            "pipeline_stats": self.stats,
            "regions": region_stats,
            "site_types": site_stats,
        }

    # ── RUN ───────────────────────────────────────────────────────────
    def run(self) -> "FootfallETL":
        """Execute the full ETL pipeline."""
        t0 = time.time()
        logger.info("=" * 60)
        logger.info("  UK RETAIL FOOTFALL ETL PIPELINE")
        logger.info("=" * 60)

        self.load_raw().validate().clean().transform().export()

        elapsed = time.time() - t0
        logger.info("=" * 60)
        logger.info("  PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Rows processed:    {self.stats['rows_raw']}")
        logger.info(f"  Rows exported:     {self.stats['rows_final']}")
        logger.info(f"  Nulls filled:      {self.stats['nulls_filled']}")
        logger.info(f"  Anomalies flagged: {self.stats['anomalies_flagged']}")
        logger.info(f"  Duplicates removed:{self.stats['duplicates_removed']}")
        logger.info(f"  Execution time:    {elapsed:.2f}s")
        logger.info("=" * 60)
        return self


# ── CLI Entry Point ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="UK Retail Footfall ETL Pipeline"
    )
    parser.add_argument(
        "--input", "-i", type=str, default=None,
        help="Path to raw data file (Excel/CSV). If omitted, generates synthetic data."
    )
    parser.add_argument(
        "--output", "-o", type=str, default="data",
        help="Output directory for cleaned data (default: data/)"
    )
    args = parser.parse_args()

    pipeline = FootfallETL(input_path=args.input, output_dir=args.output)
    pipeline.run()


if __name__ == "__main__":
    main()
