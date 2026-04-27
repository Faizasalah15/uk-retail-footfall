# UK Retail Footfall Intelligence Platform

**Turning 90 weeks of national mobility data into regional investment decisions, seasonal staffing plans, and £-impact estimates — so retail leaders act on evidence, not intuition.**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![SQL](https://img.shields.io/badge/SQL-Analytical_Queries-336791?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Dashboard](https://img.shields.io/badge/Dashboard-Interactive-00B4D8?style=flat-square&logo=chartdotjs&logoColor=white)](https://faizasalah15.github.io/uk-retail-footfall/)
[![License: MIT](https://img.shields.io/badge/License-MIT-grey?style=flat-square)](LICENSE)

🌐 **[Launch Live Dashboard →](https://faizasalah15.github.io/uk-retail-footfall/)**

---

## The Business Problem

UK retail generates over **£400 billion annually**, yet most organisations still make location, staffing, and investment decisions based on lagging indicators or gut feel. Regional footfall — the strongest leading signal of consumer demand — is publicly available but rarely translated into actionable intelligence.

**The result:** retailers over-staff quiet periods, under-invest in recovering regions, and miss seasonal swings worth 30–55% of baseline trading.

---

## What I Built

An end-to-end analytics platform that ingests ONS/BT Active Intelligence footfall data (24 million UK mobile devices, 14 regions, 3 site types) and outputs:

- **Regional benchmarking** — which regions lead, lag, or are closing the gap
- **Seasonal planning calendars** — week-by-week traffic forecasts for the next quarter
- **£-impact estimation** — live sliders translating index changes into revenue and margin
- **Investment scoring** — composite 6-dimension grades ranking every region
- **Risk alerts** — automated RAG flags for regions showing sustained decline

Everything is delivered through a **fully interactive dashboard** — no server, no login, opens in any browser.

---

## Key Insights & Business Impact

| Insight | So What? |
|---------|----------|
| **London consistently trades 5–8 pts above UK average** (111.4 index) | Confirms premium pricing power; justifies higher rental commitments in the capital |
| **District & Local Centres outperform Town Centres** (~108 vs ~99) | Neighbourhood retail is more resilient — a signal for property investors and franchisees |
| **Christmas creates a 50+ point seasonal swing** (peaks at 130–155, January troughs at 85–92) | Staffing and inventory plans need to flex by ~50% between December and January |
| **3 distinct regional clusters** — Metro Leaders, Stable Mid-Tier, Recovering | Investment strategy should differ by cluster, not by individual region |
| **6 of the next 12 weeks forecast above baseline** | Near-term outlook supports cautious expansion in top-performing regions |
| **Automated alerts flag regions with 6+ weeks below baseline** | Early warning enables intervention before quarterly reviews surface the problem |

---

## Skills Demonstrated

### Business & Analytical
- Stakeholder-ready dashboard design (executive summary, RAG alerts, scenario modelling)
- Translating raw data into commercial £-impact estimates
- Regional benchmarking against dual baselines (historical + national average)
- Seasonal decomposition and forward planning
- Risk scoring with composite weighted dimensions
- Clear, actionable insight communication

### Technical
- End-to-end **Python ETL pipeline** (extract, validate, clean, transform, export)
- **Statistical analysis** — OLS regression, z-tests, Pearson correlation, anomaly detection
- **Time-series forecasting** — Linear, EWMA, and weighted ensemble with 95% confidence intervals
- **Unsupervised ML** — K-Means clustering with PCA dimensionality reduction and silhouette evaluation
- **SQL** — 15 analytical queries including window functions, CTEs, and materialised views
- **Interactive dashboard** — Chart.js with 6 dynamic charts, real-time computation, responsive design
- **Data visualisation** — 8 publication-quality static figures (matplotlib/seaborn)

---

## Tools & Technologies

| Category | Tools |
|----------|-------|
| **Languages** | Python 3.10+, SQL, JavaScript, HTML/CSS |
| **Data & Analysis** | pandas, NumPy, SciPy |
| **Machine Learning** | scikit-learn (K-Means, PCA) |
| **Visualisation** | matplotlib, seaborn, Chart.js 4.4 |
| **Database** | PostgreSQL / SQLite compatible |
| **Deployment** | GitHub Pages |

---

## Sample Outputs

| Regional Trends | Seasonal Heatmap |
|:-:|:-:|
| ![Regional Trends](figures/regional_trends.png) | ![Seasonal Heatmap](figures/seasonal_heatmap.png) |

| Ensemble Forecast | Correlation Matrix |
|:-:|:-:|
| ![Forecast](figures/forecast_chart.png) | ![Correlation](figures/correlation_heatmap.png) |

| Site Type Comparison | Regional Rankings |
|:-:|:-:|
| ![Site Types](figures/site_type_comparison.png) | ![Rankings](figures/regional_ranking_bars.png) |

| Cluster Analysis (PCA) | Anomaly Detection |
|:-:|:-:|
| ![Clusters](figures/cluster_scatter.png) | ![Anomalies](figures/anomaly_timeline.png) |

---

## Project Structure

```
uk-retail-footfall/
├── run_all.py              # Orchestrator — runs full pipeline in sequence
├── data_pipeline.py        # ETL: extract → validate → clean → transform → export
├── analysis.py             # Statistical analysis (7 methods)
├── forecasting.py          # 3-model forecasting with walk-forward validation
├── segmentation.py         # K-Means clustering + PCA
├── visualisations.py       # 8 static publication-quality charts
├── sql_queries.sql         # 15 analytical SQL queries + views
├── index.html              # Interactive dashboard (10 sections)
├── data/                   # Cleaned datasets (generated)
├── reports/                # Analysis outputs (generated)
└── figures/                # Chart images (generated)
```

### Quick Start

```bash
git clone https://github.com/Faizasalah15/uk-retail-footfall.git
cd uk-retail-footfall
pip install -r requirements.txt
python run_all.py
```

The dashboard requires no installation — open `index.html` in any modern browser.

---

## Why This Project Matters

This is not a tutorial exercise. It mirrors the kind of work done by analytics teams at consultancies and retail HQs:

- **Deloitte / KPMG** use footfall benchmarking for retail due diligence and high street health assessments
- **Tesco / M&S / John Lewis** rely on regional demand signals for store investment and seasonal workforce planning
- **Property developers** use volatility-adjusted performance scores to assess location risk before committing capital
- **Local authorities** track footfall recovery to measure the effectiveness of town centre regeneration schemes

The platform demonstrates the ability to take a messy, real-world dataset and deliver a polished decision-support tool — the core skill set of a Business Analyst or Data Analyst.

---

## Limitations & Next Steps

- Data is synthetically modelled to mirror official ONS seasonal and regional patterns (not the live feed)
- Forecasting models are intentionally simple — appropriate for the scope and transparent in methodology
- Future: live ONS API integration, ARIMA/Prophet forecasting, geospatial mapping, automated PDF reporting

---

## Author

**Ashima Faiza Salahudeen Alimajasmin**

[LinkedIn](https://www.linkedin.com/in/ashima-faiza) · [GitHub](https://github.com/Faizasalah15)

---

<p align="center">
  <sub>Data source: ONS / BT Active Intelligence — Crown Copyright, Open Government Licence v3.0</sub>
</p>
