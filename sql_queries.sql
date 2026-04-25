-- =============================================================
-- UK RETAIL FOOTFALL INTELLIGENCE PLATFORM — SQL ANALYSIS
-- =============================================================
-- Database: PostgreSQL / SQLite compatible
-- Data Source: ONS / BT Active Intelligence
-- Coverage: Jul 2024 – Apr 2026 | 14 regions | 3 site types
-- Index: 2023 average = 100
-- =============================================================

-- ── SCHEMA ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS footfall_weekly (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,  -- SERIAL for PostgreSQL
    week_ending     DATE NOT NULL,
    region          VARCHAR(50) NOT NULL,
    site_type       VARCHAR(50),
    footfall_index  DECIMAL(8,4),
    year            INT,
    month           INT,
    week_number     INT,
    quarter         INT,
    season          VARCHAR(10),
    is_holiday_week BOOLEAN DEFAULT FALSE,
    above_baseline  BOOLEAN DEFAULT FALSE,
    rolling_4w_avg  DECIMAL(8,4),
    rolling_12w_avg DECIMAL(8,4)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fw_region ON footfall_weekly(region);
CREATE INDEX IF NOT EXISTS idx_fw_week ON footfall_weekly(week_ending);
CREATE INDEX IF NOT EXISTS idx_fw_region_week ON footfall_weekly(region, week_ending);
CREATE INDEX IF NOT EXISTS idx_fw_season ON footfall_weekly(season);
CREATE INDEX IF NOT EXISTS idx_fw_month ON footfall_weekly(month);


-- =============================================================
-- Q1: Latest index per region
-- Purpose: Snapshot of current performance across all regions
-- =============================================================
SELECT
    region,
    week_ending,
    footfall_index,
    CASE
        WHEN footfall_index > 100 THEN 'Above baseline'
        WHEN footfall_index = 100 THEN 'At baseline'
        ELSE 'Below baseline'
    END AS performance_status
FROM footfall_weekly
WHERE week_ending = (SELECT MAX(week_ending) FROM footfall_weekly)
ORDER BY footfall_index DESC;


-- =============================================================
-- Q2: Regional ranking by average index
-- Purpose: Identify consistently strong and weak regions
-- =============================================================
SELECT
    region,
    ROUND(AVG(footfall_index), 2) AS avg_index,
    ROUND(MIN(footfall_index), 2) AS min_index,
    ROUND(MAX(footfall_index), 2) AS max_index,
    ROUND(AVG(footfall_index) - 100, 2) AS vs_baseline,
    RANK() OVER (ORDER BY AVG(footfall_index) DESC) AS rank_position
FROM footfall_weekly
WHERE site_type IS NULL OR site_type = ''
GROUP BY region
ORDER BY avg_index DESC;


-- =============================================================
-- Q3: Best and worst week per region
-- Purpose: Identify peak and trough weeks for planning
-- =============================================================
SELECT
    region,
    MAX(footfall_index) AS peak_index,
    (SELECT week_ending FROM footfall_weekly fw2
     WHERE fw2.region = fw.region
     ORDER BY footfall_index DESC LIMIT 1) AS peak_week,
    MIN(footfall_index) AS trough_index,
    (SELECT week_ending FROM footfall_weekly fw3
     WHERE fw3.region = fw.region
     ORDER BY footfall_index ASC LIMIT 1) AS trough_week,
    ROUND(MAX(footfall_index) - MIN(footfall_index), 2) AS range_amplitude
FROM footfall_weekly fw
GROUP BY region
ORDER BY range_amplitude DESC;


-- =============================================================
-- Q4: Month-over-month change using LAG()
-- Purpose: Track monthly momentum for trend detection
-- =============================================================
SELECT
    region,
    year,
    month,
    ROUND(AVG(footfall_index), 2) AS monthly_avg,
    ROUND(AVG(footfall_index) - LAG(AVG(footfall_index))
        OVER (PARTITION BY region ORDER BY year, month), 2) AS mom_change,
    CASE
        WHEN AVG(footfall_index) > LAG(AVG(footfall_index))
            OVER (PARTITION BY region ORDER BY year, month)
        THEN '↑ Improving'
        ELSE '↓ Declining'
    END AS direction
FROM footfall_weekly
GROUP BY region, year, month
ORDER BY region, year, month;


-- =============================================================
-- Q5: Year-over-year change using LAG() with 52-week offset
-- Purpose: Like-for-like seasonal comparison
-- =============================================================
SELECT
    fw1.region,
    fw1.week_ending AS current_week,
    fw1.footfall_index AS current_index,
    fw2.footfall_index AS prior_year_index,
    ROUND(fw1.footfall_index - fw2.footfall_index, 2) AS yoy_change,
    ROUND(((fw1.footfall_index - fw2.footfall_index) /
           NULLIF(fw2.footfall_index, 0)) * 100, 2) AS yoy_pct_change
FROM footfall_weekly fw1
LEFT JOIN footfall_weekly fw2
    ON fw1.region = fw2.region
    AND fw2.week_ending = DATE(fw1.week_ending, '-364 days')
WHERE fw2.footfall_index IS NOT NULL
ORDER BY fw1.region, fw1.week_ending DESC;


-- =============================================================
-- Q6: Rolling 4-week average using window function
-- Purpose: Smooth weekly volatility for trend analysis
-- =============================================================
SELECT
    region,
    week_ending,
    footfall_index,
    ROUND(AVG(footfall_index) OVER (
        PARTITION BY region
        ORDER BY week_ending
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ), 2) AS calc_rolling_4w,
    ROUND(AVG(footfall_index) OVER (
        PARTITION BY region
        ORDER BY week_ending
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ), 2) AS calc_rolling_12w
FROM footfall_weekly
ORDER BY region, week_ending;


-- =============================================================
-- Q7: Christmas peak detection (week 49-52, index > 120)
-- Purpose: Quantify seasonal Christmas lift by region
-- =============================================================
SELECT
    region,
    year,
    week_number,
    week_ending,
    footfall_index,
    ROUND(footfall_index - 100, 2) AS above_baseline_pts,
    RANK() OVER (PARTITION BY year ORDER BY footfall_index DESC) AS xmas_rank
FROM footfall_weekly
WHERE week_number BETWEEN 49 AND 52
  AND footfall_index > 120
ORDER BY year, footfall_index DESC;


-- =============================================================
-- Q8: January slump detection (week 1-4, index < 95)
-- Purpose: Identify regions most affected by post-Christmas dip
-- =============================================================
SELECT
    region,
    year,
    week_number,
    footfall_index,
    ROUND(100 - footfall_index, 2) AS below_baseline_pts,
    RANK() OVER (PARTITION BY year ORDER BY footfall_index ASC) AS slump_rank
FROM footfall_weekly
WHERE week_number BETWEEN 1 AND 4
  AND footfall_index < 95
ORDER BY year, footfall_index ASC;


-- =============================================================
-- Q9: Regional performance vs UK total (correlated subquery)
-- Purpose: Benchmark each region against national average
-- =============================================================
SELECT
    fw.region,
    fw.week_ending,
    fw.footfall_index AS region_index,
    (SELECT footfall_index FROM footfall_weekly uk
     WHERE uk.region = 'UK Total'
       AND uk.week_ending = fw.week_ending
     LIMIT 1) AS uk_total_index,
    ROUND(fw.footfall_index - (
        SELECT footfall_index FROM footfall_weekly uk
        WHERE uk.region = 'UK Total'
          AND uk.week_ending = fw.week_ending
        LIMIT 1
    ), 2) AS vs_uk_total,
    CASE
        WHEN fw.footfall_index > (
            SELECT footfall_index FROM footfall_weekly uk
            WHERE uk.region = 'UK Total'
              AND uk.week_ending = fw.week_ending LIMIT 1)
        THEN 'Outperforming'
        ELSE 'Underperforming'
    END AS relative_performance
FROM footfall_weekly fw
WHERE fw.region != 'UK Total'
ORDER BY fw.week_ending DESC, vs_uk_total DESC;


-- =============================================================
-- Q10: Site type winner per month
-- Purpose: Track which site type leads each month
-- =============================================================
SELECT
    year,
    month,
    site_type,
    ROUND(AVG(footfall_index), 2) AS avg_index,
    RANK() OVER (PARTITION BY year, month ORDER BY AVG(footfall_index) DESC) AS monthly_rank
FROM footfall_weekly
WHERE site_type IS NOT NULL AND site_type != ''
GROUP BY year, month, site_type
ORDER BY year, month, monthly_rank;


-- =============================================================
-- Q11: Regions consistently above baseline (>80% of weeks)
-- Purpose: Identify reliable high-performance regions
-- =============================================================
SELECT
    region,
    COUNT(*) AS total_weeks,
    SUM(CASE WHEN footfall_index > 100 THEN 1 ELSE 0 END) AS weeks_above,
    ROUND(100.0 * SUM(CASE WHEN footfall_index > 100 THEN 1 ELSE 0 END)
        / COUNT(*), 1) AS pct_above_baseline,
    CASE
        WHEN 100.0 * SUM(CASE WHEN footfall_index > 100 THEN 1 ELSE 0 END)
            / COUNT(*) > 80 THEN '★ Consistent Performer'
        WHEN 100.0 * SUM(CASE WHEN footfall_index > 100 THEN 1 ELSE 0 END)
            / COUNT(*) > 50 THEN '● Mixed Performance'
        ELSE '▼ Below Average'
    END AS classification
FROM footfall_weekly
GROUP BY region
HAVING COUNT(*) >= 10
ORDER BY pct_above_baseline DESC;


-- =============================================================
-- Q12: Top 5 highest footfall weeks ever recorded
-- Purpose: Identify record-breaking retail periods
-- =============================================================
SELECT
    region,
    week_ending,
    footfall_index,
    season,
    week_number,
    CASE
        WHEN week_number BETWEEN 49 AND 52 THEN 'Christmas period'
        WHEN month BETWEEN 7 AND 8 THEN 'Summer peak'
        ELSE 'Other'
    END AS period_context
FROM footfall_weekly
ORDER BY footfall_index DESC
LIMIT 5;


-- =============================================================
-- Q13: Seasonal index calculation per week-of-year
-- Purpose: Compute average performance per week for forecasting
-- =============================================================
SELECT
    week_number,
    ROUND(AVG(footfall_index), 2) AS seasonal_index,
    ROUND(AVG(footfall_index) - 100, 2) AS vs_baseline,
    COUNT(*) AS sample_size,
    ROUND(MIN(footfall_index), 2) AS week_min,
    ROUND(MAX(footfall_index), 2) AS week_max,
    CASE
        WHEN AVG(footfall_index) > 115 THEN 'HIGH season'
        WHEN AVG(footfall_index) > 105 THEN 'MODERATE season'
        WHEN AVG(footfall_index) > 95 THEN 'LOW season'
        ELSE 'TROUGH season'
    END AS season_class
FROM footfall_weekly
WHERE region = 'UK Total'
GROUP BY week_number
ORDER BY week_number;


-- =============================================================
-- Q14: Cohort-style analysis — performance by quarter
-- Purpose: Compare quarters across years for strategic planning
-- =============================================================
SELECT
    region,
    year,
    quarter,
    ROUND(AVG(footfall_index), 2) AS q_avg,
    ROUND(MIN(footfall_index), 2) AS q_min,
    ROUND(MAX(footfall_index), 2) AS q_max,
    COUNT(*) AS weeks_in_quarter,
    ROUND(AVG(footfall_index) - LAG(AVG(footfall_index))
        OVER (PARTITION BY region, quarter ORDER BY year), 2) AS yoy_q_change
FROM footfall_weekly
GROUP BY region, year, quarter
ORDER BY region, year, quarter;


-- =============================================================
-- Q15: Executive dashboard query (CTE-based)
-- Purpose: Single query combining current state, trend, ranking
-- =============================================================
WITH latest AS (
    SELECT region, footfall_index AS current_index, week_ending
    FROM footfall_weekly
    WHERE week_ending = (SELECT MAX(week_ending) FROM footfall_weekly)
),
averages AS (
    SELECT region,
           ROUND(AVG(footfall_index), 2) AS avg_index,
           ROUND(MAX(footfall_index), 2) AS peak_index,
           ROUND(MIN(footfall_index), 2) AS trough_index
    FROM footfall_weekly
    GROUP BY region
),
trend AS (
    SELECT region,
           ROUND(AVG(footfall_index), 2) AS recent_avg
    FROM footfall_weekly
    WHERE week_ending >= DATE((SELECT MAX(week_ending) FROM footfall_weekly), '-84 days')
    GROUP BY region
),
ranked AS (
    SELECT region,
           RANK() OVER (ORDER BY AVG(footfall_index) DESC) AS overall_rank
    FROM footfall_weekly
    GROUP BY region
)
SELECT
    l.region,
    l.current_index,
    a.avg_index,
    a.peak_index,
    a.trough_index,
    t.recent_avg AS last_12w_avg,
    ROUND(l.current_index - a.avg_index, 2) AS vs_average,
    r.overall_rank,
    CASE
        WHEN l.current_index > a.avg_index AND t.recent_avg > a.avg_index THEN '🟢 Strong'
        WHEN l.current_index > 100 THEN '🟡 Moderate'
        ELSE '🔴 Weak'
    END AS health_status
FROM latest l
JOIN averages a ON l.region = a.region
JOIN trend t ON l.region = t.region
JOIN ranked r ON l.region = r.region
ORDER BY r.overall_rank;


-- =============================================================
-- BONUS: View — vw_regional_summary (latest week snapshot)
-- =============================================================
CREATE VIEW IF NOT EXISTS vw_regional_summary AS
SELECT
    region,
    week_ending,
    footfall_index AS current_index,
    rolling_4w_avg,
    rolling_12w_avg,
    above_baseline,
    season,
    RANK() OVER (ORDER BY footfall_index DESC) AS current_rank
FROM footfall_weekly
WHERE week_ending = (SELECT MAX(week_ending) FROM footfall_weekly);


-- =============================================================
-- BONUS: Stored procedure equivalent (SQLite function / PG proc)
-- get_regional_report(region_name)
-- Note: Written as parameterised query for portability
-- =============================================================
-- Usage: Replace 'London' with desired region name
-- PostgreSQL: CREATE FUNCTION get_regional_report(p_region TEXT)
--             RETURNS TABLE(...) AS $$ ... $$ LANGUAGE sql;

SELECT
    'REGIONAL REPORT' AS report_type,
    region,
    COUNT(*) AS total_weeks,
    ROUND(AVG(footfall_index), 2) AS mean_index,
    ROUND(MIN(footfall_index), 2) AS min_index,
    ROUND(MAX(footfall_index), 2) AS max_index,
    ROUND(AVG(CASE WHEN season = 'Summer' THEN footfall_index END), 2) AS summer_avg,
    ROUND(AVG(CASE WHEN season = 'Winter' THEN footfall_index END), 2) AS winter_avg,
    ROUND(AVG(CASE WHEN month = 12 THEN footfall_index END), 2) AS december_avg,
    ROUND(AVG(CASE WHEN month = 1 THEN footfall_index END), 2) AS january_avg,
    SUM(CASE WHEN above_baseline THEN 1 ELSE 0 END) AS weeks_above_100,
    ROUND(100.0 * SUM(CASE WHEN above_baseline THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_above
FROM footfall_weekly
WHERE region = 'London'  -- Replace with parameter
GROUP BY region;


-- =============================================================
-- BONUS: Index recommendations
-- =============================================================
-- Composite index for executive dashboard query:
-- This covers the most common WHERE + JOIN patterns
CREATE INDEX IF NOT EXISTS idx_fw_composite
    ON footfall_weekly(region, week_ending, footfall_index);
-- Why: The executive dashboard CTE joins on region and filters on
-- week_ending. A composite index avoids full table scans and
-- enables index-only scans for the aggregation queries.

-- Covering index for seasonal analysis:
CREATE INDEX IF NOT EXISTS idx_fw_seasonal
    ON footfall_weekly(week_number, month, season, footfall_index);
-- Why: Seasonal queries filter by week_number/month and aggregate
-- footfall_index. This covering index satisfies the query entirely
-- from the index without touching the heap.

-- Partial index for anomaly detection:
CREATE INDEX IF NOT EXISTS idx_fw_anomalies
    ON footfall_weekly(region, week_ending, footfall_index)
    WHERE footfall_index > 130 OR footfall_index < 85;
-- Why: Anomaly queries target extreme values. A partial index
-- keeps the index small while speeding up outlier detection.


-- =============================================================
-- END OF SQL ANALYSIS FILE
-- =============================================================
