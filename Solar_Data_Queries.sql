CREATE DATABASE IF NOT EXISTS solar_energy_db;
USE solar_energy_db;

SET GLOBAL local_infile = 1;

CREATE TABLE IF NOT EXISTS solar_installations (
    installation_date DATE,
    pv_system_size_dc DECIMAL(12,2),
    total_installed_price DECIMAL(12,2),
    rebate_or_grant DECIMAL(12,2),
    customer_segment VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    utility_service_territory VARCHAR(255),
    third_party_owned VARCHAR(50),
    installer_name VARCHAR(255),
    tracking VARCHAR(50),
    ground_mounted VARCHAR(50),
    azimuth_1 DECIMAL(10,2),
    tilt_1 DECIMAL(10,2),
    module_manufacturer_1 VARCHAR(255),
    module_model_1 VARCHAR(255),
    module_quantity_1 INT,
    technology_module_1 VARCHAR(100),
    efficiency_module_1 DECIMAL(10,2),
    inverter_manufacturer_1 VARCHAR(255),
    inverter_model_1 VARCHAR(255),
    output_capacity_inverter_1 DECIMAL(12,2),
    inverter_loading_ratio DECIMAL(10,2),
    battery_rated_capacity_kwh DECIMAL(12,2),
    price_per_watt DECIMAL(10,2)
);

TRUNCATE TABLE solar_installations;

LOAD DATA LOCAL INFILE '/Users/shraddhapatel/Desktop/Saylifile'
INTO TABLE solar_installations
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    @installation_date,
    @pv_system_size_dc,
    @total_installed_price,
    @rebate_or_grant,
    customer_segment,
    state,
    zip_code,
    utility_service_territory,
    third_party_owned,
    installer_name,
    @tracking,
    @ground_mounted,
    @azimuth_1,
    @tilt_1,
    module_manufacturer_1,
    module_model_1,
    @module_quantity_1,
    technology_module_1,
    @efficiency_module_1,
    inverter_manufacturer_1,
    inverter_model_1,
    @output_capacity_inverter_1,
    @inverter_loading_ratio,
    @battery_rated_capacity_kwh,
    @price_per_watt
)
SET
installation_date = STR_TO_DATE(NULLIF(@installation_date, ''), '%d-%m-%Y'),
pv_system_size_dc = NULLIF(REPLACE(REPLACE(@pv_system_size_dc, ',', ''), '$', ''), ''),
total_installed_price = NULLIF(REPLACE(REPLACE(@total_installed_price, ',', ''), '$', ''), ''),
rebate_or_grant = NULLIF(REPLACE(REPLACE(@rebate_or_grant, ',', ''), '$', ''), ''),
tracking = NULLIF(@tracking, ''),
ground_mounted = NULLIF(@ground_mounted, ''),
azimuth_1 = NULLIF(@azimuth_1, ''),
tilt_1 = NULLIF(@tilt_1, ''),
module_quantity_1 = NULLIF(@module_quantity_1, ''),
efficiency_module_1 = NULLIF(REPLACE(@efficiency_module_1, '%', ''), ''),
output_capacity_inverter_1 = NULLIF(@output_capacity_inverter_1, ''),
inverter_loading_ratio = NULLIF(@inverter_loading_ratio, ''),
battery_rated_capacity_kwh = NULLIF(@battery_rated_capacity_kwh, ''),
price_per_watt = NULLIF(REPLACE(REPLACE(@price_per_watt, ',', ''), '$', ''), '');

SELECT COUNT(*) AS total_rows
FROM solar_installations;

SELECT *
FROM solar_installations
LIMIT 10;


-- ============================================================
-- ANALYTICS QUERIES
-- ============================================================

-- -----------------------------------------------
-- Q1: Total Installations by State
-- -----------------------------------------------
SELECT
    state,
    COUNT(*)                                             AS total_installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_system_size_kw,
    ROUND(SUM(pv_system_size_dc), 2)                     AS total_capacity_kw,
    ROUND(SUM(pv_system_size_dc) / 1000, 3)              AS total_capacity_mw,
    ROUND(AVG(total_installed_price), 0)                 AS avg_install_price,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt
FROM solar_installations
GROUP BY state
ORDER BY total_installations DESC;

-- -----------------------------------------------
-- Q2: Installations by Year
-- -----------------------------------------------
SELECT
    YEAR(installation_date)                              AS install_year,
    COUNT(*)                                             AS installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(SUM(pv_system_size_dc), 2)                     AS total_kw_installed,
    ROUND(AVG(total_installed_price), 0)                 AS avg_price,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt,
    SUM(CASE WHEN battery_rated_capacity_kwh > 0 THEN 1 ELSE 0 END) AS with_battery
FROM solar_installations
WHERE installation_date IS NOT NULL
GROUP BY install_year
ORDER BY install_year;

-- -----------------------------------------------
-- Q3: Customer Segment Analysis
-- -----------------------------------------------
SELECT
    customer_segment,
    COUNT(*)                                             AS installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(AVG(total_installed_price), 0)                 AS avg_price,
    ROUND(AVG(rebate_or_grant), 0)                       AS avg_rebate,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt,
    ROUND(AVG(rebate_or_grant) /
          NULLIF(AVG(total_installed_price), 0) * 100, 2) AS rebate_pct
FROM solar_installations
WHERE customer_segment IS NOT NULL
GROUP BY customer_segment
ORDER BY installations DESC;

-- -----------------------------------------------
-- Q4: Top 10 States by Total Capacity Installed
-- -----------------------------------------------
SELECT
    state,
    COUNT(*)                                             AS installations,
    ROUND(SUM(pv_system_size_dc), 2)                     AS total_kw,
    ROUND(SUM(pv_system_size_dc) / 1000, 3)              AS total_mw,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_w,
    ROUND(SUM(total_installed_price), 0)                 AS total_spend,
    ROUND(SUM(rebate_or_grant), 0)                       AS total_rebates
FROM solar_installations
GROUP BY state
ORDER BY total_kw DESC
LIMIT 10;

-- -----------------------------------------------
-- Q5: Price Per Watt Trend Over Time
-- -----------------------------------------------
SELECT
    YEAR(installation_date)                              AS yr,
    COUNT(*)                                             AS installations,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt,
    ROUND(MIN(price_per_watt), 4)                        AS min_price_per_watt,
    ROUND(MAX(price_per_watt), 4)                        AS max_price_per_watt,
    ROUND(AVG(total_installed_price), 0)                 AS avg_total_price
FROM solar_installations
WHERE installation_date IS NOT NULL
  AND price_per_watt   IS NOT NULL
  AND price_per_watt > 0
GROUP BY yr
ORDER BY yr;

-- -----------------------------------------------
-- Q6: Third-Party Owned vs Customer Owned
-- -----------------------------------------------
SELECT
    CASE third_party_owned
        WHEN 1 THEN 'Third-Party Owned (Lease/PPA)'
        WHEN 0 THEN 'Customer Owned'
        ELSE 'Unknown'
    END                                                  AS ownership_type,
    COUNT(*)                                             AS installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(AVG(total_installed_price), 0)                 AS avg_price,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt
FROM solar_installations
GROUP BY third_party_owned
ORDER BY installations DESC;

-- -----------------------------------------------
-- Q7: Solar Module Technology Breakdown
-- -----------------------------------------------
SELECT
    technology_module_1                                  AS technology,
    COUNT(*)                                             AS installations,
    ROUND(AVG(efficiency_module_1) * 100, 3)             AS avg_efficiency_pct,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_system_size_kw,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt
FROM solar_installations
WHERE technology_module_1 IS NOT NULL
GROUP BY technology_module_1
ORDER BY installations DESC;

-- -----------------------------------------------
-- Q8: Top Module Manufacturers
-- -----------------------------------------------
SELECT
    module_manufacturer_1                                AS manufacturer,
    COUNT(*)                                             AS installations,
    ROUND(AVG(efficiency_module_1) * 100, 3)             AS avg_efficiency_pct,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_system_size_kw,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt
FROM solar_installations
WHERE module_manufacturer_1 IS NOT NULL
  AND module_manufacturer_1 != 'no match'
GROUP BY module_manufacturer_1
ORDER BY installations DESC
LIMIT 15;

-- -----------------------------------------------
-- Q9: Top Inverter Manufacturers
-- -----------------------------------------------
SELECT
    inverter_manufacturer_1                              AS manufacturer,
    COUNT(*)                                             AS installations,
    ROUND(AVG(output_capacity_inverter_1), 4)            AS avg_inverter_kw,
    ROUND(AVG(inverter_loading_ratio), 4)                AS avg_loading_ratio
FROM solar_installations
WHERE inverter_manufacturer_1 IS NOT NULL
  AND inverter_manufacturer_1 != 'no match'
GROUP BY inverter_manufacturer_1
ORDER BY installations DESC
LIMIT 15;

-- -----------------------------------------------
-- Q10: Ground Mounted vs Rooftop
-- -----------------------------------------------
SELECT
    CASE ground_mounted
        WHEN 1 THEN 'Ground Mounted'
        WHEN 0 THEN 'Rooftop'
        ELSE 'Unknown'
    END                                                  AS mount_type,
    COUNT(*)                                             AS installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(AVG(total_installed_price), 0)                 AS avg_price,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt,
    ROUND(AVG(tilt_1), 2)                                AS avg_tilt_deg
FROM solar_installations
GROUP BY ground_mounted
ORDER BY installations DESC;

-- -----------------------------------------------
-- Q11: Battery Storage Adoption
-- -----------------------------------------------
SELECT
    CASE
        WHEN battery_rated_capacity_kwh > 0 THEN 'With Battery Storage'
        ELSE 'No Battery Storage'
    END                                                  AS battery_status,
    COUNT(*)                                             AS installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_system_kw,
    ROUND(AVG(battery_rated_capacity_kwh), 3)            AS avg_battery_kwh,
    ROUND(AVG(total_installed_price), 0)                 AS avg_price
FROM solar_installations
GROUP BY battery_status
ORDER BY installations DESC;

-- -----------------------------------------------
-- Q12: Rebate / Incentive Analysis by State
-- -----------------------------------------------
SELECT
    state,
    COUNT(*)                                             AS installations,
    ROUND(AVG(rebate_or_grant), 0)                       AS avg_rebate,
    ROUND(SUM(rebate_or_grant), 0)                       AS total_rebates,
    ROUND(AVG(total_installed_price), 0)                 AS avg_install_price,
    ROUND(AVG(rebate_or_grant) /
          NULLIF(AVG(total_installed_price), 0) * 100, 2) AS rebate_coverage_pct
FROM solar_installations
WHERE rebate_or_grant > 0
GROUP BY state
ORDER BY avg_rebate DESC
LIMIT 15;

-- -----------------------------------------------
-- Q13: Top Installers by Volume
-- -----------------------------------------------
SELECT
    installer_name,
    COUNT(*)                                             AS installations,
    COUNT(DISTINCT state)                                AS states_active,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_per_watt
FROM solar_installations
WHERE installer_name IS NOT NULL
  AND installer_name != 'no match'
GROUP BY installer_name
ORDER BY installations DESC
LIMIT 15;

-- -----------------------------------------------
-- Q14: Monthly Installation Trend
-- -----------------------------------------------
SELECT
    YEAR(installation_date)                              AS yr,
    MONTH(installation_date)                             AS mo,
    MONTHNAME(installation_date)                         AS month_name,
    COUNT(*)                                             AS installations,
    ROUND(AVG(pv_system_size_dc), 3)                     AS avg_size_kw,
    ROUND(AVG(price_per_watt), 4)                        AS avg_price_w
FROM solar_installations
WHERE installation_date IS NOT NULL
GROUP BY yr, mo, month_name
ORDER BY yr, mo;

-- -----------------------------------------------
-- Q15: Efficiency Distribution by Technology
-- -----------------------------------------------
SELECT
    technology_module_1,
    ROUND(MIN(efficiency_module_1) * 100, 2)             AS min_eff_pct,
    ROUND(AVG(efficiency_module_1) * 100, 2)             AS avg_eff_pct,
    ROUND(MAX(efficiency_module_1) * 100, 2)             AS max_eff_pct,
    COUNT(*)                                             AS count
FROM solar_installations
WHERE efficiency_module_1 IS NOT NULL
  AND efficiency_module_1 > 0
  AND technology_module_1 IS NOT NULL
GROUP BY technology_module_1
ORDER BY avg_eff_pct DESC;

-- ============================================================
-- VIEWS FOR DASHBOARD
-- ============================================================

CREATE OR REPLACE VIEW v_state_summary AS
SELECT
    state,
    COUNT(*)                                AS total_installations,
    ROUND(SUM(pv_system_size_dc), 2)        AS total_kw,
    ROUND(AVG(pv_system_size_dc), 3)        AS avg_size_kw,
    ROUND(AVG(price_per_watt), 4)           AS avg_price_per_watt,
    ROUND(AVG(total_installed_price), 0)    AS avg_install_price,
    ROUND(SUM(rebate_or_grant), 0)          AS total_rebates,
    ROUND(AVG(rebate_or_grant), 0)          AS avg_rebate,
    SUM(third_party_owned)                  AS third_party_count,
    SUM(ground_mounted)                     AS ground_mounted_count,
    SUM(CASE WHEN battery_rated_capacity_kwh > 0 THEN 1 ELSE 0 END) AS battery_count
FROM solar_installations
GROUP BY state;

CREATE OR REPLACE VIEW v_yearly_trend AS
SELECT
    YEAR(installation_date)                 AS yr,
    COUNT(*)                                AS installations,
    ROUND(AVG(pv_system_size_dc), 3)        AS avg_size_kw,
    ROUND(AVG(price_per_watt), 4)           AS avg_price_per_watt,
    ROUND(AVG(total_installed_price), 0)    AS avg_total_price,
    ROUND(SUM(pv_system_size_dc), 2)        AS total_kw
FROM solar_installations
WHERE installation_date IS NOT NULL
GROUP BY yr
ORDER BY yr;

CREATE OR REPLACE VIEW v_segment_summary AS
SELECT
    customer_segment,
    state,
    COUNT(*)                                AS installations,
    ROUND(AVG(pv_system_size_dc), 3)        AS avg_size_kw,
    ROUND(AVG(price_per_watt), 4)           AS avg_price_w,
    ROUND(AVG(rebate_or_grant), 0)          AS avg_rebate
FROM solar_installations
WHERE customer_segment IS NOT NULL
GROUP BY customer_segment, state;
