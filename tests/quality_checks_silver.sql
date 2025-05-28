/*
===============================================================================
Integrity Verifications
===============================================================================
Overview:
    These queries validate that the Silver layer is accurate, complete, and 
    formatted correctly. They cover checks for:
      - Missing or repeated primary keys.
      - Extraneous whitespace in text columns.
      - Value conformity and consistency.
      - Date fields within acceptable boundaries.
      - Logical coherence between related measures.

When to Execute:
    - Immediately after loading data into the Silver schema.
    - Investigate and fix any deviations reported.
===============================================================================
*/

-- ====================================================================
-- silver.crm_cust_info
-- ====================================================================
-- 1) Detect NULL or duplicate customer IDs
--    Expectation: no rows returned
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2) Find unwanted spaces in the customer key
--    Expectation: no rows returned
SELECT 
    cst_key
FROM silver.crm_cust_info
WHERE cst_key <> TRIM(cst_key);

-- 3) Review all distinct marital status values
SELECT DISTINCT 
    cst_marital_status
FROM silver.crm_cust_info;


-- ====================================================================
-- silver.crm_prd_info
-- ====================================================================
-- 1) Detect NULL or duplicate product IDs
--    Expectation: no rows returned
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2) Find unwanted spaces in product names
--    Expectation: no rows returned
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- 3) Identify missing or negative costs
--    Expectation: no rows returned
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- 4) Review all distinct product line codes
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;

-- 5) Check for start dates occurring after end dates
--    Expectation: no rows returned
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ====================================================================
-- silver.crm_sales_details
-- ====================================================================
-- 1) Validate due dates fall within realistic range
--    Expectation: no rows returned
SELECT 
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt < '1900-01-01' 
   OR sls_due_dt > '2050-01-01';

-- 2) Ensure order date is not later than ship or due dates
--    Expectation: no rows returned
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- 3) Confirm sales amount equals quantity × price
--    Expectation: no rows returned
SELECT DISTINCT 
    sls_sales, 
    sls_quantity, 
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <> sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;


-- ====================================================================
-- silver.erp_cust_az12
-- ====================================================================
-- 1) Highlight birthdates outside plausible bounds
--    Expectation: no rows returned
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURRENT_DATE;

-- 2) Review all distinct gender entries
SELECT DISTINCT 
    gen
FROM silver.erp_cust_az12;


-- ====================================================================
-- silver.erp_loc_a101
-- ====================================================================
-- Review all distinct country codes
SELECT DISTINCT 
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;


-- ====================================================================
-- silver.erp_px_cat_g1v2
-- ====================================================================
-- 1) Find stray spaces in category fields
--    Expectation: no rows returned
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat         <> TRIM(cat)
   OR subcat      <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);

-- 2) Review all distinct maintenance categories
SELECT DISTINCT 
    maintenance
FROM silver.erp_px_cat_g1v2;
