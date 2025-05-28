-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS silver;

-- Translate T-SQL procedure to PostgreSQL PL/pgSQL
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '========================================';

    RAISE NOTICE '--- Loading CRM Tables ---';

    -- silver.crm_cust_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Truncating silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE 'Inserting into silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        trim(cst_firstname),
        trim(cst_lastname),
        CASE
          WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
          WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
          ELSE 'n/a'
        END,
        CASE
          WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
          WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
          ELSE 'n/a'
        END,
        cst_create_date
    FROM (
      SELECT
        *,
        row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
      FROM bronze.crm_cust_info
      WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration (crm_cust_info): % seconds',
                 EXTRACT(EPOCH FROM end_time - start_time);

    -- silver.crm_prd_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Truncating silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE 'Inserting into silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        replace(substring(prd_key from 1 for 5), '-', '_') AS cat_id,
        substring(prd_key from 7)                      AS prd_key,
        prd_nm,
        coalesce(prd_cost,0),
        CASE
          WHEN upper(trim(prd_line)) = 'M' THEN 'Mountain'
          WHEN upper(trim(prd_line)) = 'R' THEN 'Road'
          WHEN upper(trim(prd_line)) = 'S' THEN 'Other Sales'
          WHEN upper(trim(prd_line)) = 'T' THEN 'Touring'
          ELSE 'n/a'
        END,
        prd_start_dt::date,
        (lead(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
         ) - INTERVAL '1 day')::date
    FROM bronze.crm_prd_info;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration (crm_prd_info): % seconds',
                 EXTRACT(EPOCH FROM end_time - start_time);

    -- silver.crm_sales_details
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Truncating silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE 'Inserting into silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
          WHEN sls_order_dt = 0
            OR length(sls_order_dt::text) <> 8 THEN NULL
          ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
        END,
        CASE
          WHEN sls_ship_dt = 0
            OR length(sls_ship_dt::text) <> 8 THEN NULL
          ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
        END,
        CASE
          WHEN sls_due_dt = 0
            OR length(sls_due_dt::text) <> 8 THEN NULL
          ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
        END,
        CASE
          WHEN sls_sales IS NULL
            OR sls_sales <= 0
            OR sls_sales <> sls_quantity * abs(sls_price)
          THEN sls_quantity * abs(sls_price)
          ELSE sls_sales
        END,
        sls_quantity,
        CASE
          WHEN sls_price IS NULL OR sls_price <= 0
          THEN (sls_sales / NULLIF(sls_quantity,0))
          ELSE sls_price
        END
    FROM bronze.crm_sales_details;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration (crm_sales_details): % seconds',
                 EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE '--- Loading ERP Tables ---';

    -- silver.erp_cust_az12
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Truncating silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE 'Inserting into silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
          WHEN cid LIKE 'NAS%' THEN substring(cid from 4)
          ELSE cid
        END,
        CASE
          WHEN bdate > CURRENT_DATE THEN NULL
          ELSE bdate
        END,
        CASE
          WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
          WHEN upper(trim(gen)) IN ('M','MALE')   THEN 'Male'
          ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration (erp_cust_az12): % seconds',
                 EXTRACT(EPOCH FROM end_time - start_time);

    -- silver.erp_loc_a101
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Truncating silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE 'Inserting into silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        replace(cid, '-', ''),
        CASE
          WHEN trim(cntry) = 'DE'         THEN 'Germany'
          WHEN trim(cntry) IN ('US','USA') THEN 'United States'
          WHEN cntry IS NULL OR trim(cntry) = '' THEN 'n/a'
          ELSE trim(cntry)
        END
    FROM bronze.erp_loc_a101;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration (erp_loc_a101): % seconds',
                 EXTRACT(EPOCH FROM end_time - start_time);

    -- silver.erp_px_cat_g1v2
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Truncating silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Inserting into silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Load Duration (erp_px_cat_g1v2): % seconds',
                 EXTRACT(EPOCH FROM end_time - start_time);

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Silver Layer COMPLETED';
    RAISE NOTICE 'Total Load Duration: % seconds',
                 EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING '========================================';
        RAISE WARNING 'ERROR DURING loading silver layer: %', SQLERRM;
        RAISE WARNING '========================================';
END;
$$;
