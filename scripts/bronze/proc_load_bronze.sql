-- ================================================
-- Procedure: Load Bronze Layer (Source → Bronze)
-- ================================================
-- This procedure truncates each bronze table and then
-- loads data from CSV files using PostgreSQL’s COPY command.
-- Adjust the file paths to wherever your CSVs live on the
-- database server’s filesystem.
-- ================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '========================================';

    -- ═══ Loading CRM Tables ═══

    -- crm_cust_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> COPY into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    FROM '/path/to/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- crm_prd_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> COPY into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info (
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    FROM '/path/to/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- crm_sales_details
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> COPY into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details (
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
    FROM '/path/to/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ═══ Loading ERP Tables ═══

    -- erp_loc_a101
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> COPY into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 (
        cid,
        cntry
    )
    FROM '/path/to/source_erp/loc_a101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- erp_cust_az12
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> COPY into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    FROM '/path/to/source_erp/cust_az12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- erp_px_cat_g1v2
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> COPY into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    FROM '/path/to/source_erp/px_cat_g1v2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Bronze Layer COMPLETED';
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '========================================';
    
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '========================================';
    RAISE WARNING 'ERROR DURING Load Bronze Layer';
    RAISE WARNING 'Error Message: %', SQLERRM;
    RAISE WARNING '========================================';
END;
$$;
