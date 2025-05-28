/*
===============================================================================
Quality Verification
===============================================================================
Overview:
    This batch of queries verifies that the Gold layer is sound, coherent, and 
    free of anomalies. Specifically, it covers:
      - Ensuring each dimension’s surrogate key is distinct.
      - Confirming every fact record correctly references its dimensions.
      - Checking that the star schema relationships hold.

Instructions:
    - Run these checks regularly.  
    - If any rows return, investigate and correct the underlying data issues.
===============================================================================
*/

-- ====================================================================
-- Assessing 'gold.dim_customers'
-- ====================================================================
-- Verify that every customer_key is unique.
-- Expected outcome: no rows returned.
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- Assessing 'gold.dim_products'
-- ====================================================================
-- Verify that every product_key is unique.
-- Expected outcome: no rows returned.
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- Assessing 'gold.fact_sales'
-- ====================================================================
-- Confirm that all foreign keys in the fact table match existing dimension keys.
-- Any rows returned indicate missing dimension references.
SELECT f.*
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
  ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
  ON f.product_key = p.product_key
WHERE c.customer_key IS NULL
   OR p.product_key    IS NULL;
