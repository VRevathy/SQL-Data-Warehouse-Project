/*
*******************************************************************************************
Script Purpose
*************************************************************
  The script performs various quality checks for data consistency, accuracy and standardisation across the 'silver' schemas. It includes checks for:
    -- Null or duplicate primary Keys
    - Unwanted spaces in string fields
    - Data standardisation and consistency
    - Invalid date ranges and orders
- Data consistency between related fields.

Usage Notes:
- Run these checks after loading silver layer.
- Investigate and resolve any discrepancies found during checks
*****************************************************************
*/

---- Check for Nulls  or Duplicates in PRIMARY Key in silver layer- cust_info table
--Expectation: No Result
SELECT 
cst_id, 
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL
--Check for unwanted Spaces
--Expectation : No Results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname !=TRIM(cst_lastname)

--Check for unwanted Spaces
--Expectation : No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname !=TRIM(cst_firstname)

-- Check Data Standardisation and Consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

-- Check Data Standardisation and Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info


---=========================================
---- Check for Nulls  or Duplicates in PRIMARY Key in silver layer- cust_info table
SELECT 
cst_id, 
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL

--========================
--Check the quality of the silver Table
-- Quality Checks
--Check for Nulls or Duplicates in primary Key
-- Expectation: No Result
SELECT 
prd_id, 
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

--Check for unwanted Spaces
--Expectation : No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm !=TRIM(prd_nm)
USE DataWarehouse
GO
--Check for NULLs or Negative Numbers
--Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL

--Data Standardisation and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--Check for nvalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt<prd_start_dt

SELECT *
FROM
silver.crm_prd_info

-- Check for Invalid Dates
-- Also the dates are in Integer type.
-- We need to convert the Integer to Date.

USE DataWarehouse
GO
SELECT
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0

--There are so many Zeros, Replace the Zero with NULL
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0

-- Check the length of the Date.
-- Check for outliers by validating the boundaries of the date Range
USE DataWarehouse
GO
SELECT 
NULLIF(sls_order_dt,0)sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LEN(sls_order_dt)!=8 OR sls_order_dt >20500101

-- Check for outliers by validating the boundaries of the date Range
SELECT 
NULLIF(sls_order_dt,0)sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt >20500101 OR sls_order_dt<19000101

--Final check for Invalid Dates and Outliers
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0
OR LEN(sls_order_dt) !=8
OR sls_order_dt>20500101 OR sls_order_dt < 19000101

--- Check for Shipping date Column
SELECT 
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt<=0
OR LEN(sls_ship_dt) !=8
OR sls_ship_dt>20500101 OR sls_ship_dt < 19000101

--- Check for Due Date Column
SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt<=0
OR LEN(sls_due_dt) !=8
OR sls_due_dt>20500101 OR sls_due_dt < 19000101

-- Check for Invalid Date Orders
USE DataWarehouse
GO
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity and Price
--->> Sales= Quantity * Price
-->> Values must not be NULL , Zero, or negative.
USE DataWarehouse
GO
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity *ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
	THEN sls_sales /NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales !=sls_quantity *sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,
sls_quantity,
sls_price

--- Quality Check of the Silver Table
-- Check for Invalid Date Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt >sls_due_dt

--Check Data Consistency: between Sales, Quantity and Price
USE DataWarehouse
GO
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales!=sls_quantity *sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 or sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- Final check on the Table
SELECT * FROM silver.crm_sales_details

--Identify Out-Of -Range Dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()

USE DataWarehouse
GO
--- Data Standardisation & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

-- Final check on the Table:
SELECT * silver.erp_cust_az12








