-- 1. Count Customers
SELECT COUNT(*) AS Total_Customers
FROM bronze.olist_customers_dataset;

-- 2. Count Products
SELECT COUNT(*) AS Total_Products
FROM bronze.olist_products_dataset;

-- 3. Count Sellers
SELECT COUNT(*) AS Total_Sellers
FROM bronze.olist_sellers_dataset;

-- 4. Count Orders
SELECT COUNT(*) AS Total_Orders
FROM bronze.Orders;

-- 5. Check Duplicate Customers
SELECT customer_id, COUNT(*) AS Duplicate_Count
FROM bronze.olist_customers_dataset
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 6. Check Duplicate Products
SELECT product_id, COUNT(*) AS Duplicate_Count
FROM bronze.olist_products_dataset
GROUP BY product_id
HAVING COUNT(*) > 1;

-- 7. Check Duplicate Sellers
SELECT seller_id, COUNT(*) AS Duplicate_Count
FROM bronze.olist_sellers_dataset
GROUP BY seller_id
HAVING COUNT(*) > 1;

-- 8. Check Duplicate Orders
SELECT order_id, COUNT(*) AS Duplicate_Count
FROM bronze.Orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 9. Check NULL Primary Keys
SELECT 'Customers' AS Table_Name, COUNT(*) AS Null_Keys
FROM bronze.olist_customers_dataset
WHERE customer_id IS NULL
UNION ALL
SELECT 'Products', COUNT(*)
FROM bronze.olist_products_dataset
WHERE product_id IS NULL
UNION ALL
SELECT 'Sellers', COUNT(*)
FROM bronze.olist_sellers_dataset
WHERE seller_id IS NULL
UNION ALL
SELECT 'Orders', COUNT(*)
FROM bronze.Orders
WHERE order_id IS NULL;