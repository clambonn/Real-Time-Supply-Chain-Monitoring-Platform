-- Database: SupplyChain_DWH
-- Script: Stored Procedure for Staging Order Ingestion & Testing
-- Author: Hanan Hosny

USE SupplyChain_DWH;
GO

-- 1. Create Stored Procedure for Consumer Ingestion
IF OBJECT_ID('staging.sp_insert_stg_order', 'P') IS NOT NULL
    DROP PROCEDURE staging.sp_insert_stg_order;
GO

CREATE PROCEDURE staging.sp_insert_stg_order
    @order_id VARCHAR(50),
    @customer_id VARCHAR(50),
    @order_status VARCHAR(20),
    @order_purchase_timestamp DATETIME,
    @order_approved_at DATETIME = NULL,
    @order_delivered_carrier_date DATETIME = NULL,
    @order_delivered_customer_date DATETIME = NULL,
    @order_estimated_delivery_date DATETIME = NULL
AS
BEGIN
    -- Check if order_id already exists to prevent duplicates
    IF NOT EXISTS (SELECT 1 FROM staging.stg_orders WHERE order_id = @order_id)
    BEGIN
        INSERT INTO staging.stg_orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            ingested_at
        )
        VALUES (
            @order_id,
            @customer_id,
            @order_status,
            @order_purchase_timestamp,
            @order_approved_at,
            @order_delivered_carrier_date,
            @order_delivered_customer_date,
            @order_estimated_delivery_date,
            GETDATE()
        );

        PRINT 'Order inserted successfully.';
    END
    ELSE
    BEGIN
        PRINT 'Order already exists. Duplicate ignored.';
    END
END;
GO

-- 2. Test Ingestion and Duplicate Prevention

-- Test 1: Insert New Order
EXEC staging.sp_insert_stg_order 
    @order_id = 'TEST_ORDER_001',
    @customer_id = 'CUST_TEST_999',
    @order_status = 'delivered',
    @order_purchase_timestamp = '2026-11-09 10:00:00';

-- Test 2: Duplicate Order Attempt (Should be ignored)
EXEC staging.sp_insert_stg_order 
    @order_id = 'TEST_ORDER_001',
    @customer_id = 'CUST_TEST_999',
    @order_status = 'delivered',
    @order_purchase_timestamp = '2026-11-09 10:00:00';

-- Verify Test Result (Should show 1 row only)
SELECT * FROM staging.stg_orders WHERE order_id = 'TEST_ORDER_001';
GO