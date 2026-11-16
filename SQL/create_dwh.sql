-- Database: SupplyChain_DWH
-- Script: Create Data Warehouse Databases and Tables
-- Author: Hanan Hosny

-- 1. Create Database
CREATE DATABASE SupplyChain_DWH;
GO

USE SupplyChain_DWH;
GO

-- 2. Create Schemas
CREATE SCHEMA staging;
GO

CREATE SCHEMA core;
GO

-- 3. Staging Table (For Kafka Consumer Ingestion)
CREATE TABLE staging.stg_orders (
    order_id VARCHAR(50) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    order_purchase_timestamp DATETIME NOT NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    ingested_at DATETIME DEFAULT GETDATE()
);

-- 4. Dimension Tables

-- Customer Dimension
CREATE TABLE core.dim_customers (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NULL,
    customer_city VARCHAR(100) NULL,
    customer_state VARCHAR(5) NULL,
    created_at DATETIME DEFAULT GETDATE()
);

-- Product Dimension
CREATE TABLE core.dim_products (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_category_name VARCHAR(100) NULL,
    product_category_name_english VARCHAR(100) NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL,
    created_at DATETIME DEFAULT GETDATE()
);

-- Seller Dimension
CREATE TABLE core.dim_sellers (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL UNIQUE,
    seller_zip_code_prefix VARCHAR(10) NULL,
    seller_city VARCHAR(100) NULL,
    seller_state VARCHAR(5) NULL,
    created_at DATETIME DEFAULT GETDATE()
);

-- Date Dimension
CREATE TABLE core.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BIT NOT NULL
);

-- 5. Fact Table (Starts Empty)
CREATE TABLE core.fact_orders (
    order_sk INT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    customer_key INT NOT NULL,
    purchase_date_key INT NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    order_purchase_timestamp DATETIME NOT NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    is_delayed BIT NULL,
    delay_days INT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (customer_key) REFERENCES core.dim_customers(customer_key),
    FOREIGN KEY (purchase_date_key) REFERENCES core.dim_date(date_key)
);