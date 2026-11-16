
USE SupplyChain_Source;
GO

CREATE TABLE source.etl_watermark (
    pipeline_name VARCHAR(100) NOT NULL PRIMARY KEY,
    last_order_purchase_timestamp DATETIME2 NULL,
    last_order_id VARCHAR(50) NULL,
    last_updated_at DATETIME2 NOT NULL
        DEFAULT SYSUTCDATETIME()
);
GO

INSERT INTO source.etl_watermark (
    pipeline_name,
    last_order_purchase_timestamp,
    last_order_id
)
VALUES (
    'orders',
    NULL,
    NULL
);
GO
