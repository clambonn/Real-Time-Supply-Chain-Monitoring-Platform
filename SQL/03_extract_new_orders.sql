
USE SupplyChain_Source;
GO

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp
FROM source.orders o
INNER JOIN source.etl_watermark w
    ON w.pipeline_name = 'orders'
WHERE
    w.last_order_purchase_timestamp IS NULL
    OR o.order_purchase_timestamp > w.last_order_purchase_timestamp
    OR (
        o.order_purchase_timestamp = w.last_order_purchase_timestamp
        AND o.order_id > w.last_order_id
    )
ORDER BY
    o.order_purchase_timestamp,
    o.order_id;
GO
