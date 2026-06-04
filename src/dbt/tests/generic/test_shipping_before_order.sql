-- Generic Test: Shipping Before Order
-- Validates that the shipping/carrier date is not before the
-- order purchase date (business logic constraint).
--
-- Usage in schema.yml:
--   tests:
--     - shipping_before_order:
--         order_date_column: order_purchase_timestamp
--         shipping_date_column: order_delivered_carrier_date

{% test shipping_before_order(model, order_date_column, shipping_date_column) %}

SELECT
    *
FROM {{ model }}
WHERE {{ shipping_date_column }} IS NOT NULL
  AND {{ order_date_column }} IS NOT NULL
  AND {{ shipping_date_column }} < {{ order_date_column }}

{% endtest %}
