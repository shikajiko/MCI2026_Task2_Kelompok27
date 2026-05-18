-- Busiest Day of the Week
SELECT
    order_dow,
    COUNT(DISTINCT order_id) AS total_orders
FROM mci_task2.orders_fact
GROUP BY order_dow
ORDER BY order_dow;
