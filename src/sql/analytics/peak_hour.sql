-- Busiest Time of the Day
SELECT
    order_hour_of_day,
    COUNT(DISTINCT order_id) AS total_orders
FROM mci_task2.orders_fact
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;
