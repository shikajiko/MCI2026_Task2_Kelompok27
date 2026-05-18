-- Top Department
SELECT
    department,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items
FROM mci_task2.orders_fact
WHERE department IS NOT NULL
GROUP BY department
ORDER BY total_items DESC;
