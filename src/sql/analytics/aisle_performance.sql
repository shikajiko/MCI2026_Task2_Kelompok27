-- Aisle Performance
SELECT
    aisle,
    department,
    COUNT(*) AS total_items,
    SUM(reordered) AS total_reorder,
    ROUND(SUM(reordered) * 100.0 / COUNT(*), 2) AS reorder_rate_pct
FROM mci_task2.orders_fact
GROUP BY aisle, department
HAVING COUNT(*) >= 5
ORDER BY reorder_rate_pct DESC
LIMIT 20;
