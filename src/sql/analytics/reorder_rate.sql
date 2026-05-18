-- Product Reorder Rate
SELECT
    product_name,
    COUNT(*) AS total_appearance,
    SUM(reordered) AS total_reordered,
    ROUND(SUM(reordered) * 100.0 / COUNT(*), 2) AS reorder_rate_pct
FROM mci_task2.orders_fact
GROUP BY product_name
HAVING COUNT(*) >= 5
ORDER BY reorder_rate_pct DESC
LIMIT 20;
