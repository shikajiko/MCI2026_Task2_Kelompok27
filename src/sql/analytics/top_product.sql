-- Top Product
SELECT
    product_name,
    COUNT(*) AS total_pembelian,
    SUM(reordered) AS total_reorder
FROM mci_task2.orders_fact
GROUP BY product_name
ORDER BY total_pembelian DESC
LIMIT 20;
