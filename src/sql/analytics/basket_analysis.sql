-- Market Basket Analysis
SELECT
    a.product_name AS product_a,
    b.product_name AS product_b,
    COUNT(*) AS co_occurrence
FROM mci_task2.orders_fact a
JOIN mci_task2.orders_fact b
    ON a.order_id = b.order_id
    AND a.product_id < b.product_id
GROUP BY product_a, product_b
HAVING co_occurrence >= 3
ORDER BY co_occurrence DESC
LIMIT 30;
