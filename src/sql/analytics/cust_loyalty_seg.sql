-- Customer Loyalty Segmentation
SELECT
    CASE
        WHEN max_order_number = 1 THEN 'New (1 order)'
        WHEN max_order_number BETWEEN 2 AND 5 THEN 'Returning (2-5 orders)'
        WHEN max_order_number BETWEEN 6 AND 15 THEN 'Regular (6-15 orders)'
        ELSE 'Loyal (16+ orders)'
    END AS segment,
    COUNT(*) AS total_users
FROM (
    SELECT user_id, MAX(order_number) AS max_order_number
    FROM mci_task2.orders_fact
    GROUP BY user_id
) t
GROUP BY segment;
