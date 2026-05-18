-- Order Trend by User Segmentation
SELECT
    CASE
        WHEN max_order_number = 1 THEN 'New (1 order)'
        WHEN max_order_number BETWEEN 2 AND 5 THEN 'Returning (2-5 orders)'
        WHEN max_order_number BETWEEN 6 AND 15 THEN 'Regular (6-15 orders)'
        ELSE 'Loyal (16+ orders)'
    END AS segment,
    ROUND(AVG(basket_size), 2) AS avg_basket_size,
    COUNT(DISTINCT user_id) AS total_users
FROM (
    SELECT
        o.user_id,
        o.order_id,
        COUNT(o.product_id) AS basket_size,
        MAX(o.order_number) OVER (PARTITION BY o.user_id) AS max_order_number
    FROM mci_task2.orders_fact o
    GROUP BY o.user_id, o.order_id, o.order_number
) t
GROUP BY segment
ORDER BY avg_basket_size DESC;
