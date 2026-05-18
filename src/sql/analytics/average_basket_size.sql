-- Average Basket Size
SELECT
    AVG(basket_size) AS avg_basket_size
FROM (
    SELECT order_id, COUNT(product_id) AS basket_size
    FROM mci_task2.orders_fact
    GROUP BY order_id
) t;
