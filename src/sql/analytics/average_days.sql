-- Average Days Between Orders
SELECT
    AVG(days_since_prior_order) AS avg_days_between_orders
FROM mci_task2.orders_fact
WHERE days_since_prior_order IS NOT NULL;
