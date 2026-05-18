CREATE TABLE mci_task2.orders_fact (
            order_id            Int32,
            user_id             Int32,
            order_number        Int32,
            order_dow           Int32,
            order_hour_of_day   Int32,
            days_since_prior_order Nullable(Float32),
            eval_set            String,
            product_id          Int32,
            product_name        String,
            aisle_id            Int32,
            aisle               String,
            department_id       Int32,
            department          Nullable(String),
            add_to_cart_order   Int32,
            reordered           Int32,
            is_days_since_prior_order_missing UInt8
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
