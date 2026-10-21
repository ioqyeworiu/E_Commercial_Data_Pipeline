CREATE TABLE bronze(
    url_Id BIGINT,
    title VARCHAR(1000),
    brand VARCHAR(1000),
    price DOUBLE,
    discount DOUBLE,
    sell_price DOUBLE,
    sold INTEGER,
    product_rate FLOAT,
    product_rate_count INTEGER,
    category_tree VARCHAR(1000),
    category_lv1 VARCHAR(1000),
    category_leaf VARCHAR(1000),
    brand_origin VARCHAR(1000),
    made_in VARCHAR(1000),
    warranty VARCHAR(1000),
    description TEXT,
    seller VARCHAR(1000),
    seller_rate float,
    seller_rate_count INTEGER,
    timestamp TIMESTAMP
)
ENGINE MergeTree
ORDER BY (url_Id, timestamp);