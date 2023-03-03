COPY INTO customer
FROM 's3://repo.databend.rs/datasets/tpch10/customer/' pattern = 'customer.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE customer;
COPY INTO lineitem
FROM 's3://repo.databend.rs/datasets/tpch10/lineitem/' pattern = 'lineitem.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE lineitem;
COPY INTO nation
FROM 's3://repo.databend.rs/datasets/tpch10/nation/' pattern = 'nation.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE nation;
COPY INTO orders
FROM 's3://repo.databend.rs/datasets/tpch10/orders/' pattern = 'orders.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE orders;
COPY INTO partsupp
FROM 's3://repo.databend.rs/datasets/tpch10/partsupp/' pattern = 'partsupp.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE partsupp;
COPY INTO part
FROM 's3://repo.databend.rs/datasets/tpch10/part/' pattern = 'part.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE part;
COPY INTO region
FROM 's3://repo.databend.rs/datasets/tpch10/region/' pattern = 'region.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE region;
COPY INTO supplier
FROM 's3://repo.databend.rs/datasets/tpch10/supplier/' pattern = 'supplier.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE supplier;
