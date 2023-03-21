COPY INTO customer
FROM 's3://repo.databend.rs/tpch100/customer/' pattern = 'customer.tbl.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 1
    );
ANALYZE TABLE customer;
COPY INTO lineitem
FROM 's3://repo.databend.rs/tpch100/lineitem/' pattern = 'lineitem.tbl.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE lineitem;
COPY INTO nation
FROM 's3://repo.databend.rs/tpch100/nation.tbl' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE nation;
COPY INTO orders
FROM 's3://repo.databend.rs/tpch100/orders/' pattern = 'orders.tbl.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE orders;
COPY INTO partsupp
FROM 's3://repo.databend.rs/tpch100/partsupp/' pattern = 'partsupp.tbl.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE partsupp;
COPY INTO part
FROM 's3://repo.databend.rs/tpch100/part/' pattern = 'part.tbl.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE part;
COPY INTO region
FROM 's3://repo.databend.rs/tpch100/region.tbl' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE region;
COPY INTO supplier
FROM 's3://repo.databend.rs/tpch100/supplier/' pattern = 'supplier.tbl.*' file_format =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE supplier;
