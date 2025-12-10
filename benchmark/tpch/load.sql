COPY INTO customer
FROM @datasets/tpch100/customer/ PATTERN = 'customer.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 1
    );
ANALYZE TABLE customer;
COPY INTO lineitem
FROM @datasets/tpch100/lineitem/ PATTERN = 'lineitem.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE lineitem;
COPY INTO nation
FROM @datasets/tpch100/nation.tbl FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE nation;
COPY INTO orders
FROM @datasets/tpch100/orders/ PATTERN = 'orders.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE orders;
COPY INTO partsupp
FROM @datasets/tpch100/partsupp/ PATTERN = 'partsupp.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE partsupp;
COPY INTO part
FROM @datasets/tpch100/part/ PATTERN = 'part.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE part;
COPY INTO region
FROM @datasets/tpch100/region.tbl FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE region;
COPY INTO supplier
FROM @datasets/tpch100/supplier/ PATTERN = 'supplier.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE supplier;
