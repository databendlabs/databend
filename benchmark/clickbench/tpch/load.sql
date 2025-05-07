COPY INTO customer
FROM @datasets/tpch/customer/ PATTERN = 'customer.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 1
    );
ANALYZE TABLE customer;
COPY INTO lineitem
FROM @datasets/tpch/lineitem/ PATTERN = 'lineitem.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE lineitem;
COPY INTO nation
FROM @datasets/tpch/nation.tbl FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE nation;
COPY INTO orders
FROM @datasets/tpch/orders/ PATTERN = 'orders.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE orders;
COPY INTO partsupp
FROM @datasets/tpch/partsupp/ PATTERN = 'partsupp.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE partsupp;
COPY INTO part
FROM @datasets/tpch/part/ PATTERN = 'part.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE part;
COPY INTO region
FROM @datasets/tpch/region.tbl FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE region;
COPY INTO supplier
FROM @datasets/tpch/supplier/ PATTERN = 'supplier.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE supplier;
