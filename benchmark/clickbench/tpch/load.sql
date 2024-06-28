COPY INTO customer
FROM 's3://repo.databend.com/tpch100/customer/' CONNECTION =(REGION = 'us-east-2') PATTERN = 'customer.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 1
    );
ANALYZE TABLE customer;
COPY INTO lineitem
FROM 's3://repo.databend.com/tpch100/lineitem/' CONNECTION =(REGION = 'us-east-2') PATTERN = 'lineitem.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE lineitem;
COPY INTO nation
FROM 's3://repo.databend.com/tpch100/nation.tbl' CONNECTION =(REGION = 'us-east-2') FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE nation;
COPY INTO orders
FROM 's3://repo.databend.com/tpch100/orders/' CONNECTION =(REGION = 'us-east-2') PATTERN = 'orders.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE orders;
COPY INTO partsupp
FROM 's3://repo.databend.com/tpch100/partsupp/' CONNECTION =(REGION = 'us-east-2') PATTERN = 'partsupp.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE partsupp;
COPY INTO part
FROM 's3://repo.databend.com/tpch100/part/' CONNECTION =(REGION = 'us-east-2') PATTERN = 'part.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE part;
COPY INTO region
FROM 's3://repo.databend.com/tpch100/region.tbl' CONNECTION =(REGION = 'us-east-2') FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE region;
COPY INTO supplier
FROM 's3://repo.databend.com/tpch100/supplier/' CONNECTION =(REGION = 'us-east-2') PATTERN = 'supplier.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE supplier;
