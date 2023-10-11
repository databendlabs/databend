-- load data to source_table, it's almost 11997996 rows in source
COPY INTO source_table
FROM  'fs:///tmp/data/lineitem.tbl' PATTERN = 'lineitem.tbl.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );
ANALYZE TABLE source_table;

-- load data to target_table, it's almost 6000000 rows in source
COPY INTO target_table
FROM  'fs:///tmp/data/lineitem2.tbl' PATTERN = 'lineitem.tbl2.*' FILE_FORMAT =(
        type = 'CSV' field_delimiter = '|' record_delimiter = '\n' skip_header = 0
    );

ANALYZE TABLE target_table;
