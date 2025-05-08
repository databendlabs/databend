COPY INTO hits
FROM @datasets/hits/hits.parquet FILE_FORMAT =(TYPE = 'PARQUET');
ANALYZE TABLE hits;
