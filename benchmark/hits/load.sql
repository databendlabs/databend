COPY INTO hits
FROM @hits_local/hits.parquet FILE_FORMAT =(TYPE = 'PARQUET');
