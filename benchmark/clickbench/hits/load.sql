COPY INTO hits
FROM 's3://repo.databend.rs/hits_p/' CONNECTION =(REGION = 'us-east-2') PATTERN = '.*[.]tsv' FILE_FORMAT =(
        type = 'TSV' field_delimiter = '\t' record_delimiter = '\n' skip_header = 1
    );
ANALYZE TABLE hits;
