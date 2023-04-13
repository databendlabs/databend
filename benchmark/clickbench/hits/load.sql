COPY INTO hits
FROM 's3://repo.databend.rs/hits_p/' pattern = '.*[.]tsv' CONNECTION =(REGION = 'us-east-2') FILE_FORMAT =(
        type = 'TSV' field_delimiter = '\t' record_delimiter = '\n' skip_header = 1
    );
ANALYZE TABLE hits;
