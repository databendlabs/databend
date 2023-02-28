COPY INTO hits FROM 's3://repo.databend.rs/hits_p/' pattern ='.*[.]tsv' file_format=(type='TSV' field_delimiter='\t' record_delimiter='\n' skip_header=1);
