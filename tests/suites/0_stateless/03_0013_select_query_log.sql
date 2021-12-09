truncate table system.query_log;
select * from numbers(100) where number > 95;
select log_type, handler_type, cpu_usage, memory_usage, read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, query_kind, query_text, sql_user, sql_user_quota, sql_user_privileges from system.query_log;
truncate table system.query_log;

