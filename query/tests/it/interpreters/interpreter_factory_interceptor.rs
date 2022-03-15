// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_interpreter_interceptor() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;
    {
        let query = "select number from numbers_mt(100) where number > 90";
        ctx.attach_query_str(query);
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        interpreter.start().await?;
        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);

        let expected = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 91     |",
            "| 92     |",
            "| 93     |",
            "| 94     |",
            "| 95     |",
            "| 96     |",
            "| 97     |",
            "| 98     |",
            "| 99     |",
            "+--------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        interpreter.finish().await?;
    }

    // Check.
    {
        let query = "select log_type, handler_type, cpu_usage, scan_rows, scan_bytes, scan_partitions, written_rows, written_bytes, result_rows, result_bytes, query_kind, query_text, sql_user, sql_user_quota, session_settings from system.query_log";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;

        let expected = vec![
            "+----------+--------------+-----------+-----------+------------+-----------------+--------------+---------------+-------------+--------------+------------+------------------------------------------------------+----------+--------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| log_type | handler_type | cpu_usage | scan_rows | scan_bytes | scan_partitions | written_rows | written_bytes | result_rows | result_bytes | query_kind | query_text                                           | sql_user | sql_user_quota                 | session_settings                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |",
            "+----------+--------------+-----------+-----------+------------+-----------------+--------------+---------------+-------------+--------------+------------+------------------------------------------------------+----------+--------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| 1        | TestSession  | 8         | 0         | 0          | 0               | 0            | 0             | 0           | 0            | SelectPlan | select number from numbers_mt(100) where number > 90 | root     | UserQuota<cpu:0,mem:0,store:0> | [[enable_new_processor_framework, 0, 0, SESSION, Enable new processor framework if value != 0, default value: 0], [flight_client_timeout, 60, 60, SESSION, Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds], [max_block_size, 10000, 10000, SESSION, Maximum block size for reading], [max_threads, 8, 16, SESSION, The maximum number of threads to execute the request. By default, it is determined automatically.], [parallel_read_threads, 1, 1, SESSION, The maximum number of parallelism for reading data. By default, it is 1.], [storage_occ_backoff_init_delay_ms, 5, 5, SESSION, The initial retry delay in millisecond. By default, it is 5 ms.], [storage_occ_backoff_max_delay_ms, 20000, 20000, SESSION, The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds.], [storage_occ_backoff_max_elapsed_ms, 120000, 120000, SESSION, The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.], [storage_read_buffer_size, 1048576, 1048576, SESSION, The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.]] |",
            "| 2        | TestSession  | 8         | 100       | 800        | 0               | 0            | 0             | 9           | 72           | SelectPlan | select number from numbers_mt(100) where number > 90 | root     | UserQuota<cpu:0,mem:0,store:0> | [[enable_new_processor_framework, 0, 0, SESSION, Enable new processor framework if value != 0, default value: 0], [flight_client_timeout, 60, 60, SESSION, Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds], [max_block_size, 10000, 10000, SESSION, Maximum block size for reading], [max_threads, 8, 16, SESSION, The maximum number of threads to execute the request. By default, it is determined automatically.], [parallel_read_threads, 1, 1, SESSION, The maximum number of parallelism for reading data. By default, it is 1.], [storage_occ_backoff_init_delay_ms, 5, 5, SESSION, The initial retry delay in millisecond. By default, it is 5 ms.], [storage_occ_backoff_max_delay_ms, 20000, 20000, SESSION, The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds.], [storage_occ_backoff_max_elapsed_ms, 120000, 120000, SESSION, The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.], [storage_read_buffer_size, 1048576, 1048576, SESSION, The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.]] |",
            "+----------+--------------+-----------+-----------+------------+-----------------+--------------+---------------+-------------+--------------+------------+------------------------------------------------------+----------+--------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_interpreter_interceptor_for_insert() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;
    {
        let query = "create table t as select number from numbers_mt(1)";
        ctx.attach_query_str(query);
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        interpreter.start().await?;
        let stream = interpreter.execute(None).await?;
        stream.try_collect::<Vec<_>>().await?;
        interpreter.finish().await?;
    }

    // Check.
    {
        let query = "select log_type, handler_type, cpu_usage, scan_rows, scan_bytes, scan_partitions, written_rows, written_bytes, result_rows, result_bytes, query_kind, query_text, sql_user, sql_user_quota, session_settings from system.query_log";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;

        let expected = vec![
            "+----------+--------------+-----------+-----------+------------+-----------------+--------------+---------------+-------------+--------------+-----------------+----------------------------------------------------+----------+--------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| log_type | handler_type | cpu_usage | scan_rows | scan_bytes | scan_partitions | written_rows | written_bytes | result_rows | result_bytes | query_kind      | query_text                                         | sql_user | sql_user_quota                 | session_settings                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |",
            "+----------+--------------+-----------+-----------+------------+-----------------+--------------+---------------+-------------+--------------+-----------------+----------------------------------------------------+----------+--------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| 1        | TestSession  | 8         | 0         | 0          | 0               | 0            | 0             | 0           | 0            | CreateTablePlan | create table t as select number from numbers_mt(1) | root     | UserQuota<cpu:0,mem:0,store:0> | [[enable_new_processor_framework, 0, 0, SESSION, Enable new processor framework if value != 0, default value: 0], [flight_client_timeout, 60, 60, SESSION, Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds], [max_block_size, 10000, 10000, SESSION, Maximum block size for reading], [max_threads, 8, 16, SESSION, The maximum number of threads to execute the request. By default, it is determined automatically.], [parallel_read_threads, 1, 1, SESSION, The maximum number of parallelism for reading data. By default, it is 1.], [storage_occ_backoff_init_delay_ms, 5, 5, SESSION, The initial retry delay in millisecond. By default, it is 5 ms.], [storage_occ_backoff_max_delay_ms, 20000, 20000, SESSION, The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds.], [storage_occ_backoff_max_elapsed_ms, 120000, 120000, SESSION, The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.], [storage_read_buffer_size, 1048576, 1048576, SESSION, The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.]] |",
            "| 2        | TestSession  | 8         | 1         | 8          | 0               | 1            | 8             | 0           | 0            | CreateTablePlan | create table t as select number from numbers_mt(1) | root     | UserQuota<cpu:0,mem:0,store:0> | [[enable_new_processor_framework, 0, 0, SESSION, Enable new processor framework if value != 0, default value: 0], [flight_client_timeout, 60, 60, SESSION, Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds], [max_block_size, 10000, 10000, SESSION, Maximum block size for reading], [max_threads, 8, 16, SESSION, The maximum number of threads to execute the request. By default, it is determined automatically.], [parallel_read_threads, 1, 1, SESSION, The maximum number of parallelism for reading data. By default, it is 1.], [storage_occ_backoff_init_delay_ms, 5, 5, SESSION, The initial retry delay in millisecond. By default, it is 5 ms.], [storage_occ_backoff_max_delay_ms, 20000, 20000, SESSION, The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds.], [storage_occ_backoff_max_elapsed_ms, 120000, 120000, SESSION, The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.], [storage_read_buffer_size, 1048576, 1048576, SESSION, The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.]] |",
            "+----------+--------------+-----------+-----------+------------+-----------------+--------------+---------------+-------------+--------------+-----------------+----------------------------------------------------+----------+--------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
