// Copyright 2022 Datafuse Labs.
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
use databend_query::sql::PlanParser;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_settings_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    // show settings.
    {
        let plan = PlanParser::parse(ctx.clone(), "show settings").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowSettingsInterpreter");

        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+------------------------------------+---------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------+--------+",
            "| name                               | value   | default | level   | description                                                                                                                                | type   |",
            "+------------------------------------+---------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------+--------+",
            "| enable_new_processor_framework     | 0       | 0       | SESSION | Enable new processor framework if value != 0, default value: 0                                                                             | UInt64 |",
            "| flight_client_timeout              | 60      | 60      | SESSION | Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds                                         | UInt64 |",
            "| max_block_size                     | 10000   | 10000   | SESSION | Maximum block size for reading                                                                                                             | UInt64 |",
            "| max_threads                        | 8       | 16      | SESSION | The maximum number of threads to execute the request. By default, it is determined automatically.                                          | UInt64 |",
            "| parallel_read_threads              | 1       | 1       | SESSION | The maximum number of parallelism for reading data. By default, it is 1.                                                                   | UInt64 |",
            "| storage_occ_backoff_init_delay_ms  | 5       | 5       | SESSION | The initial retry delay in millisecond. By default, it is 5 ms.                                                                            | UInt64 |",
            "| storage_occ_backoff_max_delay_ms   | 20000   | 20000   | SESSION | The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds. | UInt64 |",
            "| storage_occ_backoff_max_elapsed_ms | 120000  | 120000  | SESSION | The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.                    | UInt64 |",
            "| storage_read_buffer_size           | 1048576 | 1048576 | SESSION | The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.                                                             | UInt64 |",
            "+------------------------------------+---------+---------+---------+--------------------------------------------------------------------------------------------------------------------------------------------+--------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
