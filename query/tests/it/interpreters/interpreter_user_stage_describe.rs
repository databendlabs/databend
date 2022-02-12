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
async fn test_desc_stageinterpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;

    // create stage
    {
        let query = "CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP  record_delimiter='|') comments='test'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CreateUserStageInterpreter");
        let _ = executor.execute(None).await?;
    }

    // desc stage
    {
        let plan = PlanParser::parse(ctx.clone(), "desc stage test_stage").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+-------------------+-------------------+----------------+------------------+-------------------+------------------+",
            "| parent_properties | properties        | property_types | property_values  | property_defaults | property_changed |",
            "+-------------------+-------------------+----------------+------------------+-------------------+------------------+",
            "| stage_params      | url               | String         | s3://load/files/ |                   | true             |",
            "| credentials       | access_key_id     | String         | 1a2b3c           |                   | true             |",
            "| credentials       | secret_access_key | String         | 4x5y6z           |                   | true             |",
            "| file_format       | format            | String         | Csv              | Csv               | false            |",
            "| file_format       | record_delimiter  | String         | |                |                   | true             |",
            // default record_delimiter is \n, so it breaks with an empty line
            "|                   |                   |                |                  |                   |                  |",
            "| file_format       | field_delimiter   | String         | ,                | ,                 | false            |",
            "| file_format       | csv_header        | Boolean        | false            | false             | false            |",
            "| file_format       | compression       | String         | Gzip             | None              | true             |",
            "+-------------------+-------------------+----------------+------------------+-------------------+------------------+",
        ];
        common_datablocks::assert_blocks_eq(expected, result.as_slice());
    }

    Ok(())
}
