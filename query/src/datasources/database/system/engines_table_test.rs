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

use common_exception::Result;
use common_planners::*;
use common_base::tokio;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::catalogs::Table;
use crate::datasources::database::system::EnginesTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_engines_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    ctx.get_settings().set_max_threads(2)?;

    let table = EnginesTable::create();
    let source_plan = table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_settings().get_max_threads()? as usize,
    )?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);
    assert_eq!(block.num_rows(), 3);
    // TODO rename to databend after merge
    let expected = vec![
    "+---------+-----------------------------------------------------------------------------------------------+",
    "| name    | description                                                                                   |",
    "+---------+-----------------------------------------------------------------------------------------------+",
    "| DEFAULT | default database engine, with embedded metastore backend                                      |",
    "| EXAMPLE | The example engine is used by example databases and tables.                                   |",
    "| SYSTEM  | The system engine is used by tables in the system database, which store Databend information. |",
    "+---------+-----------------------------------------------------------------------------------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
