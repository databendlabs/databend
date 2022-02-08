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

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use databend_query::storages::system::EnginesTable;
use databend_query::storages::Table;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_engines_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    let table: Arc<dyn Table> = Arc::new(EnginesTable::create(1));
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx.clone(), &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;

    let expected = vec![
        "+--------+-----------------------+",
        "| Engine | Comment               |",
        "+--------+-----------------------+",
        "| FUSE   | FUSE Storage Engine   |",
        "| GITHUB | GITHUB Storage Engine |",
        "| MEMORY | MEMORY Storage Engine |",
        "| NULL   | NULL Storage Engine   |",
        "+--------+-----------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected.clone(), result.as_slice());

    let stream_ = table.read(ctx, &source_plan).await?;
    let result_ = stream_.try_collect::<Vec<_>>().await?;
    common_datablocks::assert_blocks_sorted_eq(expected, result_.as_slice());

    Ok(())
}
