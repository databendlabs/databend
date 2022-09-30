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

use std::io::Write;

use common_base::base::tokio;
use common_datablocks::pretty_format_blocks;
use common_exception::Result;
use databend_query::sessions::TableContext;
use databend_query::storages::system::SettingsTable;
use databend_query::storages::TableStreamReadWrap;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;
use goldenfile::Mint;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_settings_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/system/testdata");
    let file = &mut mint.new_goldenfile("settings.txt").unwrap();

    let (_guard, ctx) = crate::tests::create_query_context().await?;
    ctx.get_settings().set_max_threads(2)?;

    let table = SettingsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let formatted = pretty_format_blocks(&blocks).unwrap();
    write!(file, "{formatted}").unwrap();
    Ok(())
}
