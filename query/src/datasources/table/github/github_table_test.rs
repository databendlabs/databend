// Copyright 2020 Datafuse Labs.
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
use common_datablocks::DataBlock;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::Extras;
use futures::TryStreamExt;

use super::github_table::GithubTable;
use crate::catalogs::ToReadDataSourcePlan;
use crate::datasources::context::TableContext;

#[tokio::test]
async fn test_github_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let table = GithubTable::try_create(
        TableInfo {
            desc: "'github table test".into(),
            name: "datafuselabs".into(),
            ident: TableIdent::default(),
            meta: TableMeta {
                engine: "Github".into(),
                ..TableMeta::default()
            },
        },
        TableContext::default(),
    )?;

    let push_downs = Some(Extras {
        limit: Some(5),
        ..Extras::default()
    });
    let source_plan = table.read_plan(ctx.clone(), push_downs)?;
    ctx.try_set_partitions(source_plan.parts.clone())?;
    assert_eq!(table.engine(), "Github");

    // read
    let stream = table.read(ctx.clone(), &source_plan).await?;
    let result = stream.try_collect::<Vec<DataBlock>>().await?;

    assert!(!result.is_empty());
    let block = result.get(0);
    assert!(block.is_some());
    let block = block.unwrap();
    assert_eq!(block.num_columns(), 8);
    assert_eq!(block.num_rows(), 5);

    Ok(())
}
