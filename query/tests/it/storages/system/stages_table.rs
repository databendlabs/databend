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

use common_base::base::tokio;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use databend_query::storages::system::StagesTable;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_stages_table() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    let user_mgr = ctx.get_user_manager();

    {
        let stage_info = UserStageInfo {
            stage_name: "test_stage".to_string(),
            ..Default::default()
        };
        user_mgr.add_stage(&tenant, stage_info, false).await?;
    }

    let table = StagesTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;
    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 6);

    let expected = vec![
        "+------------+------------+----------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+",
        "| name       | stage_type | stage_params                                                   | copy_options                                  | file_format_options                                                                                                | comment |",
        "+------------+------------+----------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+",
        r#"| test_stage | External   | StageParams { storage: Fs(StorageFsConfig { root: "_data" }) } | CopyOptions { on_error: None, size_limit: 0 } | FileFormatOptions { format: Csv, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", compression: None } |         |"#,
        "+------------+------------+----------------------------------------------------------------+-----------------------------------------------+--------------------------------------------------------------------------------------------------------------------+---------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
