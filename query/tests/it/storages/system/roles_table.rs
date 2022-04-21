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
use common_meta_types::RoleInfo;
use databend_query::storages::system::RolesTable;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_roles_table() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    ctx.get_settings().set_max_threads(2)?;

    {
        let role_info = RoleInfo::new("test");
        ctx.get_user_manager()
            .add_role(&tenant, role_info, false)
            .await?;
    }

    {
        let mut role_info = RoleInfo::new("test1");
        role_info.grants.grant_role("test".to_string());
        ctx.get_user_manager()
            .add_role(&tenant, role_info, false)
            .await?;
    }

    let table = RolesTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+-------+-----------------+",
        "| name  | inherited_roles |",
        "+-------+-----------------+",
        "| test  | 0               |",
        "| test1 | 1               |",
        "+-------+-----------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
