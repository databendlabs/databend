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
use common_meta_types::AuthType;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilege;
use common_meta_types::UserQuota;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::catalogs::Table;
use crate::catalogs::ToReadDataSourcePlan;
use crate::storages::system::UsersTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_users_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    ctx.get_settings().set_max_threads(2)?;
    ctx.get_sessions_manager()
        .get_user_manager()
        .add_user(UserInfo {
            name: "test".to_string(),
            hostname: "localhost".to_string(),
            password: Vec::from(""),
            auth_type: AuthType::None,
            privileges: UserPrivilege::empty(),
            grants: UserGrantSet::empty(),
            quota: UserQuota::no_limit(),
        })
        .await?;
    ctx.get_sessions_manager()
        .get_user_manager()
        .add_user(UserInfo {
            name: "test1".to_string(),
            hostname: "%".to_string(),
            password: Vec::from("123456789"),
            auth_type: AuthType::PlainText,
            privileges: UserPrivilege::empty(),
            grants: UserGrantSet::empty(),
            quota: UserQuota::no_limit(),
        })
        .await?;

    let table: Arc<dyn Table> = Arc::new(UsersTable::create(1));
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    let expected = vec![
        "+-------+-----------+-----------+-----------+",
        "| name  | hostname  | password  | auth_type |",
        "+-------+-----------+-----------+-----------+",
        "| test  | localhost |           | 0         |",
        "| test1 | %         | 123456789 | 1         |",
        "+-------+-----------+-----------+-----------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
