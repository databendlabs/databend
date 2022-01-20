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
use common_meta_types::AuthInfo;
use common_meta_types::AuthType;
use common_meta_types::PasswordHashMethod;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserQuota;
use databend_query::storages::system::UsersTable;
use databend_query::storages::Table;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_users_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let tenant = ctx.get_tenant();
    ctx.get_settings().set_max_threads(2)?;
    let auth_data = AuthInfo::None;
    ctx.get_user_manager()
        .add_user(&tenant, UserInfo {
            auth_info: auth_data,
            name: "test".to_string(),
            hostname: "localhost".to_string(),
            grants: UserGrantSet::empty(),
            quota: UserQuota::no_limit(),
        })
        .await?;
    let auth_data = AuthInfo::Password {
        hash_value: Vec::from("123456789"),
        hash_method: PasswordHashMethod::PlainText,
    };
    ctx.get_user_manager()
        .add_user(&tenant, UserInfo {
            auth_info: auth_data,
            name: "test1".to_string(),
            hostname: "%".to_string(),
            grants: UserGrantSet::empty(),
            quota: UserQuota::no_limit(),
        })
        .await?;
    let auth_data = AuthInfo::new(AuthType::Sha256Password, &Some("123456789".to_string()));
    assert!(auth_data.is_ok());
    ctx.get_user_manager()
        .add_user(&tenant, UserInfo {
            auth_info: auth_data.unwrap(),
            name: "test2".to_string(),
            hostname: "%".to_string(),
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
        "+-------+-----------+--------------------+------------------------------------------------------------------+",
        "| name  | hostname  | auth_type          | auth_string                                                      |",
        "+-------+-----------+--------------------+------------------------------------------------------------------+",
        "| test  | localhost | no_password        |                                                                  |",
        "| test1 | %         | plaintext_password | 123456789                                                        |",
        "| test2 | %         | sha256_password    | 15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225 |",
        "+-------+-----------+--------------------+------------------------------------------------------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
