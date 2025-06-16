// Copyright 2021 Datafuse Labs
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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_storage::DataOperator;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::meta::TEMP_TABLE_STORAGE_PREFIX;
use futures_util::TryStreamExt;
use log::info;

use crate::sessions::TableContext;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::TableArgs;
pub struct FuseVacuumTemporaryTable;

#[async_trait::async_trait]
impl SimpleTableFunc for FuseVacuumTemporaryTable {
    fn get_engine_name(&self) -> String {
        "fuse_vacuum_temporary_table".to_owned()
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("result", TableDataType::String)])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let op = DataOperator::instance().operator();
        let mut lister = op
            .lister_with(TEMP_TABLE_STORAGE_PREFIX)
            .recursive(true)
            .await?;
        let client_session_mgr = UserApiProvider::instance().client_session_api(&ctx.get_tenant());
        let mut user_session_ids = HashSet::new();
        while let Some(entry) = lister.try_next().await? {
            let path = entry.path();
            let parts: Vec<_> = path.split('/').collect();
            if parts.len() < 3 {
                return Err(ErrorCode::Internal(format!(
                    "invalid path for temp table: {path}"
                )));
            };
            user_session_ids.insert((parts[1].to_string(), parts[2].to_string()));
        }
        for (user_name, session_id) in user_session_ids {
            if client_session_mgr
                .get_client_session(&user_name, &session_id)
                .await?
                .is_none()
            {
                let path = format!("{}/{}/{}", TEMP_TABLE_STORAGE_PREFIX, user_name, session_id);
                info!(
                    "[TEMP TABLE] session={session_id} vacuum temporary table: {}",
                    path
                );
                op.remove_all(&path).await?;
            }
        }
        let col: Vec<String> = vec!["Ok".to_owned()];

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(col),
        ])))
    }

    fn create(_func_name: &str, _table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        Ok(Self)
    }
}
