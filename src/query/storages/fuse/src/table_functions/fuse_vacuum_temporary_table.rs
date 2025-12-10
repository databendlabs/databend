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
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
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

#[async_backtrace::framed]
pub async fn vacuum_inactive_temp_tables(
    ctx: &Arc<dyn TableContext>,
    limit: Option<u64>,
) -> Result<usize> {
    let op = DataOperator::instance().operator();
    let mut lister = op
        .lister_with(TEMP_TABLE_STORAGE_PREFIX)
        .recursive(true)
        .await?;

    let client_session_mgr = UserApiProvider::instance().client_session_api(&ctx.get_tenant());
    let mut user_session_ids = HashSet::new();
    let mut inactive_user_session_ids = Vec::new();
    let session_limit = limit.unwrap_or(u64::MAX) as usize;

    if session_limit == 0 {
        return Ok(0);
    }

    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().is_dir() {
            continue;
        }
        let path = entry.path();
        let parts: Vec<_> = path.split('/').collect();
        if parts.len() < 3 {
            return Err(ErrorCode::Internal(format!(
                "invalid path for temp table: {path}"
            )));
        };
        let user_name = parts[1].to_string();
        let session_id = parts[2].to_string();
        if user_session_ids.contains(&(user_name.clone(), session_id.clone())) {
            continue;
        }
        user_session_ids.insert((user_name.clone(), session_id.clone()));
        if client_session_mgr
            .get_client_session(&user_name, &session_id)
            .await?
            .is_none()
        {
            inactive_user_session_ids.push((user_name, session_id));
            if inactive_user_session_ids.len() >= session_limit {
                break;
            }
        }
    }

    let mut session_num = 0;

    for (user_name, session_id) in inactive_user_session_ids {
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
            session_num += 1;
        }
    }

    Ok(session_num)
}

pub struct FuseVacuumTemporaryTable {
    limit: Option<u64>,
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseVacuumTemporaryTable {
    fn get_engine_name(&self) -> String {
        "fuse_vacuum_temporary_table".to_owned()
    }

    fn table_args(&self) -> Option<TableArgs> {
        self.limit.map(|limit| {
            TableArgs::new_positioned(vec![databend_common_catalog::table_args::u64_literal(
                limit,
            )])
        })
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("result", TableDataType::String)])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let session_num = vacuum_inactive_temp_tables(ctx, self.limit).await?;
        let col: Vec<String> = vec![format!(
            "Ok: processed temporary tables from {} inactive sessions",
            session_num
        )];

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(col),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let limit = match table_args.positioned.len() {
            0 => None,
            1 => {
                let args = table_args.expect_all_positioned(func_name, Some(1))?;
                let limit_val = match &args[0] {
                    Scalar::Number(NumberScalar::UInt64(val)) => *val,
                    Scalar::Number(NumberScalar::UInt32(val)) => *val as u64,
                    Scalar::Number(NumberScalar::UInt16(val)) => *val as u64,
                    Scalar::Number(NumberScalar::UInt8(val)) => *val as u64,
                    Scalar::String(val) => val.parse::<u64>()?,
                    _ => {
                        return Err(ErrorCode::BadArguments(format!(
                            "invalid value {:?} expect to be unsigned integer literal.",
                            args[0]
                        )))
                    }
                };
                Some(limit_val)
            }
            _ => {
                return Err(ErrorCode::NumberArgumentsNotMatch(
                    "Expected 0 or 1 arguments".to_string(),
                ));
            }
        };
        Ok(Self { limit })
    }
}
