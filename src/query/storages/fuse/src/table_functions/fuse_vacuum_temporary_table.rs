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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_storage::DataOperator;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::meta::TEMP_TABLE_STORAGE_PREFIX;
use futures_util::TryStreamExt;
use log::info;

use crate::sessions::TableContext;
use crate::table_functions::SimpleTableFunc;

fn parse_temp_table_session<'a>(
    path: &'a str,
    tenant_name: &str,
) -> Result<(&'a str, &'a str, String)> {
    let parts: Vec<_> = path.split('/').collect();
    if parts.first() != Some(&TEMP_TABLE_STORAGE_PREFIX) {
        return Err(ErrorCode::Internal(format!(
            "invalid path for temp table: {path}"
        )));
    }

    // HTTP temporary tables are tenant-scoped, while MySQL temporary tables and
    // HTTP temporary tables created by older versions use the legacy layout.
    let user_index = if parts.get(1) == Some(&tenant_name) {
        2
    } else {
        1
    };
    let session_index = user_index + 1;
    if parts.len() <= session_index {
        return Err(ErrorCode::Internal(format!(
            "invalid path for temp table: {path}"
        )));
    }

    let session_path = parts[..=session_index].join("/");
    Ok((parts[user_index], parts[session_index], session_path))
}

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

    let tenant = ctx.get_tenant();
    let client_session_mgr = UserApiProvider::instance().client_session_api(&tenant);
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
        let (user_name, session_id, session_path) =
            parse_temp_table_session(path, tenant.tenant_name())?;
        let user_session = (user_name.to_string(), session_id.to_string(), session_path);
        if user_session_ids.contains(&user_session) {
            continue;
        }
        user_session_ids.insert(user_session.clone());
        if client_session_mgr
            .get_client_session(user_name, session_id)
            .await?
            .is_none()
        {
            inactive_user_session_ids.push(user_session);
            if inactive_user_session_ids.len() >= session_limit {
                break;
            }
        }
    }

    let mut session_num = 0;

    for (user_name, session_id, session_path) in inactive_user_session_ids {
        if client_session_mgr
            .get_client_session(&user_name, &session_id)
            .await?
            .is_none()
        {
            info!(
                "[TEMP TABLE] session={session_id} vacuum temporary table: {}",
                session_path
            );
            op.remove_all(&session_path).await?;
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
                        )));
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

#[cfg(test)]
mod tests {
    use super::parse_temp_table_session;

    #[test]
    fn test_parse_temp_table_session() {
        assert_eq!(
            (
                "root",
                "http-session",
                "_tmp_tbl/tenant_a/root/http-session".to_string()
            ),
            parse_temp_table_session(
                "_tmp_tbl/tenant_a/root/http-session/table/block.parquet",
                "tenant_a"
            )
            .unwrap()
        );
        assert_eq!(
            (
                "root",
                "legacy-session",
                "_tmp_tbl/root/legacy-session".to_string()
            ),
            parse_temp_table_session(
                "_tmp_tbl/root/legacy-session/table/block.parquet",
                "tenant_a"
            )
            .unwrap()
        );
    }

    #[test]
    fn test_rejects_invalid_temp_table_path() {
        assert!(parse_temp_table_session("_tmp_tbl/root", "tenant_a").is_err());
        assert!(parse_temp_table_session("other/root/session/block.parquet", "tenant_a").is_err());
    }
}
