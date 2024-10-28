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

use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use futures_util::TryStreamExt;
use opendal::Metakey;
use opendal::Operator;

use super::parse_opt_opt_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseTable;

pub struct FuseTimeTravelSizeArgs {
    pub database_name: Option<String>,
    pub table_name: Option<String>,
}

impl From<&FuseTimeTravelSizeArgs> for TableArgs {
    fn from(args: &FuseTimeTravelSizeArgs) -> Self {
        let mut table_args = Vec::new();
        if let Some(database_name) = &args.database_name {
            table_args.push(string_literal(database_name));
        }
        if let Some(table_name) = &args.table_name {
            table_args.push(string_literal(table_name));
        }
        TableArgs::new_positioned(table_args)
    }
}

impl TryFrom<(&str, TableArgs)> for FuseTimeTravelSizeArgs {
    type Error = ErrorCode;
    fn try_from((func_name, table_args): (&str, TableArgs)) -> Result<Self> {
        let (database_name, table_name) = parse_opt_opt_args(&table_args, func_name)?;
        Ok(Self {
            database_name,
            table_name,
        })
    }
}

pub struct FuseTimeTravelSize;

pub type FuseTimeTravelSizeFunc = SimpleArgFuncTemplate<FuseTimeTravelSize>;

#[async_trait::async_trait]
impl SimpleArgFunc for FuseTimeTravelSize {
    type Args = FuseTimeTravelSizeArgs;

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("database_name", TableDataType::String),
            TableField::new("table_name", TableDataType::String),
            TableField::new(
                "time_travel_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }

    // TODO(sky): reduce access to meta service
    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        _plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let mut database_names = Vec::new();
        let mut table_names = Vec::new();
        let mut sizes = Vec::new();
        match (&args.database_name, &args.table_name) {
            (Some(database_name), Some(table_name)) => {
                let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;
                let db = catalog
                    .get_database(&ctx.get_tenant(), database_name.as_str())
                    .await?;
                let tbl = db.get_table(table_name.as_str()).await?;
                let fuse_table = FuseTable::try_from_table(tbl.as_ref())?;
                let operator = fuse_table.get_operator();
                let storage_prefix = fuse_table.get_storage_prefix();
                let size = get_time_travel_size(storage_prefix, &operator).await?;
                database_names.push(database_name.clone());
                table_names.push(table_name.clone());
                sizes.push(size);
            }
            (Some(database_name), None) => {
                let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;
                let database = catalog
                    .get_database(&ctx.get_tenant(), database_name.as_str())
                    .await?;
                let tables = database.list_tables().await?;
                for tbl in tables {
                    let Ok(fuse_table) = FuseTable::try_from_table(tbl.as_ref()) else {
                        continue;
                    };
                    let operator = fuse_table.get_operator();
                    let storage_prefix = fuse_table.get_storage_prefix();
                    let size = get_time_travel_size(storage_prefix, &operator).await?;
                    database_names.push(database_name.clone());
                    table_names.push(tbl.name().to_string());
                    sizes.push(size);
                }
            }
            (None, None) => {
                let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;
                let databases = catalog.list_databases(&ctx.get_tenant()).await?;
                for db in databases {
                    let tables = match db.list_tables().await {
                        Ok(tables) => tables,
                        Err(e) if e.code() == ErrorCode::UNIMPLEMENTED => {
                            continue;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };
                    for tbl in tables {
                        let Ok(fuse_table) = FuseTable::try_from_table(tbl.as_ref()) else {
                            continue;
                        };
                        let operator = fuse_table.get_operator();
                        let storage_prefix = fuse_table.get_storage_prefix();
                        let size = get_time_travel_size(storage_prefix, &operator).await?;
                        database_names.push(db.name().to_string());
                        table_names.push(tbl.name().to_string());
                        sizes.push(size);
                    }
                }
            }
            (None, Some(_)) => unreachable!(),
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(database_names),
            StringType::from_data(table_names),
            UInt64Type::from_data(sizes),
        ]))
    }
}

async fn get_time_travel_size(storage_prefix: &str, op: &Operator) -> Result<u64> {
    let mut lister = op
        .lister_with(storage_prefix)
        .recursive(true)
        .metakey(Metakey::ContentLength)
        .await?;
    let mut size = 0;
    while let Some(entry) = lister.try_next().await? {
        size += entry.metadata().content_length();
    }
    Ok(size)
}
