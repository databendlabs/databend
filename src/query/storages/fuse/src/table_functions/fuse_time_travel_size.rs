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

use databend_common_base::base::tokio;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
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
use log::info;
use opendal::Operator;

use super::parse_opt_opt_args;
use crate::io::SnapshotsIO;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseTable;
use crate::FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS;

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
            TableField::new("is_dropped", TableDataType::Boolean),
            TableField::new(
                "time_travel_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "latest_snapshot_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "data_retention_period_in_hours",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new("error", TableDataType::String.wrap_nullable()),
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
        let mut is_droppeds = Vec::new();
        let mut sizes = Vec::new();
        let mut latest_snapshot_sizes = Vec::new();
        let mut data_retention_period_in_hours = Vec::new();
        let mut errors = Vec::new();
        let mut tasks = Vec::new();
        let num_threads = ctx.get_settings().get_max_threads()? as usize;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(num_threads));
        let catalog = ctx.get_default_catalog()?;
        let dbs = match &args.database_name {
            Some(db_name) => {
                let start = std::time::Instant::now();
                let db = catalog
                    .get_database(&ctx.get_tenant(), db_name.as_str())
                    .await?;
                info!("get_database cost: {:?}", start.elapsed());
                vec![db]
            }
            None => {
                let start = std::time::Instant::now();
                let dbs = catalog.list_databases(&ctx.get_tenant()).await?;
                info!("list_databases cost: {:?}", start.elapsed());
                dbs
            }
        };
        for db in dbs {
            if db.name() == "system" || db.name() == "information_schema" {
                continue;
            }

            info!("loading tables from database : {}", db.name());
            let tables = match &args.table_name {
                Some(table_name) => {
                    let start = std::time::Instant::now();
                    info!("loading table {}", table_name);
                    let table = db.get_table_history(table_name.as_str()).await?;
                    info!("get_table cost: {:?}", start.elapsed());
                    table
                }
                None => {
                    let start = std::time::Instant::now();
                    let tables = db.list_tables_history(true).await?;
                    info!("list_tables cost: {:?}", start.elapsed());
                    tables
                }
            };

            for tbl in tables {
                let Ok(fuse_table) = FuseTable::try_from_table(tbl.as_ref()) else {
                    // ignore non-fuse tables
                    continue;
                };
                if FuseTable::is_table_attached(&tbl.get_table_info().meta.options) {
                    continue;
                }
                let table_clone = tbl.clone();
                let semaphore = semaphore.clone();
                tasks.push(async move {
                    let permit = semaphore.acquire_owned().await.map_err(|e| {
                        ErrorCode::Internal(format!(
                            "semaphore closed, acquire permit failure. {}",
                            e
                        ))
                    })?;
                    let fuse_table = FuseTable::try_from_table(table_clone.as_ref()).unwrap();
                    let (time_travel_size, latest_snapshot_size) =
                        calc_tbl_size(fuse_table).await?;
                    drop(permit);
                    Ok::<_, ErrorCode>((time_travel_size, latest_snapshot_size))
                });
                let data_retention_period_in_hour = fuse_table
                    .table_info
                    .meta
                    .options
                    .get(FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS)
                    .map(|v| v.parse::<u64>())
                    .transpose()?;
                let is_dropped = fuse_table.table_info.meta.drop_on.is_some();
                database_names.push(db.name().to_string());
                table_names.push(tbl.name().to_string());
                is_droppeds.push(is_dropped);
                data_retention_period_in_hours.push(data_retention_period_in_hour);
            }
        }

        // if use execute_futures_in_parallel(), will get error: `implementation of `std::ops::FnOnce` is not general enough`
        let results = futures::future::try_join_all(tasks).await?;
        for result in results {
            let (time_travel_size, latest_snapshot_size) = result;
            sizes.push(time_travel_size);
            match latest_snapshot_size {
                Ok(size) => {
                    latest_snapshot_sizes.push(Some(size));
                    errors.push(None);
                }
                Err(e) => {
                    latest_snapshot_sizes.push(None);
                    errors.push(Some(e.to_string()));
                }
            }
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(database_names),
            StringType::from_data(table_names),
            BooleanType::from_data(is_droppeds),
            UInt64Type::from_data(sizes),
            UInt64Type::from_opt_data(latest_snapshot_sizes),
            UInt64Type::from_opt_data(data_retention_period_in_hours),
            StringType::from_opt_data(errors),
        ]))
    }
}

async fn get_time_travel_size(storage_prefix: &str, op: &Operator) -> Result<u64> {
    let mut lister = op.lister_with(storage_prefix).recursive(true).await?;
    let mut size = 0;
    while let Some(entry) = lister.try_next().await? {
        // Skip directories while calculating size
        if entry.metadata().is_dir() {
            continue;
        }
        let mut content_length = entry.metadata().content_length();
        if content_length == 0 {
            content_length = op.stat(entry.path()).await?.content_length();
        }
        size += content_length;
    }
    Ok(size)
}

async fn calc_tbl_size(tbl: &FuseTable) -> Result<(u64, Result<u64>)> {
    info!(
        "fuse_time_travel_size start calc_tbl_size:{}",
        tbl.get_table_info().desc
    );
    let operator = tbl.get_operator();
    let storage_prefix = tbl.get_storage_prefix();
    let start = std::time::Instant::now();
    let time_travel_size = get_time_travel_size(storage_prefix, &operator).await?;
    info!("get_time_travel_size cost: {:?}", start.elapsed());
    let snapshot_location = tbl.snapshot_loc();
    let latest_snapshot_size = match snapshot_location {
        Some(snapshot_location) => {
            let start = std::time::Instant::now();
            info!("fuse_time_travel_size will read: {}", snapshot_location);
            let snapshot = SnapshotsIO::read_snapshot(snapshot_location, operator).await;
            info!("read_snapshot cost: {:?}", start.elapsed());
            snapshot.map(|(snapshot, _)| {
                snapshot.summary.compressed_byte_size + snapshot.summary.index_size
            })
        }
        None => Ok(0),
    };
    Ok((time_travel_size, latest_snapshot_size))
}
