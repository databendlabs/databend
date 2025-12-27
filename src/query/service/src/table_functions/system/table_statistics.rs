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
use databend_common_catalog::table_args::string_value;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::read::SnapshotHistoryReader;
use databend_common_storages_fuse::table_functions::SimpleTableFunc;
use databend_common_storages_fuse::table_functions::string_literal;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures::stream::StreamExt;
use log::warn;

use crate::sessions::TableContext;

pub struct TableStatisticsArgs {
    database_name: String,
    table_name: Option<String>,
}

pub struct TableStatisticsFunc {
    args: TableStatisticsArgs,
}

impl From<&TableStatisticsArgs> for TableArgs {
    fn from(args: &TableStatisticsArgs) -> Self {
        let mut positional_args = vec![string_literal(&args.database_name)];

        if let Some(tbl) = &args.table_name {
            positional_args.push(string_literal(tbl));
        }

        TableArgs::new_positioned(positional_args)
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for TableStatisticsFunc {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new("engine", TableDataType::String),
            TableField::new("statistics_json", TableDataType::String),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let tenant_id = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;

        let mut db_names = Vec::new();
        let mut table_names = Vec::new();
        let mut engines = Vec::new();
        let mut stats_jsons = Vec::new();

        // If a specific table is specified
        if let Some(table_name) = &self.args.table_name {
            // Directly get the specified table
            if let Ok(tbl) = catalog
                .get_table(&tenant_id, &self.args.database_name, table_name)
                .await
            {
                let engine = tbl.get_table_info().engine().to_string();
                match engine.to_lowercase().as_str() {
                    "fuse" => {
                        if let Some(stats) = get_fuse_table_statistics(tbl).await? {
                            db_names.push(self.args.database_name.clone());
                            table_names.push(table_name.clone());
                            engines.push(engine.clone());
                            stats_jsons.push(stats);
                        }
                    }
                    _ => {
                        warn!(
                            "TableStatistics: Database {} Table {} is not a Fuse table",
                            self.args.database_name, table_name
                        );
                    }
                }
            }
        }
        // If only database is specified
        else {
            // Get all tables in the specified database
            if let Ok(db) = catalog
                .get_database(&tenant_id, &self.args.database_name)
                .await
            {
                let tables = db.list_tables().await?;

                // Process each table
                for table_info in tables {
                    let table_name = table_info.name().to_string();

                    // Get table object
                    if let Ok(tbl) = catalog
                        .get_table(&tenant_id, &self.args.database_name, &table_name)
                        .await
                    {
                        let engine = tbl.get_table_info().engine().to_string();
                        match engine.to_lowercase().as_str() {
                            "fuse" => {
                                if let Some(stats) = get_fuse_table_statistics(tbl).await? {
                                    db_names.push(self.args.database_name.clone());
                                    table_names.push(table_name.clone());
                                    engines.push(engine.clone());
                                    stats_jsons.push(stats);
                                }
                            }
                            _ => {
                                warn!(
                                    "TableStatistics: Database {} Table {} is not a Fuse table",
                                    self.args.database_name, table_name
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(db_names),
            StringType::from_data(table_names),
            StringType::from_data(engines),
            StringType::from_data(stats_jsons),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        // Require at least database name parameter
        if table_args.positioned.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "Function {} requires at least one argument: database name",
                func_name
            )));
        }

        match table_args.positioned.len() {
            1 => {
                // Only database name is provided
                let database_name = string_value(&table_args.positioned[0])?;
                Ok(Self {
                    args: TableStatisticsArgs {
                        database_name,
                        table_name: None,
                    },
                })
            }
            2 => {
                // Both database and table name are provided
                let database_name = string_value(&table_args.positioned[0])?;
                let table_name = string_value(&table_args.positioned[1])?;
                Ok(Self {
                    args: TableStatisticsArgs {
                        database_name,
                        table_name: Some(table_name),
                    },
                })
            }
            _ => Err(ErrorCode::BadArguments(format!(
                "Function {} takes 1 or 2 arguments: database_name, [table_name]",
                func_name
            ))),
        }
    }
}

pub async fn get_fuse_table_snapshot(
    table: Arc<dyn databend_common_catalog::table::Table>,
) -> Result<Option<Arc<TableSnapshot>>> {
    if let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) {
        let meta_location_generator = fuse_table.meta_location_generator().clone();
        let snapshot_location = fuse_table.snapshot_loc();

        if let Some(snapshot_location) = snapshot_location {
            let table_snapshot_reader =
                MetaReaders::table_snapshot_reader(fuse_table.get_operator());
            let format_version =
                TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());

            let lite_snapshot_stream = table_snapshot_reader.snapshot_history(
                snapshot_location,
                format_version,
                meta_location_generator.clone(),
                fuse_table.get_branch_id(),
            );

            if let Some(Ok((snapshot, _v))) = lite_snapshot_stream.take(1).next().await {
                return Ok(Some(snapshot));
            }
        }
    }

    Ok(None)
}

// Get Fuse table statistics, returns JSON string
pub async fn get_fuse_table_statistics(
    table: Arc<dyn databend_common_catalog::table::Table>,
) -> Result<Option<String>> {
    // Try to convert to FuseTable
    if let Some(table_snapshot) = get_fuse_table_snapshot(table).await? {
        // Convert snapshot to JSON, but filter out segments field
        let mut snapshot_json = serde_json::to_value(&table_snapshot)?;

        // If it's an object and contains segments field, remove it
        if let serde_json::Value::Object(ref mut obj) = snapshot_json {
            obj.remove("segments");
        }

        // Convert JSON to variant
        return Ok(Some(serde_json::to_string(&snapshot_json)?));
    }

    Ok(None)
}
