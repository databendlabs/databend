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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DummyColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use log::warn;

use crate::columns_table::dump_tables;
use crate::generate_catalog_meta;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct StatisticsTable {
    table_info: TableInfo,
}

#[derive(Default)]
struct TableColumnStatistics {
    database_name: String,
    table_name: String,
    column_name: String,

    stats_row_count: Option<u64>,
    actual_row_count: Option<u64>,
    distinct_count: Option<u64>,
    null_count: Option<u64>,
    min: Option<String>,
    max: Option<String>,
    avg_size: Option<u64>,
    histogram: String,
}

impl StatisticsTable {
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("column_name", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new(
                "stats_row_count",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "actual_row_count",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "distinct_count",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "null_count",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new("min", TableDataType::String.wrap_nullable()),
            TableField::new("max", TableDataType::String.wrap_nullable()),
            TableField::new(
                "avg_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new("histogram", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'statistics'".to_string(),
            name: "statistics".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemStatistics".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_catalog_meta(ctl_name),
                ..Default::default()
            }),
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(StatisticsTable { table_info })
    }

    #[async_backtrace::framed]
    async fn dump_table_columns(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        catalog: &Arc<dyn Catalog>,
    ) -> Result<Vec<TableColumnStatistics>> {
        let database_and_tables = dump_tables(&ctx, push_downs, catalog).await?;
        let mut rows: Vec<TableColumnStatistics> = vec![];
        for (database, tables) in database_and_tables {
            for table in tables {
                match table.engine() {
                    VIEW_ENGINE => {
                        let fields = if let Some(query) = table.options().get(QUERY) {
                            let mut planner = Planner::new(ctx.clone());
                            match planner.plan_sql(query).await {
                                Ok((plan, _)) => {
                                    infer_table_schema(&plan.schema())?.fields().clone()
                                }
                                Err(e) => {
                                    // If VIEW SELECT QUERY plan err, should return empty. not destroy the query.
                                    warn!(
                                        "failed to get columns for {}: {}",
                                        table.get_table_info().desc,
                                        e
                                    );
                                    vec![]
                                }
                            }
                        } else {
                            vec![]
                        };
                        for field in fields {
                            rows.push(TableColumnStatistics {
                                database_name: database.clone(),
                                table_name: table.name().into(),
                                column_name: field.name,
                                ..Default::default()
                            })
                        }
                    }
                    _ => {
                        let schema = table.schema();
                        // attach table should not collect statistics, source table column already collect them.
                        let columns_statistics = if !FuseTable::is_table_attached(
                            &table.get_table_info().meta.options,
                        ) {
                            table
                                .column_statistics_provider(ctx.clone())
                                .await
                                .unwrap_or_else(|e| {
                                    let msg = format!(
                                        "Collect {}.{}.{} column statistics with error: {}",
                                        catalog.name(),
                                        database,
                                        table.name(),
                                        e
                                    );
                                    warn!("{}", msg);
                                    ctx.push_warning(msg);
                                    Box::new(DummyColumnStatisticsProvider)
                                })
                        } else {
                            Box::new(DummyColumnStatisticsProvider)
                        };
                        let stats_row_count = columns_statistics.stats_num_rows();
                        let actual_row_count = columns_statistics.num_rows();
                        for field in schema.fields() {
                            let column_id = field.column_id;
                            let column_statistics = columns_statistics.column_statistics(column_id);
                            let his_info = columns_statistics.histogram(column_id);
                            let histogram = if let Some(his_info) = his_info {
                                let mut his_infos = vec![];
                                for (i, bucket) in his_info.buckets.iter().enumerate() {
                                    let min = bucket.lower_bound().to_string()?;
                                    let max = bucket.upper_bound().to_string()?;
                                    let ndv = bucket.num_distinct();
                                    let count = bucket.num_values();
                                    let his_info = format!(
                                        "[bucket id: {:?}, min: {:?}, max: {:?}, ndv: {:?}, count: {:?}]",
                                        i, min, max, ndv, count
                                    );
                                    his_infos.push(his_info);
                                }
                                his_infos.join(", ")
                            } else {
                                "".to_string()
                            };
                            rows.push(TableColumnStatistics {
                                database_name: database.clone(),
                                table_name: table.name().into(),
                                column_name: field.name().clone(),
                                stats_row_count,
                                actual_row_count,
                                distinct_count: column_statistics.and_then(|v| v.ndv),
                                null_count: column_statistics.map(|v| v.null_count),
                                min: column_statistics
                                    .and_then(|s| s.min.clone())
                                    .map(|v| v.to_string().unwrap()),
                                max: column_statistics
                                    .and_then(|s| s.max.clone())
                                    .map(|v| v.to_string().unwrap()),
                                avg_size: columns_statistics.average_size(column_id),
                                histogram,
                            })
                        }
                        // add virtual column statistics
                        let table_info = table.get_table_info();
                        if let Some(virtual_schema) = &table_info.meta.virtual_schema {
                            for virtual_field in virtual_schema.fields() {
                                if let (Ok(source_field), Some(column_statistics)) = (
                                    schema.field_of_column_id(virtual_field.source_column_id),
                                    columns_statistics.column_statistics(virtual_field.column_id),
                                ) {
                                    let column_name =
                                        format!("{}{}", source_field.name, virtual_field.name);
                                    rows.push(TableColumnStatistics {
                                        database_name: database.clone(),
                                        table_name: table.name().into(),
                                        column_name,
                                        stats_row_count,
                                        actual_row_count,
                                        distinct_count: column_statistics.ndv,
                                        null_count: Some(column_statistics.null_count),
                                        min: column_statistics
                                            .min
                                            .clone()
                                            .map(|v| v.to_string().unwrap()),
                                        max: column_statistics
                                            .max
                                            .clone()
                                            .map(|v| v.to_string().unwrap()),
                                        avg_size: columns_statistics
                                            .average_size(virtual_field.column_id),
                                        histogram: "".to_string(),
                                    })
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl AsyncSystemTable for StatisticsTable {
    const NAME: &'static str = "system.statistics";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr
            .get_catalog(
                ctx.get_tenant().tenant_name(),
                self.get_table_info().catalog(),
                ctx.session_state()?,
            )
            .await?;
        let rows = self.dump_table_columns(ctx, push_downs, &catalog).await?;

        let mut names = Vec::with_capacity(rows.len());
        let mut databases = Vec::with_capacity(rows.len());
        let mut tables = Vec::with_capacity(rows.len());
        let mut stats_row_counts = Vec::with_capacity(rows.len());
        let mut actual_row_counts = Vec::with_capacity(rows.len());
        let mut distinct_counts = Vec::with_capacity(rows.len());
        let mut null_counts = Vec::with_capacity(rows.len());
        let mut mins = Vec::with_capacity(rows.len());
        let mut maxes = Vec::with_capacity(rows.len());
        let mut avg_sizes = Vec::with_capacity(rows.len());
        let mut histograms = Vec::with_capacity(rows.len());
        for row in rows {
            names.push(row.column_name);
            databases.push(row.database_name);
            tables.push(row.table_name);
            stats_row_counts.push(row.stats_row_count);
            actual_row_counts.push(row.actual_row_count);
            distinct_counts.push(row.distinct_count);
            null_counts.push(row.null_count);
            mins.push(row.min);
            maxes.push(row.max);
            avg_sizes.push(row.avg_size);
            histograms.push(row.histogram);
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(databases),
            StringType::from_data(tables),
            UInt64Type::from_opt_data(stats_row_counts),
            UInt64Type::from_opt_data(actual_row_counts),
            UInt64Type::from_opt_data(distinct_counts),
            UInt64Type::from_opt_data(null_counts),
            StringType::from_opt_data(mins),
            StringType::from_opt_data(maxes),
            UInt64Type::from_opt_data(avg_sizes),
            StringType::from_data(histograms),
        ]))
    }
}
