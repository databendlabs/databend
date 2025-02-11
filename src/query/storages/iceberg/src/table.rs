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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_storages_common_table_meta::table::ChangeType;
use futures::TryStreamExt;
use iceberg::io::FileIOBuilder;

use crate::partition::IcebergPartInfo;
use crate::predicate::PredicateBuilder;
use crate::statistics;
use crate::statistics::IcebergStatistics;
use crate::table_source::IcebergTableSource;
use crate::IcebergCatalog;

pub const ICEBERG_ENGINE: &str = "ICEBERG";

/// accessor wrapper as a table
#[derive(Clone)]
pub struct IcebergTable {
    info: TableInfo,

    pub table: iceberg::table::Table,
    statistics: IcebergStatistics,
}

impl IcebergTable {
    /// create a new table on the table directory
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let (table, statistics) = Self::parse_engine_options(&info.meta.engine_options)?;
        Ok(Box::new(Self {
            info,
            table,
            statistics,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: ICEBERG_ENGINE.to_string(),
            comment: "ICEBERG Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    pub async fn load_iceberg_table(
        ctl: &IcebergCatalog,
        database: &str,
        table_name: &str,
    ) -> Result<iceberg::table::Table> {
        let db_ident = iceberg::NamespaceIdent::new(database.to_string());
        let table = ctl
            .iceberg_catalog()
            .load_table(&iceberg::TableIdent::new(db_ident, table_name.to_string()))
            .await
            .map_err(|err| {
                ErrorCode::ReadTableDataError(format!("Iceberg catalog load failed: {err:?}"))
            })?;
        Ok(table)
    }

    pub fn get_schema(table: &iceberg::table::Table) -> Result<TableSchema> {
        let meta = table.metadata();

        // Build arrow schema from iceberg metadata.
        let arrow_schema: ArrowSchema = meta.current_schema().as_ref().try_into().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;
        TableSchema::try_from(&arrow_schema)
    }

    /// build_engine_options will generate `engine_options` from [`iceberg::table::Table`] so that
    /// we can distribute it across nodes and rebuild this table without loading from catalog again.
    ///
    /// We will never persist the `engine_options` to storage, so it's safe to change the implementation.
    /// As long as you make sure both [`build_engine_options`] and [`parse_engine_options`] been updated.
    pub fn build_engine_options(
        table: &iceberg::table::Table,
        statistics: &statistics::IcebergStatistics,
    ) -> Result<BTreeMap<String, String>> {
        let (file_io_scheme, file_io_props) = table.file_io().clone().into_builder().into_parts();
        let file_io_props = serde_json::to_string(&file_io_props)?;
        let metadata_location = table
            .metadata_location()
            .map(|v| v.to_string())
            .unwrap_or_default();
        let metadata = serde_json::to_string(table.metadata())?;
        let identifier = serde_json::to_string(table.identifier())?;
        let statistics = serde_json::to_string(statistics)?;

        Ok(BTreeMap::from_iter([
            ("iceberg.file_io.scheme".to_string(), file_io_scheme),
            ("iceberg.file_io.props".to_string(), file_io_props),
            ("iceberg.metadata_location".to_string(), metadata_location),
            ("iceberg.metadata".to_string(), metadata),
            ("iceberg.identifier".to_string(), identifier),
            ("iceberg.statistics".to_string(), statistics),
        ]))
    }

    /// parse_engine_options will parse `engine_options` to [`BTreeMap`] so that we can rebuild the table.
    ///
    /// See [`build_engine_options`] for more information.
    pub fn parse_engine_options(
        options: &BTreeMap<String, String>,
    ) -> Result<(iceberg::table::Table, statistics::IcebergStatistics)> {
        let file_io_scheme = options.get("iceberg.file_io.scheme").ok_or_else(|| {
            ErrorCode::ReadTableDataError(
                "Rebuild iceberg table failed: Missing iceberg.file_io.scheme",
            )
        })?;

        let file_io_props: HashMap<String, String> =
            serde_json::from_str(options.get("iceberg.file_io.props").ok_or_else(|| {
                ErrorCode::ReadTableDataError(
                    "Rebuild iceberg table failed: Missing iceberg.file_io.props",
                )
            })?)?;

        let metadata_location = options
            .get("iceberg.metadata_location")
            .map(|s| s.to_string())
            .unwrap_or_default();

        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(options.get("iceberg.metadata").ok_or_else(|| {
                ErrorCode::ReadTableDataError(
                    "Rebuild iceberg table failed: Missing iceberg.metadata",
                )
            })?)?;

        let identifier: iceberg::TableIdent =
            serde_json::from_str(options.get("iceberg.identifier").ok_or_else(|| {
                ErrorCode::ReadTableDataError(
                    "Rebuild iceberg table failed: Missing iceberg.identifier",
                )
            })?)?;

        let statistics: statistics::IcebergStatistics =
            serde_json::from_str(options.get("iceberg.statistics").ok_or_else(|| {
                ErrorCode::ReadTableDataError(
                    "Rebuild iceberg table failed: Missing iceberg.statistics",
                )
            })?)?;

        let file_io = FileIOBuilder::new(file_io_scheme)
            .with_props(file_io_props)
            .build()
            .map_err(|err| {
                ErrorCode::ReadTableDataError(format!(
                    "Rebuild iceberg table file io failed: {err:?}"
                ))
            })?;

        let table = iceberg::table::Table::builder()
            .identifier(identifier)
            .metadata(metadata)
            .metadata_location(metadata_location)
            .file_io(file_io)
            .build()
            .map_err(|err| {
                ErrorCode::ReadTableDataError(format!("Rebuild iceberg table failed: {err:?}"))
            })?;

        Ok((table, statistics))
    }

    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub async fn try_create_from_iceberg_catalog(
        ctl: IcebergCatalog,
        database_name: &str,
        table_name: &str,
    ) -> Result<IcebergTable> {
        let table = Self::load_iceberg_table(&ctl, database_name, table_name).await?;
        let table_schema = Self::get_schema(&table)?;
        let statistics = statistics::IcebergStatistics::parse(&table).await?;

        let engine_options = Self::build_engine_options(&table, &statistics)?;

        // construct table info
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("{database_name}.{table_name}"),
            name: table_name.to_string(),
            meta: TableMeta {
                schema: Arc::new(table_schema),
                engine: "iceberg".to_string(),
                engine_options,
                created_on: Utc::now(),
                ..Default::default()
            },
            catalog_info: ctl.info(),
            ..Default::default()
        };

        Ok(Self {
            info,
            table,
            statistics,
        })
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                IcebergTableSource::create(ctx.clone(), output, output_schema.clone(), self.clone())
            },
            max_threads.max(1),
        )
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let mut scan = self.table.scan();

        if let Some(push_downs) = &push_downs {
            if let Some(projection) = &push_downs.projection {
                scan = scan.select(
                    projection
                        .project_schema(&self.schema())
                        .fields
                        .iter()
                        .map(|v| v.name.clone()),
                );
            }
            if let Some(filter) = &push_downs.filters {
                let predicate = PredicateBuilder::default().build(&filter.filter);
                scan = scan.with_filter(predicate)
            }
        }

        let tasks: Vec<_> = scan
            .build()
            .map_err(|err| ErrorCode::Internal(format!("iceberg table scan build: {err:?}")))?
            .plan_files()
            .await
            .map_err(|err| ErrorCode::Internal(format!("iceberg table scan plan: {err:?}")))?
            .try_collect()
            .await
            .map_err(|err| ErrorCode::Internal(format!("iceberg table scan collect: {err:?}")))?;

        let mut read_rows = 0;
        let mut read_bytes = 0;
        let total_files = tasks.len();
        let parts: Vec<_> = tasks
            .into_iter()
            .map(|v: iceberg::scan::FileScanTask| {
                read_rows += v.record_count.unwrap_or_default() as usize;
                read_bytes += v.length as usize;
                Arc::new(Box::new(IcebergPartInfo::new(v)) as Box<dyn PartInfo>)
            })
            .collect();

        Ok((
            PartStatistics::new_estimated(None, read_rows, read_bytes, parts.len(), total_files),
            Partitions::create(PartitionsShuffleKind::Mod, parts),
        ))
    }
}

#[async_trait]
impl Table for IcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _require_fresh: bool,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        let table = self.table.clone();
        if table.metadata().current_snapshot().is_none() {
            return Ok(None);
        };

        let mut statistics = TableStatistics::default();
        statistics.num_rows = Some(self.statistics.record_count);
        statistics.data_size_compressed = Some(self.statistics.file_size_in_bytes);
        statistics.number_of_segments = Some(self.statistics.number_of_manifest_files);
        statistics.number_of_blocks = Some(self.statistics.number_of_data_files);

        Ok(Some(statistics))
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        Ok(Box::new(self.statistics.clone()))
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        // TODO: we will support dry run later.
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn support_prewhere(&self) -> bool {
        false
    }
}
