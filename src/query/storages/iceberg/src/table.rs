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

use arrow_schema::Schema;
use async_trait::async_trait;
use chrono::Utc;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataSchema;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_storages_orc::ORCSource;
use databend_common_storages_orc::StripeDecoder;
use databend_common_storages_parquet::ParquetReaderBuilder;
use databend_common_storages_parquet::ParquetSource;
use databend_common_storages_parquet::ParquetSourceType;
use databend_storages_common_table_meta::table::ChangeType;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIOBuilder;

use crate::partition::convert_file_scan_task;
use crate::predicate::PredicateBuilder;
use crate::statistics;
use crate::statistics::IcebergStatistics;

const ICEBERG_TABLE_FORMAT_OPT: &str = "write.format.default";
const ICEBERG_TABLE_FORMAT_OPT_ORC: &str = "orc";
const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

pub const ICEBERG_ENGINE: &str = "ICEBERG";

/// accessor wrapper as a table
#[derive(Clone)]
pub struct IcebergTable {
    info: TableInfo,

    pub table: iceberg::table::Table,
    // None means current_snapshot_id
    pub snapshot_id: Option<i64>,
    statistics: IcebergStatistics,
}

impl IcebergTable {
    /// create a new table on the table directory
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let (table, statistics) = Self::parse_engine_options(&info.meta.engine_options)?;
        Ok(Box::new(Self {
            info,
            table,
            snapshot_id: None,
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
        ctl: Arc<dyn iceberg::Catalog>,
        database: &str,
        table_name: &str,
    ) -> Result<iceberg::table::Table> {
        let db_ident = iceberg::NamespaceIdent::new(database.to_string());
        let table = ctl
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
        let arrow_schema = schema_to_arrow_schema(meta.current_schema().as_ref()).map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;
        let mut fields = Vec::with_capacity(arrow_schema.fields().len());

        for arrow_f in arrow_schema.fields().iter() {
            let field_id: ColumnId = arrow_f
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(|s| s.parse::<ColumnId>())
                .transpose()?
                .unwrap_or(0);
            let mut field = TableField::try_from(arrow_f.as_ref())?;
            field.column_id = field_id;
            fields.push(field);
        }
        Ok(TableSchema {
            fields,
            metadata: arrow_schema.metadata().clone().into_iter().collect(),
            next_column_id: 0,
        })
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

    pub fn try_from_table(tbl: &dyn Table) -> Result<&Self> {
        tbl.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "expects table of engine iceberg, but got {}",
                tbl.engine()
            ))
        })
    }

    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub async fn try_create_from_iceberg_catalog(
        ctl: Arc<dyn iceberg::Catalog>,
        catalog_info: Arc<CatalogInfo>,
        database_name: &str,
        table_name: &str,
    ) -> Result<IcebergTable> {
        let table = Self::load_iceberg_table(ctl, database_name, table_name).await?;
        let table_schema = Self::get_schema(&table)?;
        let statistics = statistics::IcebergStatistics::parse(&table).await?;

        let engine_options = Self::build_engine_options(&table, &statistics)?;
        // Let's prepare the parquet schema for the table

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
            catalog_info,
            ..Default::default()
        };

        Ok(Self {
            info,
            table,
            snapshot_id: None,
            statistics,
        })
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let internal_columns = plan
            .internal_columns
            .as_ref()
            .map(|m| {
                m.values()
                    .map(|i| i.column_type.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let table_schema = self.info.schema();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let op = self.table.file_io();
        if let Some(true) = self
            .table
            .metadata()
            .properties()
            .get(ICEBERG_TABLE_FORMAT_OPT)
            .map(|format| format.as_str() == ICEBERG_TABLE_FORMAT_OPT_ORC)
        {
            let projection =
                PushDownInfo::projection_of_push_downs(&table_schema, plan.push_downs.as_ref());
            let data_schema: DataSchema = Arc::new(projection.project_schema(&table_schema)).into();
            let arrow_schema = Arc::new(Self::convert_orc_schema(&Schema::from(&data_schema)));
            let data_schema = Arc::new(data_schema);
            pipeline.add_source(
                |output| {
                    ORCSource::try_create_with_schema(
                        output,
                        ctx.clone(),
                        Arc::new(op.clone()),
                        arrow_schema.clone(),
                        None,
                        projection.clone(),
                    )
                },
                max_threads,
            )?;
            pipeline.try_resize(max_threads)?;
            pipeline.add_accumulating_transformer(|| {
                StripeDecoder::new(
                    ctx.clone(),
                    data_schema.clone(),
                    arrow_schema.clone(),
                    vec![],
                )
            });
        } else {
            let arrow_schema: Schema = table_schema.as_ref().into();
            let need_row_number = internal_columns.contains(&InternalColumnType::FileRowNumber);
            let topk = plan
                .push_downs
                .as_ref()
                .and_then(|p| p.top_k(&self.schema()));
            let read_options = ParquetReadOptions::default()
                .with_prune_row_groups(true)
                .with_prune_pages(false);
            let op = Arc::new(op.clone());
            let mut builder = ParquetReaderBuilder::create(
                ctx.clone(),
                op.clone(),
                table_schema.clone(),
                arrow_schema.clone(),
            )?
            .with_options(read_options)
            .with_push_downs(plan.push_downs.as_ref());

            if !need_row_number {
                builder = builder.with_topk(topk.as_ref());
            }

            let row_group_reader = Arc::new(
                builder.build_row_group_reader(ParquetSourceType::Iceberg, need_row_number)?,
            );

            let topk = Arc::new(topk);

            pipeline.add_source(
                |output| {
                    ParquetSource::create(
                        ctx.clone(),
                        ParquetSourceType::Iceberg,
                        output,
                        row_group_reader.clone(),
                        None,
                        topk.clone(),
                        internal_columns.clone(),
                        plan.push_downs.clone(),
                        table_schema.clone(),
                        op.clone(),
                    )
                },
                max_threads,
            )?
        }
        Ok(())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        // Some means navigate is valid
        let mut scan = if let Some(snapshot_id) = self.snapshot_id {
            let scan = self.table.scan();
            scan.snapshot_id(snapshot_id)
        } else {
            self.table.scan()
        };

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
                let (_, predicate) = PredicateBuilder::build(&filter.filter);
                scan = scan.with_filter(predicate)
            }
        }

        let tasks: Vec<_> = scan
            .with_delete_file_processing_enabled(true)
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
                Arc::new(convert_file_scan_task(v))
            })
            .collect();

        Ok((
            PartStatistics::new_exact(read_rows, read_bytes, parts.len(), total_files),
            Partitions::create(PartitionsShuffleKind::Mod, parts),
        ))
    }

    fn convert_orc_schema(schema: &Schema) -> Schema {
        fn visit_field(field: &arrow_schema::FieldRef) -> arrow_schema::FieldRef {
            Arc::new(
                arrow_schema::Field::new(
                    field.name(),
                    visit_type(field.data_type()),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            )
        }

        // orc-rust is not compatible with UTF8 View
        fn visit_type(ty: &arrow_schema::DataType) -> arrow_schema::DataType {
            match ty {
                arrow_schema::DataType::Utf8View => arrow_schema::DataType::Utf8,
                arrow_schema::DataType::List(field) => {
                    arrow_schema::DataType::List(visit_field(field))
                }
                arrow_schema::DataType::ListView(field) => {
                    arrow_schema::DataType::ListView(visit_field(field))
                }
                arrow_schema::DataType::FixedSizeList(field, len) => {
                    arrow_schema::DataType::FixedSizeList(visit_field(field), *len)
                }
                arrow_schema::DataType::LargeList(field) => {
                    arrow_schema::DataType::LargeList(visit_field(field))
                }
                arrow_schema::DataType::LargeListView(field) => {
                    arrow_schema::DataType::LargeListView(visit_field(field))
                }
                arrow_schema::DataType::Struct(fields) => {
                    let visited_fields = fields.iter().map(visit_field).collect::<Vec<_>>();
                    arrow_schema::DataType::Struct(arrow_schema::Fields::from(visited_fields))
                }
                arrow_schema::DataType::Union(fields, mode) => {
                    let (ids, fields): (Vec<_>, Vec<_>) = fields
                        .iter()
                        .map(|(i, field)| (i, visit_field(field)))
                        .unzip();
                    arrow_schema::DataType::Union(
                        arrow_schema::UnionFields::new(ids, fields),
                        *mode,
                    )
                }
                arrow_schema::DataType::Dictionary(key, value) => {
                    arrow_schema::DataType::Dictionary(
                        Box::new(visit_type(key)),
                        Box::new(visit_type(value)),
                    )
                }
                arrow_schema::DataType::Map(field, v) => {
                    arrow_schema::DataType::Map(visit_field(field), *v)
                }
                ty => {
                    debug_assert!(!ty.is_nested());
                    ty.clone()
                }
            }
        }

        let fields = schema.fields().iter().map(visit_field).collect::<Vec<_>>();

        Schema::new(fields).with_metadata(schema.metadata().clone())
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

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn navigate_to(
        &self,
        _ctx: &Arc<dyn TableContext>,
        navigation: &TimeNavigation,
    ) -> Result<Arc<dyn Table>> {
        let snapshot_id = match navigation {
            TimeNavigation::TimeTravel(np) => match np {
                NavigationPoint::SnapshotID(sid) => {
                    let sid = sid.parse::<i64>()?;
                    if self.table.metadata().snapshot_by_id(sid).is_some() {
                        Some(sid)
                    } else {
                        None
                    }
                }
                NavigationPoint::TimePoint(dt) => {
                    let ts = dt.timestamp_millis();
                    self.table
                        .metadata()
                        .history()
                        .iter()
                        .filter(|log| log.timestamp_ms < ts)
                        .max_by_key(|log| log.timestamp_ms)
                        .map(|s| s.snapshot_id)
                }
                _ => {
                    return Err(ErrorCode::Unimplemented(format!(
                            "Time travel operation is not supported for the table '{}', which uses the '{}' engine.",
                            self.name(),
                            self.get_table_info().engine(),
                        )));
                }
            },
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Time travel operation is not supported for the table '{}', which uses the '{}' engine.",
                    self.name(),
                    self.get_table_info().engine(),
                )));
            }
        };

        if snapshot_id.is_none() {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        }
        let (table, statistics) = Self::parse_engine_options(&self.info.meta.engine_options)?;
        Ok(Arc::new(IcebergTable {
            info: self.info.clone(),
            table,
            snapshot_id,
            statistics,
        }))
    }
}
