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
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::init_operator;
use databend_common_storages_parquet::ParquetFilesPart;
use databend_common_storages_parquet::ParquetPart;
use databend_common_storages_parquet::ParquetRSPruner;
use databend_common_storages_parquet::ParquetRSReaderBuilder;
use databend_storages_common_table_meta::table::OPT_KEY_ENGINE_META;
use deltalake::kernel::Add;
use deltalake::DeltaTableBuilder;
use opendal::Metakey;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;
use url::Url;

// use object_store_opendal::OpendalStore;
use crate::dal::OpendalStore;
use crate::partition::DeltaPartInfo;
use crate::partition_columns::get_partition_values;
use crate::partition_columns::get_pushdown_without_partition_columns;
use crate::table_source::DeltaTableSource;

pub const DELTA_ENGINE: &str = "DELTA";

pub struct DeltaTable {
    info: TableInfo,
    table: OnceCell<deltalake::table::DeltaTable>,
    meta: DeltaTableMeta,
}

#[derive(Serialize, Deserialize)]
pub struct DeltaTableMeta {
    partition_columns: Vec<String>,
}

/// In a delta table, partition columns are not stored in parquet file.
/// so it needs a few efforts to make pushdown work:
/// - context:
///   - Table store partition column names in meta.engine_options.
///   - Each partition carries all partition column values in the same order.
///   - With this order, we can get need info with a PartitionIndex.
/// - pushdown:
///   - projections (mask): partition columns are excluded when read parquet file and inserted at last.
///   - filter pass to parquet reader: all partition columns are appended to the filter input columns.
///   - pruner: ColumnRef of partition columns in filter expr are replace with const scalars.
/// Type of partition columns can only be simple primitive types.
impl DeltaTable {
    #[async_backtrace::framed]
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let meta_string = info
            .meta
            .engine_options
            .get(OPT_KEY_ENGINE_META)
            .ok_or_else(|| ErrorCode::Internal("missing engine option OPT_KEY_ENGINE_META"))?;
        let meta: DeltaTableMeta = serde_json::from_str(meta_string).map_err(|e| {
            ErrorCode::Internal(format!(
                "fail to deserialize DeltaTableMeta({meta_string}): {e:?}"
            ))
        })?;
        Ok(Box::new(Self {
            info,
            table: OnceCell::new(),
            meta,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: DELTA_ENGINE.to_string(),
            comment: "DELTA Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    fn get_storage_params(&self) -> Result<&StorageParams> {
        self.info.meta.storage_params.as_ref().ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Delta table {} must have storage parameters",
                self.info.name
            ))
        })
    }

    #[allow(dead_code)]
    fn get_partition_fields(&self) -> Result<Vec<&TableField>> {
        self.meta
            .partition_columns
            .iter()
            .map(|name| self.info.meta.schema.field_with_name(name))
            .collect()
    }

    #[async_backtrace::framed]
    pub async fn get_meta(table: &deltalake::table::DeltaTable) -> Result<(TableSchema, String)> {
        let delta_meta = table.get_schema().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;

        // Build arrow schema from delta metadata.
        let arrow_schema: ArrowSchema = delta_meta.try_into().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;

        let state = table.metadata().map_err(|_| {
            ErrorCode::ReadTableDataError("bug: Delta table current_metadata is None.")
        })?;
        let meta = DeltaTableMeta {
            partition_columns: state.partition_columns.clone(),
        };
        let meta = serde_json::to_string(&meta).map_err(|e| {
            ErrorCode::ReadTableDataError(format!("fail to serialize DeltaTableMeta: {e:?}"))
        })?;

        let schema = TableSchema::try_from(&arrow_schema)?;
        Ok((schema, meta))
    }

    #[async_backtrace::framed]
    pub async fn load(sp: &StorageParams) -> Result<deltalake::table::DeltaTable> {
        let op = init_operator(sp)?;
        let opendal_store = Arc::new(OpendalStore::new(op).with_metakey(Metakey::Version));

        let mut table = DeltaTableBuilder::from_uri(Url::from_directory_path("/").unwrap())
            .with_storage_backend(opendal_store, Url::from_directory_path("/").unwrap())
            .build()
            .map_err(|err| {
                ErrorCode::ReadTableDataError(format!("Delta table load failed: {err:?}"))
            })?;

        table.load().await.map_err(|err| {
            ErrorCode::ReadTableDataError(format!("Delta table load failed: {err:?}"))
        })?;
        Ok(table)
    }

    #[async_backtrace::framed]
    async fn table(&self) -> Result<&deltalake::table::DeltaTable> {
        self.table
            .get_or_try_init(|| async {
                let sp = self.get_storage_params()?;
                Self::load(sp).await
            })
            .await
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

        let table_schema = self.schema();
        let non_partition_fields = table_schema
            .fields()
            .iter()
            .filter(|field| !self.meta.partition_columns.contains(&field.name))
            .cloned()
            .collect();
        let table_schema = Arc::new(TableSchema::new(non_partition_fields));

        let arrow_schema = table_schema.as_ref().into();
        let leaf_fields = Arc::new(table_schema.leaf_fields());

        let mut read_options = ParquetReadOptions::default();

        if !ctx.get_settings().get_enable_parquet_page_index()? {
            read_options = read_options.with_prune_pages(false);
        }

        if !ctx.get_settings().get_enable_parquet_rowgroup_pruning()? {
            read_options = read_options.with_prune_row_groups(false);
        }

        if !ctx.get_settings().get_enable_parquet_prewhere()? {
            read_options = read_options.with_do_prewhere(false);
        }

        let pruner = ParquetRSPruner::try_create(
            ctx.get_function_context()?,
            table_schema.clone(),
            leaf_fields,
            &plan.push_downs,
            read_options,
            self.meta.partition_columns.clone(),
        )?;

        let sp = self.get_storage_params()?;
        let op = init_operator(sp)?;
        let partition_field_indexes: Result<Vec<FieldIndex>> = self
            .meta
            .partition_columns
            .iter()
            .map(|name| self.info.meta.schema.index_of(name))
            .collect();
        let partition_field_indexes = partition_field_indexes?;
        let push_downs = if let Some(ref p) = plan.push_downs {
            Some(get_pushdown_without_partition_columns(
                p.clone(),
                &partition_field_indexes[..],
            )?)
        } else {
            None
        };
        let mut builder =
            ParquetRSReaderBuilder::create(ctx.clone(), op, table_schema, &arrow_schema)?
                .with_options(read_options)
                .with_push_downs(push_downs.as_ref())
                .with_pruner(Some(pruner))
                .with_partition_columns(self.meta.partition_columns.clone());

        let parquet_reader = Arc::new(builder.build_full_reader()?);

        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                DeltaTableSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    parquet_reader.clone(),
                    self.get_partition_fields()?.into_iter().cloned().collect(),
                )
            },
            max_threads.max(1),
        )
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let table = self.table().await?;

        let mut read_rows = 0;
        let mut read_bytes = 0;

        let partition_fields = self.get_partition_fields()?;
        let adds = table
            .snapshot()
            .and_then(|f| f.file_actions())
            .map_err(|e| {
                ErrorCode::ReadTableDataError(format!("Cannot read file_actions: {e:?}"))
            })?;
        let total_files = adds.len();

        #[derive(serde::Deserialize)]
        struct Stats {
            #[serde(rename = "numRecords")]
            pub num_records: i64,
        }

        let parts = adds.iter()
            .map(|add: &Add| {
                let num_records = add
                    .get_stats_parsed()
                    .ok()
                    .and_then(|s| match (s, add.stats.as_ref()) {
                        (Some(s), _) => Some(s.num_records),
                        (None, Some(s)) => {
                            let stats = serde_json::from_str::<Stats>(s.as_str()).unwrap();
                            Some(stats.num_records)
                        }
                        _ => None,
                    }
                    ).unwrap_or(1);
                read_rows += num_records as usize;
                read_bytes += add.size as usize;
                let partition_values = get_partition_values(add, &partition_fields[..])?;
                Ok(Arc::new(
                    Box::new(DeltaPartInfo {
                        partition_values,
                        data: ParquetPart::ParquetFiles(
                            ParquetFilesPart {
                                files: vec![(add.path.clone(), add.size as u64)],
                                estimated_uncompressed_size: add.size as u64, // This field is not used here.
                            },
                        ),
                    }) as Box<dyn PartInfo>
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((
            PartStatistics::new_estimated(None, read_rows, read_bytes, parts.len(), total_files),
            Partitions::create(PartitionsShuffleKind::Mod, parts),
        ))
    }
}

#[async_trait]
impl Table for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
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
        true
    }
}
