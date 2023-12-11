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
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use common_arrow::arrow::datatypes::Field as Arrow2Field;
use common_arrow::arrow::datatypes::Schema as Arrow2Schema;
use common_catalog::catalog::StorageDescription;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::TableSchema;
use common_meta_app::schema::TableInfo;
use common_meta_app::storage::StorageParams;
use common_pipeline_core::Pipeline;
use common_storage::init_operator;
use common_storages_parquet::ParquetFilesPart;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetRSPruner;
use common_storages_parquet::ParquetRSReaderBuilder;
use deltalake::kernel::Add;
use deltalake::logstore::default_logstore::DefaultLogStore;
use deltalake::logstore::LogStoreConfig;
use deltalake::DeltaTableConfig;
use tokio::sync::OnceCell;
use url::Url;

// use object_store_opendal::OpendalStore;
use crate::dal::OpendalStore;
use crate::partition::DeltaPartInfo;
use crate::table_source::DeltaTableSource;

pub const DELTA_ENGINE: &str = "DELTA";

/// accessor wrapper as a table
///
/// TODO: we should use icelake Table instead.
pub struct DeltaTable {
    info: TableInfo,
    table: OnceCell<deltalake::table::DeltaTable>,
}

impl DeltaTable {
    #[async_backtrace::framed]
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(Self {
            info,
            table: OnceCell::new(),
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

    #[async_backtrace::framed]
    pub async fn get_schema(table: &deltalake::table::DeltaTable) -> Result<TableSchema> {
        let delta_meta = table.get_schema().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;

        // Build arrow schema from delta metadata.
        let arrow_schema: ArrowSchema = delta_meta.try_into().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;

        // Build arrow2 schema from arrow schema.
        let fields: Vec<Arrow2Field> = arrow_schema
            .fields()
            .into_iter()
            .map(|f| f.into())
            .collect();
        let arrow2_schema = Arrow2Schema::from(fields);

        Ok(TableSchema::from(&arrow2_schema))
    }

    pub async fn load(sp: &StorageParams) -> Result<deltalake::table::DeltaTable> {
        let op = init_operator(sp)?;
        let opendal_store = Arc::new(OpendalStore::new(op));
        let config = DeltaTableConfig::default();
        let log_store = Arc::new(DefaultLogStore::new(opendal_store, LogStoreConfig {
            location: Url::from_directory_path("/").unwrap(),
            options: HashMap::new().into(),
        }));
        let mut table = deltalake::table::DeltaTable::new(log_store, config);
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
        let arrow_schema = table_schema.to_arrow();
        let arrow_fields = arrow_schema
            .fields
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<arrow_schema::Field>>();
        let arrow_schema = arrow_schema::Schema::new(arrow_fields);
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
        )?;

        let sp = self.get_storage_params()?;
        let op = init_operator(sp)?;
        let mut builder =
            ParquetRSReaderBuilder::create(ctx.clone(), op, table_schema, &arrow_schema)?
                .with_options(read_options)
                .with_push_downs(plan.push_downs.as_ref())
                .with_pruner(Some(pruner));

        let parquet_reader = Arc::new(builder.build_full_reader()?);

        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                DeltaTableSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    parquet_reader.clone(),
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

        let adds = table.get_state().files();
        let total_files = adds.len();
        let parts = adds.iter()
            .map(|add: &Add| {
                let stats = add
                    .get_stats()
                    .map_err(|e| ErrorCode::ReadTableDataError(format!("Cannot get stats: {e:?}")))?
                    .ok_or_else(|| {
                        ErrorCode::ReadTableDataError(format!(
                            "Current DeltaTable assuming Add contains Stats, but found in {}.",
                            add.path
                        ))
                    })?;
                read_rows += stats.num_records as usize;
                read_bytes += add.size as usize;
                Ok(Arc::new(
                    Box::new(DeltaPartInfo::Parquet(ParquetPart::ParquetFiles(
                        ParquetFilesPart {
                            files: vec![(add.path.clone(), add.size as u64)],
                            estimated_uncompressed_size: add.size as u64, // This field is not used here.
                        },
                    ))) as Box<dyn PartInfo>,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((
            PartStatistics::new_estimated(None, read_rows, read_bytes, parts.len(), total_files),
            Partitions::create_nolazy(PartitionsShuffleKind::Mod, parts),
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
