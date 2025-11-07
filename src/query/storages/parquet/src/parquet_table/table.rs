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
use std::time::Instant;

use arrow_schema::Schema as ArrowSchema;
use chrono::DateTime;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::ParquetTableInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::DummyColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::FILENAME_COLUMN_ID;
use databend_common_expression::FILE_ROW_NUMBER_COLUMN_ID;
use databend_common_meta_app::principal::ParquetFileFormatParams;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use databend_common_settings::Settings;
use databend_common_storage::init_stage_operator;
use databend_common_storage::parquet::infer_schema_with_extension;
use databend_common_storage::read_metadata_async;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_storages_common_table_meta::table::ChangeType;
use log::info;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;

use crate::meta::read_metas_in_parallel;
use crate::parquet_table::stats::create_stats_provider;
use crate::schema::arrow_to_table_schema;

pub struct ParquetTable {
    pub(super) read_options: ParquetReadOptions,
    pub(super) stage_info: StageInfo,
    pub(super) files_info: StageFilesInfo,

    pub(super) operator: Operator,

    pub(super) table_info: TableInfo,
    pub(super) arrow_schema: ArrowSchema,
    pub(super) schema_descr: SchemaDescPtr,
    pub(super) files_to_read: Option<Vec<StageFileInfo>>,
    pub(super) schema_from: String,
    pub(super) compression_ratio: f64,
    /// Leaf fields of the schema.
    /// It's should be parallel with the parquet schema descriptor.
    /// Computing leaf fields could be expensive, so we store it here.
    pub(super) leaf_fields: Arc<Vec<TableField>>,

    pub(super) need_stats_provider: bool,
    pub(super) max_threads: usize,
    pub(super) max_memory_usage: u64,
}

impl ParquetTable {
    pub fn from_info(info: &ParquetTableInfo) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&info.stage_info)?;

        Ok(Arc::new(ParquetTable {
            table_info: info.table_info.clone(),
            arrow_schema: info.arrow_schema.clone(),
            operator,
            read_options: info.read_options,
            stage_info: info.stage_info.clone(),
            files_info: info.files_info.clone(),
            files_to_read: info.files_to_read.clone(),
            schema_descr: info.schema_descr.clone(),
            schema_from: info.schema_from.clone(),
            leaf_fields: info.leaf_fields.clone(),
            compression_ratio: info.compression_ratio,
            need_stats_provider: info.need_stats_provider,
            max_threads: info.max_threads,
            max_memory_usage: info.max_memory_usage,
        }))
    }

    #[async_backtrace::framed]
    pub async fn create(
        ctx: &dyn TableContext,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        read_options: ParquetReadOptions,
        files_to_read: Option<Vec<StageFileInfo>>,
        settings: Arc<Settings>,
        query_kind: QueryKind,
        case_sensitive: bool,
        fmt: &ParquetFileFormatParams,
    ) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&stage_info)?;
        let first_file = match &files_to_read {
            Some(files) => Some(files[0].clone()),
            None => files_info.first_file(&operator).await?,
        };

        let Some(first_file) = first_file else {
            return ctx.get_zero_table().await;
        };

        let first_file = first_file.path;

        let (arrow_schema, schema_descr, compression_ratio) =
            Self::prepare_metas(&first_file, operator.clone()).await?;
        let schema =
            arrow_to_table_schema(&arrow_schema, case_sensitive, fmt.use_logic_type)?.into();
        let table_info = create_parquet_table_info(schema, &stage_info)?;
        let leaf_fields = Arc::new(table_info.schema().leaf_fields());

        // If the query is `COPY`, we don't need to collect column statistics.
        // It's because the only transform could be contained in `COPY` command is projection.
        let need_stats_provider = match query_kind {
            QueryKind::CopyIntoTable | QueryKind::CopyIntoLocation => true,
            QueryKind::Unknown => {
                // add this branch to ensure query_kind is set
                return Err(ErrorCode::Internal(
                    "Unexpected QueryKind::Unknown: query_kind was not properly set before calling ParquetRSTable::create.",
                ));
            }
            _ => false,
        };

        let max_threads = settings.get_max_threads()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()?;

        Ok(Arc::new(ParquetTable {
            table_info,
            arrow_schema,
            operator,
            read_options,
            schema_descr,
            leaf_fields,
            stage_info,
            files_info,
            files_to_read,
            compression_ratio,
            schema_from: first_file,
            need_stats_provider,
            max_threads,
            max_memory_usage,
        }))
    }

    #[async_backtrace::framed]
    async fn prepare_metas(
        path: &str,
        operator: Operator,
    ) -> Result<(ArrowSchema, SchemaDescPtr, f64)> {
        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let stat = operator.stat(path).await?;
        let size = stat.content_length();
        info!("infer schema from file {}, with stat {:?}", path, stat);
        let first_meta = read_metadata_async(path, &operator, Some(size)).await?;
        let arrow_schema = infer_schema_with_extension(first_meta.file_metadata())?;
        let compression_ratio = get_compression_ratio(&first_meta);
        let schema_descr = first_meta.file_metadata().schema_descr_ptr();
        Ok((arrow_schema, schema_descr, compression_ratio))
    }
}

#[async_trait::async_trait]
impl Table for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn supported_internal_column(&self, column_id: ColumnId) -> bool {
        (FILE_ROW_NUMBER_COLUMN_ID..=FILENAME_COLUMN_ID).contains(&column_id)
    }

    fn support_prewhere(&self) -> bool {
        self.read_options.do_prewhere()
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::ParquetSource(ParquetTableInfo {
            table_info: self.table_info.clone(),
            arrow_schema: self.arrow_schema.clone(),
            read_options: self.read_options,
            stage_info: self.stage_info.clone(),
            schema_descr: self.schema_descr.clone(),
            leaf_fields: self.leaf_fields.clone(),
            files_info: self.files_info.clone(),
            files_to_read: self.files_to_read.clone(),
            schema_from: self.schema_from.clone(),
            compression_ratio: self.compression_ratio,
            need_stats_provider: self.need_stats_provider,
            max_threads: self.max_threads,
            max_memory_usage: self.max_memory_usage,
        })
    }

    /// The returned partitions only record the locations of files to read.
    /// So they don't have any real statistics.
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
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

    fn is_stage_table(&self) -> bool {
        true
    }

    async fn column_statistics_provider(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        if !self.need_stats_provider {
            return Ok(Box::new(DummyColumnStatisticsProvider));
        }

        let limit = 32;
        let sample_meta_count = 4;

        let mut total_files = None;
        let file_locations = match &self.files_to_read {
            Some(files) => {
                total_files = Some(files.len());

                files
                    .iter()
                    .filter(|f| f.size > 0)
                    .map(|f| (f.path.clone(), f.size, f.dedup_key()))
                    .collect::<Vec<_>>()
            }
            None => self
                .files_info
                .list(&self.operator, 1, Some(limit))
                .await?
                .into_iter()
                .map(|f| (f.path.clone(), f.size, f.dedup_key()))
                .collect::<Vec<_>>(),
        };

        if file_locations.len() < limit {
            total_files = Some(file_locations.len());
        }

        // we don't know how many file and rows to read, then return dummy provider
        if total_files.is_none() {
            return Ok(Box::new(DummyColumnStatisticsProvider));
        }

        let total_files = file_locations.len();
        let file_locations = file_locations
            .into_iter()
            .take(sample_meta_count)
            .collect::<Vec<_>>();

        let num_columns = self.leaf_fields.len();
        let now = Instant::now();
        log::info!("begin read {} parquet file metas", file_locations.len());
        let metas = read_metas_in_parallel(
            &self.operator,
            &file_locations, // The first file is already read.
            (self.schema_descr.clone(), self.schema_from.clone()),
            self.leaf_fields.clone(),
            self.max_threads,
            self.max_memory_usage,
            true,
        )
        .await?;
        let elapsed = now.elapsed();
        log::info!(
            "end read {} parquet file metas, use {:?}",
            file_locations.len(),
            elapsed
        );
        let provider = create_stats_provider(&metas, total_files, num_columns);
        Ok(Box::new(provider))
    }

    async fn table_statistics(
        &self,
        ctx: Arc<dyn TableContext>,
        _require_fresh: bool,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        let col_stats = self.column_statistics_provider(ctx).await?;
        let num_rows = col_stats.num_rows();
        Ok(Some(TableStatistics {
            num_rows,
            ..Default::default()
        }))
    }
}

fn create_parquet_table_info(
    schema: Arc<TableSchema>,
    stage_info: &StageInfo,
) -> Result<TableInfo> {
    Ok(TableInfo {
        ident: TableIdent::new(0, 0),
        desc: "''.'read_parquet'".to_string(),
        name: format!("read_parquet({})", stage_info.stage_name),
        meta: TableMeta {
            schema,
            engine: "SystemReadParquet".to_string(),
            created_on: DateTime::from_timestamp(0, 0).unwrap(),
            updated_on: DateTime::from_timestamp(0, 0).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn get_compression_ratio(filemeta: &ParquetMetaData) -> f64 {
    let compressed_size: i64 = filemeta
        .row_groups()
        .iter()
        .map(|g| g.compressed_size())
        .sum();
    let uncompressed_size: i64 = filemeta
        .row_groups()
        .iter()
        .map(|g| {
            g.columns()
                .iter()
                .map(|c| c.uncompressed_size())
                .sum::<i64>()
        })
        .sum();
    if compressed_size == 0 {
        1.0
    } else {
        (uncompressed_size as f64) / (compressed_size as f64)
    }
}
