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
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::FullParquetMeta;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::ParquetTableInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::DummyColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::init_stage_operator;
use databend_common_storage::parquet_rs::infer_schema_with_extension;
use databend_common_storage::parquet_rs::read_metadata_async;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_storages_common_table_meta::table::ChangeType;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;

use super::stats::create_stats_provider;
use crate::parquet_rs::meta::read_metas_in_parallel;
use crate::parquet_rs::schema::arrow_to_table_schema;

pub struct ParquetRSTable {
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

    /// Lazy read parquet file metas.
    ///
    /// After `column_statistics_provider` is called, the parquet metas will be store in memory.
    /// This instance is only be stored on one query node (the coordinator node in cluster mode),
    /// because it's only used to `column_statistics_provider` and `read_partitions`
    /// and these methods are all called during planning.
    ///
    /// The reason why wrap the metas in [`Mutex`] is that [`ParquetRSTable`] should impl [`Sync`] and [`Send`],
    /// and this field should have inner mutability (it will be initialized lazily in `column_statistics_provider`).
    ///
    /// As `paruqet_metas` will not be accessed by two threads simultaneously, use [`Mutex`] will not bring to much performance overhead.
    pub(super) parquet_metas: Arc<Mutex<Vec<Arc<FullParquetMeta>>>>,
    pub(super) need_stats_provider: bool,
    pub(super) max_threads: usize,
    pub(super) max_memory_usage: u64,
}

impl ParquetRSTable {
    pub fn from_info(info: &ParquetTableInfo) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&info.stage_info)?;

        Ok(Arc::new(ParquetRSTable {
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
            parquet_metas: info.parquet_metas.clone(),
            need_stats_provider: info.need_stats_provider,
            max_threads: info.max_threads,
            max_memory_usage: info.max_memory_usage,
        }))
    }

    #[async_backtrace::framed]
    pub async fn create(
        ctx: Arc<dyn TableContext>,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        read_options: ParquetReadOptions,
        files_to_read: Option<Vec<StageFileInfo>>,
    ) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&stage_info)?;
        let first_file = match &files_to_read {
            Some(files) => files[0].path.clone(),
            None => files_info.first_file(&operator).await?.path.clone(),
        };

        let (arrow_schema, schema_descr, compression_ratio) =
            Self::prepare_metas(&first_file, operator.clone()).await?;

        let table_info = create_parquet_table_info(&arrow_schema, &stage_info)?;
        let leaf_fields = Arc::new(table_info.schema().leaf_fields());

        // If the query is `COPY`, we don't need to collect column statistics.
        // It's because the only transform could be contained in `COPY` command is projection.
        let need_stats_provider = !matches!(
            ctx.get_query_kind(),
            QueryKind::CopyIntoTable | QueryKind::CopyIntoLocation
        );
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()?;

        Ok(Arc::new(ParquetRSTable {
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
            parquet_metas: Arc::new(Mutex::new(vec![])),
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
        let size = operator.stat(path).await?.content_length();
        let first_meta = read_metadata_async(path, &operator, Some(size)).await?;
        let arrow_schema = infer_schema_with_extension(first_meta.file_metadata())?;
        let compression_ratio = get_compression_ratio(&first_meta);
        let schema_descr = first_meta.file_metadata().schema_descr_ptr();
        Ok((arrow_schema, schema_descr, compression_ratio))
    }
}

#[async_trait::async_trait]
impl Table for ParquetRSTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        true
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
            parquet_metas: self.parquet_metas.clone(),
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
        ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        if !self.need_stats_provider {
            return Ok(Box::new(DummyColumnStatisticsProvider));
        }

        let thread_num = ctx.get_settings().get_max_threads()? as usize;

        // This method can only be called once.
        // Unwrap safety: no other thread will hold this lock.
        let mut parquet_metas = self.parquet_metas.try_lock().unwrap();
        assert!(parquet_metas.is_empty());

        // Lazy read parquet file metas.
        let file_locations = match &self.files_to_read {
            Some(files) => files
                .iter()
                .map(|f| (f.path.clone(), f.size))
                .collect::<Vec<_>>(),
            None => self
                .files_info
                .list(&self.operator, thread_num, None)
                .await?
                .into_iter()
                .map(|f| (f.path, f.size))
                .collect::<Vec<_>>(),
        };

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
        )
        .await?;
        let elapsed = now.elapsed();
        log::info!(
            "end read {} parquet file metas, use {} secs",
            file_locations.len(),
            elapsed.as_secs_f32()
        );

        let provider = create_stats_provider(&metas, num_columns);

        *parquet_metas = metas;

        Ok(Box::new(provider))
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        // Unwrap safety: no other thread will hold this lock.
        let parquet_metas = self.parquet_metas.try_lock().unwrap();
        if parquet_metas.is_empty() {
            return Ok(None);
        }

        let num_rows = parquet_metas
            .iter()
            .map(|m| m.meta.file_metadata().num_rows() as u64)
            .sum();

        // Other fields are not needed yet.
        Ok(Some(TableStatistics {
            num_rows: Some(num_rows),
            ..Default::default()
        }))
    }
}

fn create_parquet_table_info(schema: &ArrowSchema, stage_info: &StageInfo) -> Result<TableInfo> {
    Ok(TableInfo {
        ident: TableIdent::new(0, 0),
        desc: "''.'read_parquet'".to_string(),
        name: format!("read_parquet({})", stage_info.stage_name),
        meta: TableMeta {
            schema: arrow_to_table_schema(schema)?.into(),
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
