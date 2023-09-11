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

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::ParquetTableInfo;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::ColumnStatisticsProvider;
use common_catalog::table::ParquetTableColumnStatisticsProvider;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchema;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::Pipeline;
use common_storage::init_stage_operator;
use common_storage::parquet_rs::infer_schema_with_extension;
use common_storage::parquet_rs::read_metadata_async;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;

use super::meta::read_metas_in_parallel;
use super::meta::FullParquetMeta;
use super::stats::create_stats_provider;
use crate::parquet_rs::statistics::collect_row_group_stats;
use crate::utils::naive_parquet_table_info;

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

    pub(super) _parquet_metas: Vec<FullParquetMeta>, /* TODO(parquet): use this after implement ser/de for `FullParquetMeta`. */
    pub(super) stats_provider: ParquetTableColumnStatisticsProvider,
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
            compression_ratio: info.compression_ratio,
            _parquet_metas: vec![],
            stats_provider: info.stats_provider.clone(),
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
        let file_locations = match &files_to_read {
            Some(files) => files
                .iter()
                .map(|f| (f.path.clone(), f.size))
                .collect::<Vec<_>>(),
            None => files_info
                .list(&operator, false, None)
                .await?
                .into_iter()
                .map(|f| (f.path, f.size))
                .collect::<Vec<_>>(),
        };

        if file_locations.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "No parquet files to read in {}",
                files_info.path
            )));
        }

        // We use the first file in the file list to infer all the necessray information of this stage table.
        let (first_file, size) = file_locations[0].clone();
        let first_meta = read_metadata_async(&first_file, &operator, Some(size)).await?;
        let arrow_schema = infer_schema_with_extension(&first_meta)?;
        let compression_ratio = get_compression_ratio(&first_meta);
        let schema_descr = first_meta.file_metadata().schema_descr_ptr();
        let mut table_info = create_parquet_table_info(&arrow_schema)?;
        let leaf_fields = table_info.schema().leaf_fields();
        let first_stats = collect_row_group_stats(first_meta.row_groups(), &leaf_fields);
        let num_columns = leaf_fields.len();

        let mut metas = read_metas_in_parallel(
            ctx,
            &operator,
            &file_locations[1..], // The first file is already read.
            (schema_descr.clone(), first_file.clone()),
            leaf_fields,
        )
        .await?;
        metas.push(FullParquetMeta {
            location: first_file.clone(),
            meta: Arc::new(first_meta),
            row_group_level_stats: first_stats,
        });

        let stats_provider = create_stats_provider(&metas, num_columns);
        table_info.meta.statistics.number_of_rows = stats_provider.num_rows();

        Ok(Arc::new(ParquetRSTable {
            table_info,
            arrow_schema,
            operator,
            read_options,
            schema_descr,
            stage_info,
            files_info,
            files_to_read,
            compression_ratio,
            schema_from: first_file,
            _parquet_metas: metas,
            stats_provider,
        }))
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
            files_info: self.files_info.clone(),
            files_to_read: self.files_to_read.clone(),
            schema_from: self.schema_from.clone(),
            compression_ratio: self.compression_ratio,
            stats_provider: self.stats_provider.clone(),
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
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }

    fn is_stage_table(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(&self) -> Result<Box<dyn ColumnStatisticsProvider>> {
        Ok(Box::new(self.stats_provider.clone()))
    }
}

fn lower_field_name(field: &ArrowField) -> ArrowField {
    let name = field.name().to_lowercase();
    let field = field.clone().with_name(name);
    match &field.data_type() {
        ArrowDataType::List(f) => {
            let inner = lower_field_name(f);
            field.with_data_type(ArrowDataType::List(Arc::new(inner)))
        }
        ArrowDataType::Struct(fields) => {
            let typ = ArrowDataType::Struct(
                fields
                    .iter()
                    .map(|f| lower_field_name(f))
                    .collect::<Vec<_>>()
                    .into(),
            );
            field.with_data_type(typ)
        }
        _ => field,
    }
}

fn arrow_to_table_schema(schema: &ArrowSchema) -> Result<TableSchema> {
    let fields = schema
        .fields
        .iter()
        .map(|f| Arc::new(lower_field_name(f)))
        .collect::<Vec<_>>();
    let schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());
    TableSchema::try_from(&schema).map_err(ErrorCode::from_std_error)
}

fn create_parquet_table_info(schema: &ArrowSchema) -> Result<TableInfo> {
    Ok(naive_parquet_table_info(
        arrow_to_table_schema(schema)?.into(),
    ))
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
