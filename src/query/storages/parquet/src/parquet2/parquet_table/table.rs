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

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::parquet::read as pread;
use databend_common_arrow::parquet::metadata::SchemaDescriptor;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Parquet2TableInfo;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::Parquet2TableColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::init_stage_operator;
use databend_common_storage::ColumnNodes;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use opendal::Operator;

use crate::parquet2::parquet_table::table::pread::FileMetaData;
use crate::parquet2::statistics::collect_basic_column_stats;

pub struct Parquet2Table {
    pub(super) read_options: ParquetReadOptions,
    pub(super) stage_info: StageInfo,
    pub(super) files_info: StageFilesInfo,

    pub(super) operator: Operator,

    pub(super) table_info: TableInfo,
    pub(super) arrow_schema: ArrowSchema,
    pub(super) schema_descr: SchemaDescriptor,
    pub(super) files_to_read: Option<Vec<StageFileInfo>>,
    pub(super) schema_from: String,
    pub(super) compression_ratio: f64,

    pub(super) column_statistics_provider: Parquet2TableColumnStatisticsProvider,
}

impl Parquet2Table {
    pub fn from_info(info: &Parquet2TableInfo) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&info.stage_info)?;
        Ok(Arc::new(Parquet2Table {
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
            column_statistics_provider: info.column_statistics_provider.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl Table for Parquet2Table {
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

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Option<TableStatistics>> {
        let s = &self.table_info.meta.statistics;
        Ok(Some(TableStatistics {
            num_rows: Some(s.number_of_rows),
            data_size: Some(s.data_bytes),
            data_size_compressed: Some(s.compressed_data_bytes),
            index_size: Some(s.index_data_bytes),
            number_of_blocks: s.number_of_blocks,
            number_of_segments: s.number_of_segments,
        }))
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        Ok(Box::new(self.column_statistics_provider.clone()))
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::Parquet2Source(Parquet2TableInfo {
            table_info: self.table_info.clone(),
            arrow_schema: self.arrow_schema.clone(),
            read_options: self.read_options,
            stage_info: self.stage_info.clone(),
            schema_descr: self.schema_descr.clone(),
            files_info: self.files_info.clone(),
            files_to_read: self.files_to_read.clone(),
            schema_from: self.schema_from.clone(),
            compression_ratio: self.compression_ratio,
            column_statistics_provider: self.column_statistics_provider.clone(),
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
}

fn lower_field_name(field: &mut ArrowField) {
    field.name = field.name.to_lowercase();
    match &mut field.data_type {
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => {
            lower_field_name(f.as_mut());
        }
        ArrowDataType::Struct(ref mut fields) => {
            for f in fields {
                lower_field_name(f);
            }
        }
        _ => {}
    }
}

pub(crate) fn arrow_to_table_schema(mut schema: ArrowSchema) -> TableSchema {
    schema.fields.iter_mut().for_each(|f| {
        lower_field_name(f);
    });
    TableSchema::from(&schema)
}

pub(super) fn create_parquet_table_info(schema: ArrowSchema, stage_info: &StageInfo) -> TableInfo {
    TableInfo {
        ident: TableIdent::new(0, 0),
        desc: "''.'read_parquet'".to_string(),
        name: format!("read_parquet({})", stage_info.stage_name),
        meta: TableMeta {
            schema: arrow_to_table_schema(schema).into(),
            engine: "SystemReadParquet".to_string(),
            created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
            updated_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub(super) fn create_parquet2_statistics_provider(
    file_metas: Vec<FileMetaData>,
    arrow_schema: &ArrowSchema,
) -> Result<Parquet2TableColumnStatisticsProvider> {
    // Collect Basic Columns Statistics from file metas.
    let mut num_rows = 0;
    let mut basic_column_stats = vec![BasicColumnStatistics::new_null(); arrow_schema.fields.len()];
    let column_nodes = ColumnNodes::new_from_schema(arrow_schema, None);
    for file_meta in file_metas.into_iter() {
        num_rows += file_meta.num_rows;
        let no_stats = file_meta.row_groups.is_empty()
            || file_meta.row_groups.iter().any(|r| {
                r.columns()
                    .iter()
                    .any(|c| c.metadata().statistics.is_none())
            });
        if !no_stats {
            if let Ok(row_group_column_stats) =
                collect_basic_column_stats(&column_nodes, &file_meta.row_groups)
            {
                for (idx, col_stat) in row_group_column_stats.into_iter().enumerate() {
                    basic_column_stats[idx].merge(col_stat);
                }
            }
        }
    }

    let mut column_stats = HashMap::new();
    for (column_id, col_stat) in basic_column_stats.into_iter().enumerate() {
        column_stats.insert(column_id as u32, col_stat);
    }
    let column_statistics_provider =
        Parquet2TableColumnStatisticsProvider::new(column_stats, num_rows as u64);
    Ok(column_statistics_provider)
}
