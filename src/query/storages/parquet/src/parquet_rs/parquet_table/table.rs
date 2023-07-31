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
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchema;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::Pipeline;
use common_storage::init_stage_operator;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use opendal::Operator;
use parquet::schema::types::SchemaDescPtr;

use crate::utils::naive_parquet_table_info;

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
            compression_ratio: info.compression_ratio,
        }))
    }
}

#[async_trait::async_trait]
impl Table for ParquetTable {
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
}

fn lower_field_name(field: ArrowField) -> ArrowField {
    let name = field.name().to_lowercase();
    let field = field.with_name(name);
    match &field.data_type() {
        ArrowDataType::List(f) => {
            let inner = lower_field_name(f.as_ref().clone());
            field.with_data_type(ArrowDataType::List(Arc::new(inner)))
        }
        ArrowDataType::Struct(fields) => {
            let typ = ArrowDataType::Struct(
                fields
                    .iter()
                    .map(|f| lower_field_name(f.as_ref().clone()))
                    .collect::<Vec<_>>()
                    .into(),
            );
            field.with_data_type(typ)
        }
        _ => field,
    }
}

pub(crate) fn arrow_to_table_schema(schema: ArrowSchema) -> Result<TableSchema> {
    let fields = schema
        .fields
        .iter()
        .map(|f| Arc::new(lower_field_name(f.as_ref().clone())))
        .collect::<Vec<_>>();
    let schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());
    TableSchema::try_from(&schema).map_err(ErrorCode::from_std_error)
}

pub(super) fn create_parquet_table_info(schema: ArrowSchema) -> Result<TableInfo> {
    Ok(naive_parquet_table_info(
        arrow_to_table_schema(schema)?.into(),
    ))
}
