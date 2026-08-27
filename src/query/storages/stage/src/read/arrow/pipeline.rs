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

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReaderBuilder;
use arrow_ipc::reader::StreamReader;
use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::ArrowFileFormatParams;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline::sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_storage::FileParseError;
use databend_common_storage::FileStatus;
use databend_common_storage::StageFileInfo;
use databend_common_storage::init_stage_operator;
use databend_storages_common_stage::project_columnar;

use crate::BytesBatch;
use crate::read::load_context::LoadContext;
use crate::read::whole_file_reader::WholeFileData;
use crate::read::whole_file_reader::WholeFileReader;

#[derive(Clone, Copy)]
pub enum ArrowIpcMode {
    File,
    Stream,
}

pub struct ArrowReadPipelineBuilder<'a> {
    pub(crate) stage_table_info: &'a StageTableInfo,
    pub(crate) compact_threshold: BlockThresholds,
    pub(crate) mode: ArrowIpcMode,
    pub(crate) format_params: ArrowFileFormatParams,
}

impl ArrowReadPipelineBuilder<'_> {
    pub fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        internal_columns: Vec<InternalColumn>,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        };

        let pos_projection = if let Some(PushDownInfo {
            projection: Some(Projection::Columns(columns)),
            ..
        }) = &plan.push_downs
        {
            Some(columns.clone())
        } else {
            None
        };

        let settings = ctx.get_settings();
        ctx.set_partitions(plan.parts.clone())?;

        let max_threads = settings.get_max_threads()? as usize;
        let num_sources = std::cmp::min(max_threads, plan.parts.len());
        let operator = init_stage_operator(&self.stage_table_info.stage_info)?;
        let load_ctx = Arc::new(LoadContext::try_create_for_copy(
            ctx.clone(),
            self.stage_table_info,
            pos_projection,
            self.compact_threshold,
            internal_columns,
        )?);

        pipeline.add_source(
            |output| {
                let reader = WholeFileReader::try_create(ctx.clone(), operator.clone())?;
                PrefetchAsyncSourcer::create(ctx.get_scan_progress(), output, reader)
            },
            num_sources,
        )?;

        pipeline.try_resize(max_threads)?;
        pipeline.try_add_accumulating_transformer(|| {
            ArrowBlockBuilder::create(
                load_ctx.clone(),
                self.mode,
                self.format_params.clone(),
                ctx.clone(),
            )
        })?;

        Ok(())
    }
}

pub async fn infer_arrow_schema(
    ctx: &dyn TableContext,
    stage_table_info: &StageTableInfo,
    mode: ArrowIpcMode,
) -> Result<TableSchemaRef> {
    let files = match &stage_table_info.files_to_copy {
        Some(files) => files.clone(),
        None => {
            let thread_num = ctx.get_settings().get_max_threads()? as usize;
            stage_table_info.list_files(thread_num, None).await?
        }
    };
    let file = files
        .into_iter()
        .find(|f| f.size > 0)
        .ok_or_else(|| ErrorCode::BadBytes("no non-empty Arrow file to infer schema"))?;
    infer_arrow_schema_from_file(&stage_table_info.stage_info, &file, mode).await
}

pub async fn infer_arrow_schema_from_file(
    stage_info: &databend_common_meta_app::principal::StageInfo,
    file: &StageFileInfo,
    mode: ArrowIpcMode,
) -> Result<TableSchemaRef> {
    let operator = init_stage_operator(stage_info)?;
    let data = operator
        .reader_with(&file.path)
        .await?
        .read(0..file.size)
        .await?
        .to_vec();
    let schema = match mode {
        ArrowIpcMode::File => FileReaderBuilder::new()
            .build(Cursor::new(data))
            .map_err(|e| ErrorCode::from(e).add_detail_back(format!("file path: {}", file.path)))?
            .schema()
            .as_ref()
            .try_into()?,
        ArrowIpcMode::Stream => StreamReader::try_new(Cursor::new(data), None)
            .map_err(|e| ErrorCode::from(e).add_detail_back(format!("file path: {}", file.path)))?
            .schema()
            .as_ref()
            .try_into()?,
    };
    Ok(Arc::new(schema))
}

pub(crate) struct ArrowBlockBuilder {
    ctx: Arc<LoadContext>,
    mode: ArrowIpcMode,
    format_params: ArrowFileFormatParams,
    source_schema: Option<TableSchemaRef>,
    projection: Option<Vec<Expr>>,
    copy_projection_evaluator: CopyProjectionEvaluator,
    internal_column_types: Vec<InternalColumnType>,
}

struct CopyProjectionEvaluator {
    schema: DataSchemaRef,
    func_ctx: Arc<FunctionContext>,
}

impl CopyProjectionEvaluator {
    fn new(schema: DataSchemaRef, func_ctx: Arc<FunctionContext>) -> Self {
        Self { schema, func_ctx }
    }

    fn project(&self, block: &DataBlock, projection: &[Expr]) -> Result<DataBlock> {
        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut entries = Vec::with_capacity(projection.len());
        let num_rows = block.num_rows();
        for (field, expr) in self.schema.fields().iter().zip(projection.iter()) {
            let entry = BlockEntry::new(evaluator.run(expr)?, || {
                (field.data_type().clone(), num_rows)
            });
            entries.push(entry);
        }
        Ok(DataBlock::new(entries, num_rows))
    }
}

impl ArrowBlockBuilder {
    pub(crate) fn create(
        ctx: Arc<LoadContext>,
        mode: ArrowIpcMode,
        format_params: ArrowFileFormatParams,
        table_ctx: Arc<dyn TableContext>,
    ) -> Result<Self> {
        let output_schema = Arc::new(DataSchema::from(ctx.schema.as_ref()));
        let func_ctx = Arc::new(table_ctx.get_function_context()?);
        let internal_column_types = ctx
            .internal_columns
            .iter()
            .map(|column| column.column_type.clone())
            .collect();
        Ok(Self {
            ctx,
            mode,
            format_params,
            source_schema: None,
            projection: None,
            copy_projection_evaluator: CopyProjectionEvaluator::new(output_schema, func_ctx),
            internal_column_types,
        })
    }

    fn read_batches(&self, path: &str, data: Vec<u8>) -> Result<Vec<RecordBatch>> {
        match self.mode {
            ArrowIpcMode::File => {
                let reader = FileReaderBuilder::new()
                    .build(Cursor::new(data))
                    .map_err(|e| {
                        ErrorCode::from(e).add_detail_back(format!("file path: {path}"))
                    })?;
                reader
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| ErrorCode::from(e).add_detail_back(format!("file path: {path}")))
            }
            ArrowIpcMode::Stream => {
                let reader = StreamReader::try_new(Cursor::new(data), None).map_err(|e| {
                    ErrorCode::from(e).add_detail_back(format!("file path: {path}"))
                })?;
                reader
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| ErrorCode::from(e).add_detail_back(format!("file path: {path}")))
            }
        }
    }

    fn project_batch(&mut self, path: &str, batch: &RecordBatch) -> Result<DataBlock> {
        let source_schema = Arc::new(TableSchema::try_from(batch.schema().as_ref())?);
        let rebuild_projection = self
            .source_schema
            .as_ref()
            .is_none_or(|schema| schema.as_ref() != source_schema.as_ref());
        if rebuild_projection {
            let (projection, _) = project_columnar(
                &source_schema,
                &self.ctx.schema,
                &self.format_params.missing_field_as,
                &self.ctx.default_exprs,
                path,
                false,
            )?;
            self.source_schema = Some(source_schema.clone());
            self.projection = Some(projection);
        }

        let source_data_schema = DataSchema::from(source_schema.as_ref());
        let block = DataBlock::from_record_batch(&source_data_schema, batch)?;
        self.copy_projection_evaluator
            .project(&block, self.projection.as_deref().unwrap())
    }
}

fn add_arrow_internal_columns(
    internal_columns: &[InternalColumnType],
    path: String,
    block: &mut DataBlock,
    start_row: &mut u64,
    content_key: Option<&str>,
    last_modified: Option<DateTime<Utc>>,
) {
    let num_rows = block.num_rows();
    for column_type in internal_columns {
        match column_type {
            InternalColumnType::FileName | InternalColumnType::FilePath => {
                block.add_const_column(Scalar::String(path.clone()), DataType::String);
            }
            InternalColumnType::FileBasename => {
                let basename = path.rsplit('/').next().unwrap_or(&path).to_string();
                block.add_const_column(Scalar::String(basename), DataType::String);
            }
            InternalColumnType::FileRowNumber => {
                let end_row = *start_row + num_rows as u64;
                block.add_column(Column::Number(
                    NumberColumnBuilder::UInt64((*start_row..end_row).collect()).build(),
                ));
                *start_row = end_row;
            }
            InternalColumnType::FileContentKey => {
                let scalar = match content_key {
                    Some(key) => Scalar::String(key.to_string()),
                    None => Scalar::Null,
                };
                block.add_const_column(scalar, DataType::Nullable(Box::new(DataType::String)));
            }
            InternalColumnType::FileLastModified => {
                let scalar = match last_modified {
                    Some(ts) => Scalar::Timestamp(ts.timestamp_micros()),
                    None => Scalar::Null,
                };
                block.add_const_column(scalar, DataType::Nullable(Box::new(DataType::Timestamp)));
            }
            _ => unreachable!("unexpected InternalColumnType in stage file reader"),
        }
    }
}

impl AccumulatingTransform for ArrowBlockBuilder {
    const NAME: &'static str = "ArrowBlockBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let meta = data
            .get_owned_meta()
            .ok_or_else(|| ErrorCode::Internal("ArrowBlockBuilder expects file data meta"))?;
        let (path, data, content_key, last_modified) = match BytesBatch::downcast_from_err(meta) {
            Ok(data) => (data.path, data.data, data.content_key, data.last_modified),
            Err(meta) => {
                let data = WholeFileData::downcast_from(meta).ok_or_else(|| {
                    ErrorCode::Internal("ArrowBlockBuilder expects Arrow file data")
                })?;
                (data.path, data.data, data.content_key, data.last_modified)
            }
        };

        if self.ctx.is_select {
            return self.transform_for_select(path, data, content_key.as_deref(), last_modified);
        }

        self.transform_for_copy(path, data, content_key.as_deref(), last_modified)
    }
}

impl ArrowBlockBuilder {
    fn transform_for_select(
        &mut self,
        path: String,
        data: Vec<u8>,
        content_key: Option<&str>,
        last_modified: Option<DateTime<Utc>>,
    ) -> Result<Vec<DataBlock>> {
        let mut blocks = Vec::new();
        let mut rows_start = 0;
        for batch in self.read_batches(&path, data)? {
            if batch.num_rows() == 0 {
                continue;
            }
            let mut block = self.project_batch(&path, &batch)?;
            add_arrow_internal_columns(
                &self.internal_column_types,
                path.clone(),
                &mut block,
                &mut rows_start,
                content_key,
                last_modified,
            );
            blocks.push(block);
        }
        Ok(blocks)
    }

    fn transform_for_copy(
        &mut self,
        path: String,
        data: Vec<u8>,
        content_key: Option<&str>,
        last_modified: Option<DateTime<Utc>>,
    ) -> Result<Vec<DataBlock>> {
        let mut file_status = FileStatus::default();
        let batches = match self.read_batches(&path, data) {
            Ok(batches) => batches,
            Err(e) => {
                self.ctx.error_handler.on_error(
                    FileParseError::BadFile {
                        format: "ARROW".to_string(),
                        message: e.to_string(),
                    }
                    .with_row(0),
                    None,
                    &mut file_status,
                    &path,
                )?;
                self.ctx.table_context.add_file_status(&path, file_status)?;
                return Ok(vec![]);
            }
        };

        let mut blocks = Vec::new();
        let mut rows_start = 0;
        let mut rows_loaded = 0;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let mut block = match self.project_batch(&path, &batch) {
                Ok(block) => block,
                Err(e) => {
                    self.ctx.error_handler.on_error(
                        FileParseError::InvalidRow {
                            format: "ARROW".to_string(),
                            message: e.to_string(),
                        }
                        .with_row(rows_start as usize),
                        None,
                        &mut file_status,
                        &path,
                    )?;
                    self.ctx.table_context.add_file_status(&path, file_status)?;
                    return Ok(vec![]);
                }
            };
            add_arrow_internal_columns(
                &self.internal_column_types,
                path.clone(),
                &mut block,
                &mut rows_start,
                content_key,
                last_modified,
            );
            rows_loaded += block.num_rows();
            blocks.push(block);
        }
        file_status.num_rows_loaded = rows_loaded;
        self.ctx.table_context.add_file_status(&path, file_status)?;
        Ok(blocks)
    }
}
