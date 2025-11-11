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

use std::mem;
use std::sync::Arc;

use databend_common_ast::ast::OnErrorMode;
use databend_common_base::base::tokio::sync::mpsc::Receiver;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_meta_app::principal::ParquetFileFormatParams;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::SyncReceiverSource;
use databend_common_pipeline_transforms::columns::TransformNullIf;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_storages_parquet::InmMemoryFile;
use parking_lot::Mutex;

use crate::compression::get_compression_with_path;
use crate::read::load_context::LoadContext;
use crate::read::row_based::format::create_row_based_file_format;
use crate::read::row_based::processors::BlockBuilder;
use crate::read::row_based::processors::Decompressor;
use crate::read::row_based::processors::Separator;
use crate::transform_generating::DataBlockIterator;
use crate::transform_generating::DataBlockIteratorBuilder;
use crate::transform_generating::GeneratingTransformer;
use crate::BytesBatch;

pub fn build_streaming_load_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    file_format_params: &FileFormatParams,
    rx: Arc<Mutex<Option<Receiver<Result<DataBlock>>>>>,
    schema: TableSchemaRef,
    default_exprs: Option<Vec<RemoteDefaultExpr>>,
    block_compact_thresholds: BlockThresholds,
) -> Result<()> {
    pipeline.add_source(
        |output| {
            let mut guard = rx.lock();
            let rx = mem::take(&mut *guard).expect("streaming load receiver already dropped");
            SyncReceiverSource::create(ctx.get_scan_progress(), rx, output.clone())
        },
        1,
    )?;
    let settings = ctx.get_settings();

    let max_threads = settings.get_max_threads()? as usize;

    // since there are only one source, a few processor is fast enough and avoid both OOM and small DataBlocks.
    let max_threads = max_threads.min(4);

    let file_format_options_ext = FileFormatOptionsExt::create_from_settings(&settings, false)?;

    let load_ctx = Arc::new(LoadContext::try_create(
        ctx.clone(),
        schema,
        file_format_options_ext,
        default_exprs,
        None,
        block_compact_thresholds,
        vec![],
        "".to_string(),
        OnErrorMode::AbortNum(1),
    )?);
    match file_format_params {
        FileFormatParams::Parquet(parquet_file_format) => {
            build_parquet(pipeline, load_ctx, parquet_file_format)
        }
        _ => row_based(pipeline, load_ctx, file_format_params, max_threads),
    }
}

fn row_based(
    pipeline: &mut Pipeline,
    load_ctx: Arc<LoadContext>,
    file_format_params: &FileFormatParams,
    max_threads: usize,
) -> Result<()> {
    let format = create_row_based_file_format(file_format_params);

    match file_format_params.compression() {
        StageFileCompression::None => {}
        compression => {
            let algo = get_compression_with_path(compression, "")?;
            pipeline.try_add_accumulating_transformer(|| {
                Decompressor::try_create(load_ctx.clone(), algo)
            })?;
        }
    }

    pipeline.try_add_accumulating_transformer(|| {
        Separator::try_create(load_ctx.clone(), format.clone())
    })?;

    pipeline.try_resize(max_threads)?;

    pipeline
        .try_add_accumulating_transformer(|| BlockBuilder::create(load_ctx.clone(), &format))?;

    Ok(())
}

fn build_parquet(
    pipeline: &mut Pipeline,
    load_ctx: Arc<LoadContext>,
    file_format_params: &ParquetFileFormatParams,
) -> Result<()> {
    let data_schema = Arc::new(DataSchema::from(&load_ctx.schema));
    let func_ctx = Arc::new(load_ctx.table_context.get_function_context()?);
    let use_logic_type = file_format_params.use_logic_type;
    pipeline.add_transform(|input_port, output_port| {
        let reader = ParquetStreamingLoadReader {
            ctx: load_ctx.table_context.clone(),
            output_schema: load_ctx.schema.clone(),
            default_exprs: load_ctx.default_exprs.clone(),
            missing_as: file_format_params.missing_field_as.clone(),
            case_sensitive: false,
            data_schema: data_schema.clone(),
            func_ctx: func_ctx.clone(),
            use_logic_type,
        };
        let proc = GeneratingTransformer::create(input_port, output_port, reader);
        Ok(ProcessorPtr::create(Box::new(proc)))
    })?;
    if !file_format_params.null_if.is_empty()
        && data_schema.fields.iter().any(|f| {
            if let DataType::Nullable(b) = f.data_type() {
                matches!(**b, DataType::String)
            } else {
                false
            }
        })
    {
        pipeline.try_add_transformer(|| {
            TransformNullIf::try_new(
                data_schema.clone(),
                data_schema.clone(),
                func_ctx.clone(),
                &file_format_params.null_if,
            )
        })?;
    }
    Ok(())
}

struct ParquetStreamingLoadReader {
    ctx: Arc<dyn TableContext>,
    output_schema: TableSchemaRef,
    data_schema: DataSchemaRef,
    default_exprs: Option<Vec<RemoteDefaultExpr>>,
    missing_as: NullAs,
    case_sensitive: bool,
    func_ctx: Arc<FunctionContext>,
    use_logic_type: bool,
}

impl DataBlockIteratorBuilder for ParquetStreamingLoadReader {
    const NAME: &'static str = "ParquetStreamingLoadReader";

    fn to_iter(&self, block: DataBlock) -> Result<DataBlockIterator> {
        let batch = block
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();
        InmMemoryFile::new(batch.path, batch.data.into()).read(
            self.ctx.clone(),
            self.output_schema.clone(),
            self.default_exprs.clone(),
            &self.missing_as.clone(),
            self.case_sensitive,
            self.func_ctx.clone(),
            self.data_schema.clone(),
            self.use_logic_type,
        )
    }
}
