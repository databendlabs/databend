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
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::SyncReceiverSource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use parking_lot::Mutex;

use crate::compression::get_compression_with_path;
use crate::read::load_context::LoadContext;
use crate::read::row_based::format::create_row_based_file_format;
use crate::read::row_based::processors::BlockBuilder;
use crate::read::row_based::processors::Decompressor;
use crate::read::row_based::processors::Separator;

pub fn build_streaming_load_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    file_format_params: &FileFormatParams,
    rx: Arc<Mutex<Option<Receiver<Result<DataBlock>>>>>,
    schema: TableSchemaRef,
    default_exprs: Option<Vec<RemoteDefaultExpr>>,
    block_compact_thresholds: BlockThresholds,
    on_error_mode: OnErrorMode,
) -> Result<()> {
    pipeline.add_source(
        |output| {
            let mut guard = rx.lock();
            let rx = mem::take(&mut *guard).expect("streaming load receiver already dropped");
            SyncReceiverSource::create(ctx.clone(), rx, output.clone())
        },
        1,
    )?;
    let settings = ctx.get_settings();

    let max_threads = settings.get_max_threads()? as usize;

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
        on_error_mode,
    )?);
    row_based(pipeline, load_ctx, file_format_params, max_threads)
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
