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

use std::sync::Arc;

use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::input_formats::InputContext;
use opendal::Operator;

use super::limit_file_size_processor::LimitFileSizeProcessor;
use super::serialize_processor::SerializeProcessor;
use super::writer_processor::RowBasedFileWriter;

// SerializeProcessor * N: serialize each data block to many small byte buffers.
// LimitFileSizeProcessor * 1:  group small byte buffers to batches (as a block meta) that are large enough as a file.
// RowBasedFileSink * N: simply concat small byte buffers to a whole and write out.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_data_to_row_based_files(
    pipeline: &mut Pipeline,
    ctx: Arc<dyn TableContext>,
    table_info: StageTableInfo,
    op: Operator,
    max_file_size: usize,
    max_threads: usize,
    uuid: String,
    group_id: &std::sync::atomic::AtomicUsize,
) -> Result<()> {
    let mut options_ext = FileFormatOptionsExt::create_from_settings(&ctx.get_settings(), false)?;
    let output_format = options_ext.get_output_format(
        table_info.schema(),
        table_info.stage_info.file_format_params.clone(),
    )?;
    let compression = table_info
        .stage_info
        .file_format_params
        .clone()
        .compression();
    let prefix = output_format.serialize_prefix()?;

    pipeline.add_transform(|input, output| {
        let mut options_ext =
            FileFormatOptionsExt::create_from_settings(&ctx.get_settings(), false)?;
        let output_format = options_ext.get_output_format(
            table_info.schema(),
            table_info.stage_info.file_format_params.clone(),
        )?;
        SerializeProcessor::try_create(ctx.clone(), input, output, output_format)
    })?;
    pipeline.try_resize(1)?;
    pipeline.add_transform(|input, output| {
        LimitFileSizeProcessor::try_create(input, output, max_file_size)
    })?;
    if max_file_size != usize::MAX {
        pipeline.try_resize(max_threads)?;
    }

    let compression = InputContext::get_compression_alg_copy(compression, "")?;

    pipeline.add_sink(|input| {
        let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RowBasedFileWriter::try_create(
            input,
            table_info.clone(),
            op.clone(),
            prefix.clone(),
            uuid.clone(),
            gid,
            compression,
        )
    })?;
    Ok(())
}
