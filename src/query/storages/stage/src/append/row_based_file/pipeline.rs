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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

use super::limit_file_size_processor::LimitFileSizeProcessor;
use super::serialize_processor::SerializeProcessor;
use super::writer_processor::RowBasedFileWriter;
use crate::compression::get_compression_with_path;

/// SerializeProcessor * N: serialize each data block to many small byte buffers.
/// LimitFileSizeProcessor * 1:  group small byte buffers to batches (as a block meta) that are large enough as a file.
/// RowBasedFileSink * N: simply concat small byte buffers to a whole and write out.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_data_to_row_based_files(
    pipeline: &mut Pipeline,
    ctx: Arc<dyn TableContext>,
    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    op: Operator,
    query_id: String,
    group_id: &std::sync::atomic::AtomicUsize,
    mem_limit: usize,
    max_threads: usize,
) -> Result<()> {
    let is_single = info.options.single;
    let max_file_size = info.options.max_file_size;
    let compression = info.stage.file_format_params.compression();
    // when serializing block to parquet, the memory may be doubled
    let mem_limit = mem_limit / 2;
    let max_file_size = if is_single {
        usize::MAX
    } else if max_file_size == 0 {
        if compression == StageFileCompression::None {
            16 * 1024 * 1024
        } else {
            64 * 1024 * 1024
        }
    } else {
        max_file_size.min(mem_limit)
    };

    let max_threads = max_threads.min(mem_limit / max_file_size).max(1);
    let format = &info.stage.file_format_params;

    let mut options_ext = FileFormatOptionsExt::create_from_settings(&ctx.get_settings(), false)?;
    let output_format = options_ext.get_output_format(schema.clone(), format.clone())?;
    let compression = info.stage.file_format_params.clone().compression();
    let prefix = output_format.serialize_prefix()?;

    pipeline.try_add_transformer(|| {
        let mut options_ext =
            FileFormatOptionsExt::create_from_settings(&ctx.get_settings(), false)?;
        let output_format = options_ext.get_output_format(schema.clone(), format.clone())?;
        Ok(SerializeProcessor::new(ctx.clone(), output_format))
    })?;
    pipeline.try_resize(1)?;
    pipeline.add_transform(|input, output| {
        LimitFileSizeProcessor::try_create(input, output, max_file_size)
    })?;

    if max_file_size != usize::MAX {
        pipeline.try_resize(max_threads)?;
    }

    let compression = get_compression_with_path(compression, "")?;

    pipeline.add_transform(|input, output| {
        let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RowBasedFileWriter::try_create(
            input,
            output,
            info.clone(),
            op.clone(),
            prefix.clone(),
            query_id.clone(),
            gid,
            compression,
            format.suffix(),
        )
    })?;
    Ok(())
}
