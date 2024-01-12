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
use opendal::Operator;

use super::limit_file_size_processor::LimitFileSizeProcessor;
use super::writer_processor::ParquetFileWriter;

// LimitFileSizeProcessor * 1:  slice/group block to batches (as a block meta) that are suitable as a file.
// ParquetFileSink * N: simply serialize blocks in each meta to a whole file and write out.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_data_to_parquet_files(
    pipeline: &mut Pipeline,
    ctx: Arc<dyn TableContext>,
    table_info: StageTableInfo,
    op: Operator,
    max_file_size: usize,
    max_threads: usize,
    uuid: String,
    group_id: &std::sync::atomic::AtomicUsize,
) -> Result<()> {
    pipeline.try_resize(1)?;
    pipeline.add_transform(|input, output| {
        LimitFileSizeProcessor::try_create(input, output, max_file_size)
    })?;
    if max_file_size != usize::MAX {
        pipeline.try_resize(max_threads)?;
    }
    pipeline.add_sink(|input| {
        let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut options_ext =
            FileFormatOptionsExt::create_from_settings(&ctx.get_settings(), false)?;
        let output_format = options_ext.get_output_format(
            table_info.schema(),
            table_info.stage_info.file_format_params.clone(),
        )?;
        ParquetFileWriter::try_create(
            input,
            table_info.clone(),
            output_format,
            op.clone(),
            uuid.clone(),
            gid,
        )
    })?;
    Ok(())
}
