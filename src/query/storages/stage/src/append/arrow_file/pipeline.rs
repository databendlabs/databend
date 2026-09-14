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

use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

use super::writer_processor::ArrowFileWriter;
use crate::append::column_based::limit_file_size_processor::LimitFileSizeProcessor;
use crate::read::arrow::ArrowIpcMode;

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_data_to_arrow_files(
    pipeline: &mut Pipeline,
    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    op: Operator,
    query_id: String,
    group_id: &std::sync::atomic::AtomicUsize,
    mem_limit: usize,
    max_file_size: usize,
) -> Result<()> {
    let max_file_size =
        LimitFileSizeProcessor::build(pipeline, mem_limit, max_file_size, &info.options)?;
    let mode = match &info.stage.file_format_params {
        FileFormatParams::Arrow(_) => ArrowIpcMode::File,
        FileFormatParams::ArrowStream(_) => ArrowIpcMode::Stream,
        _ => unreachable!(),
    };

    pipeline.add_transform(|input, output| {
        let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ArrowFileWriter::try_create(
            input,
            output,
            info.clone(),
            schema.clone(),
            op.clone(),
            query_id.clone(),
            gid,
            max_file_size,
            mode,
        )
    })?;
    Ok(())
}
