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

mod assemble;
mod fetch;
mod meta;
mod plan;
mod selection;

use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_pipeline::basic::create_resize_item;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_pipeline_transforms::create_dummy_item;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_io::ReadSettings;

use self::assemble::TransformRowFetchAssemble;
use self::fetch::ParquetRowsFetcher;
use self::fetch::TransformRowFetch;
use self::plan::BlockThreshold;
use self::plan::TransformRowFetchPlan;
use crate::FuseStorageFormat;
use crate::FuseTable;

/// Adds the fetch of `projection` to the lanes of `pipeline` selected by
/// `fetch_lane`, leaving the other lanes as they are. Each selected lane becomes
///
/// ```text
/// Plan -> Resize(1, N) -> Fetch x N -> Resize(N, 1) -> Assemble
/// ```
///
/// Each `Fetch` handles one block at a time: it reads asynchronously, so the workers
/// of the IO runtime only do IO, then deserializes synchronously on an executor
/// thread. So N bounds both the concurrent reads and the block data held in memory.
///
/// A lane keeps its order. The rows of a batch are spread over the blocks they
/// come from, whose fetches overtake each other in the N lanes, and `Assemble`
/// restores the batches and their rows: a sort upstream must not be lost.
#[allow(clippy::too_many_arguments)]
pub fn build_row_fetch_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
    need_wrap_nullable: bool,
    cache_policy: CachePolicy,
    fetch_lane: impl Fn(usize) -> bool,
) -> Result<()> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?
        .to_owned();
    let fuse_table = Arc::new(fuse_table);
    if let FuseStorageFormat::Unsupported = &fuse_table.storage_format {
        return Err(crate::unsupported_storage_format_error());
    }

    let table_schema = source.source_info.schema();
    let projected_schema = projection.project_schema(table_schema.as_ref());
    let fetched_data_types = projected_schema
        .fields()
        .iter()
        .map(|field| DataType::from(field.data_type()))
        .collect::<Vec<_>>();
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection.clone(), cache_policy)?;
    let read_settings = ReadSettings::from_ctx(&ctx)?;
    let settings = ctx.get_settings();
    let max_threads = settings.get_max_threads()? as usize;
    let block_threshold = BlockThreshold::new(
        settings.get_max_block_size()? as usize,
        settings.get_max_block_bytes()? as usize,
    );

    let lanes = (0..pipeline.output_len())
        .map(fetch_lane)
        .collect::<Vec<_>>();
    let fetched_lanes = lanes.iter().filter(|fetched| **fetched).count();
    if fetched_lanes == 0 {
        return Ok(());
    }
    // Lanes are not merged: downstream may depend on them, e.g. a block id
    // repartition upstream or the matched and unmatched lanes of a MERGE INTO.
    let width = (max_threads / fetched_lanes).max(1);

    add_pipe_by_lane(pipeline, &lanes, || {
        Ok(vec![transform_item(|input, output| {
            TransformRowFetchPlan::create(
                input,
                output,
                row_id_col_offset,
                ParquetRowsFetcher::create(fuse_table.clone(), projection.clone(), &block_reader),
                block_threshold,
            )
        })])
    })?;
    if width > 1 {
        add_pipe_by_lane(pipeline, &lanes, || Ok(vec![create_resize_item(1, width)]))?;
    }
    add_pipe_by_lane(pipeline, &lanes, || {
        Ok((0..width)
            .map(|_| {
                transform_item(|input, output| {
                    TransformRowFetch::create(input, output, block_reader.clone(), read_settings)
                })
            })
            .collect())
    })?;
    if width > 1 {
        add_pipe_by_lane(pipeline, &lanes, || Ok(vec![create_resize_item(width, 1)]))?;
    }
    add_pipe_by_lane(pipeline, &lanes, || {
        Ok(vec![transform_item(|input, output| {
            AccumulatingTransformer::create(
                input,
                output,
                TransformRowFetchAssemble::create(
                    row_id_col_offset,
                    need_wrap_nullable,
                    fetched_data_types.clone(),
                ),
            )
        })])
    })
}

/// Adds a pipe with the items of `fetched` for each fetched lane, and a
/// pass-through for each other lane.
fn add_pipe_by_lane(
    pipeline: &mut Pipeline,
    lanes: &[bool],
    fetched: impl Fn() -> Result<Vec<PipeItem>>,
) -> Result<()> {
    let mut items = Vec::with_capacity(lanes.len());
    for is_fetched in lanes {
        match is_fetched {
            true => items.extend(fetched()?),
            false => items.push(create_dummy_item()),
        }
    }
    let inputs = items.iter().map(|item| item.inputs_port.len()).sum();
    let outputs = items.iter().map(|item| item.outputs_port.len()).sum();
    pipeline.add_pipe(Pipe::create(inputs, outputs, items));
    Ok(())
}

fn transform_item(
    create: impl FnOnce(Arc<InputPort>, Arc<OutputPort>) -> Box<dyn Processor>,
) -> PipeItem {
    let input = InputPort::create();
    let output = OutputPort::create();
    let processor = ProcessorPtr::create(create(input.clone(), output.clone()));
    PipeItem::create(processor, vec![input], vec![output])
}
