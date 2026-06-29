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

use std::collections::VecDeque;
use std::sync::Arc;

use async_channel::Receiver;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::StealablePartitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SourcePipeBuilder;
use log::info;

use super::block_format::FuseParquetBlockFormat;
use super::read_block_context::ReadBlockContext;
use super::read_data_transform::ReadDataTransform;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::DeserializeDataTransform;
use crate::operations::read::partition_stream::PartitionStream;
use crate::operations::read::partition_stream::PartitionStreamSource;
use crate::operations::read::partition_stream::ReceiverPartitionStream;
use crate::operations::read::partition_stream::StealPartitionStream;

#[allow(clippy::too_many_arguments)]
pub fn build_fuse_source_pipeline(
    ctx: Arc<dyn TableContext>,
    storage_format: FuseStorageFormat,
    table_schema: Arc<TableSchema>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    mut max_threads: usize,
    plan: &DataSourcePlan,
    mut max_io_requests: usize,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    receiver: Option<Receiver<Result<PartInfoPtr>>>,
) -> Result<()> {
    (max_threads, max_io_requests) = adjust_threads_and_request(max_threads, max_io_requests, plan);

    let preserve_order = plan.parts.kind == PartitionsShuffleKind::PreserveOrder;
    if preserve_order {
        // Keep the planner-proven scan-stream count. Each stream reads its
        // assigned subsequence in order; downstream PresortedMerge performs the
        // only inter-stream merge.
        let preserve_order_streams = plan
            .parts
            .partitions
            .iter()
            .filter_map(|part| {
                FuseBlockPartInfo::from_part(part)
                    .ok()
                    .and_then(|part| part.preserve_order_stream)
            })
            .max()
            .map_or(1, |stream| stream + 1);
        max_io_requests = max_io_requests.min(max_threads).min(preserve_order_streams);
        max_threads = max_threads.min(max_io_requests);
    }

    let waker = pipeline.get_waker();
    // Keep ordered streams at one block per fetch so LIMIT can stop before a
    // stream reads blocks beyond the current merge frontier.
    let batch_size = match preserve_order {
        true => 1,
        false => ctx.get_settings().get_storage_fetch_part_num()? as usize,
    };
    let stream: Arc<dyn PartitionStream> = match receiver {
        Some(rx) => Arc::new(ReceiverPartitionStream::new(rx)),
        None => {
            let partitions = dispatch_partitions(ctx.clone(), plan, max_io_requests);
            let mut partitions = StealablePartitions::new(partitions, ctx.clone());

            if preserve_order {
                partitions.disable_steal();
            }

            Arc::new(StealPartitionStream::new(partitions.clone(), batch_size))
        }
    };

    let mut source_builder = SourcePipeBuilder::create();
    for i in 0..max_io_requests {
        let output = OutputPort::create();
        source_builder.add_source(
            output.clone(),
            PartitionStreamSource::create(
                i,
                waker.clone(),
                output,
                stream.clone(),
                ctx.clone(),
                plan.scan_id,
            )?,
        );
    }
    pipeline.add_pipe(source_builder.finalize());

    let block_format = match storage_format {
        FuseStorageFormat::Parquet => FuseParquetBlockFormat::create(),
        FuseStorageFormat::Unsupported => {
            return Err(crate::unsupported_storage_format_error());
        }
    };

    let read_block_context = ReadBlockContext::create(
        ctx.clone(),
        storage_format,
        block_reader.read_context(),
        block_format,
        index_reader.clone(),
        virtual_reader.clone(),
    )?;

    pipeline.add_transform(|input, output| {
        ReadDataTransform::create(
            plan.scan_id,
            ctx.clone(),
            table_schema.clone(),
            block_reader.clone(),
            read_block_context.clone(),
            plan.push_downs
                .as_ref()
                .and_then(|push_downs| push_downs.prewhere.clone()),
            input,
            output,
        )
    })?;

    info!(
        "[FUSE-SOURCE] Block data reader adjusted max_io_requests to {}",
        max_io_requests
    );

    if !preserve_order {
        pipeline.try_resize(std::cmp::min(max_threads, max_io_requests))?;
    }

    info!(
        "[FUSE-SOURCE] Block read pipeline resized from {} to {} threads",
        max_io_requests,
        pipeline.output_len()
    );

    match storage_format {
        FuseStorageFormat::Parquet => {
            pipeline.add_transform(|transform_input, transform_output| {
                DeserializeDataTransform::create(
                    ctx.clone(),
                    block_reader.clone(),
                    plan,
                    preserve_order,
                    transform_input,
                    transform_output,
                    index_reader.clone(),
                    virtual_reader.clone(),
                )
            })?;
        }
        FuseStorageFormat::Unsupported => {
            return Err(crate::unsupported_storage_format_error());
        }
    }

    Ok(())
}

pub fn dispatch_partitions(
    ctx: Arc<dyn TableContext>,
    plan: &DataSourcePlan,
    max_streams: usize,
) -> Vec<VecDeque<PartInfoPtr>> {
    let mut results = Vec::with_capacity(max_streams);
    // Lazy part, we can dispatch them now.
    if plan.parts.partitions_type() == PartInfoType::LazyLevel {
        return results;
    }

    results = vec![VecDeque::new(); max_streams];
    const BATCH_SIZE: usize = 64;
    let mut partitions = Vec::with_capacity(BATCH_SIZE);
    loop {
        let p = ctx.get_partitions(BATCH_SIZE);
        if p.is_empty() {
            break;
        }
        partitions.extend(p);
    }

    // that means the partition is lazy
    if partitions.is_empty() {
        return results;
    }

    let preserve_order = plan.parts.kind == PartitionsShuffleKind::PreserveOrder;
    for (i, part) in partitions.iter().enumerate() {
        let index = if preserve_order {
            FuseBlockPartInfo::from_part(part)
                .ok()
                .and_then(|part| part.preserve_order_stream)
                .filter(|stream| *stream < max_streams)
                .unwrap_or(i % max_streams)
        } else {
            i % max_streams
        };
        results[index].push_back(part.clone());
    }
    results
}

pub fn adjust_threads_and_request(
    mut max_threads: usize,
    mut max_io_requests: usize,
    plan: &DataSourcePlan,
) -> (usize, usize) {
    if plan.parts.partitions_type() == PartInfoType::BlockLevel {
        let block_nums = std::cmp::max(plan.parts.partitions.len(), 1);

        max_threads = std::cmp::min(max_threads, block_nums);
        max_io_requests = std::cmp::min(max_io_requests, block_nums);
    }
    (max_threads, max_io_requests)
}
