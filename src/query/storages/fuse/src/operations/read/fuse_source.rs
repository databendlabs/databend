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

use std::cmp::Ordering;
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
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SourcePipeBuilder;
use log::info;

use super::block_format::FuseParquetBlockFormat;
use super::read_block_context::ReadBlockContext;
use super::read_data_transform::ReadDataTransform;
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
        // Keep the original scan-stream count. Each stream reads its assigned
        // subsequence in order; downstream PresortedMerge performs the only
        // inter-stream merge.
        max_io_requests = max_io_requests.min(max_threads);
    }

    let waker = pipeline.get_waker();
    let batch_size = if preserve_order {
        1
    } else {
        ctx.get_settings().get_storage_fetch_part_num()? as usize
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
        block_reader.clone(),
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

    if plan.parts.kind == PartitionsShuffleKind::PreserveOrder {
        return dispatch_presorted_partitions(partitions, max_streams).expect(
            "presorted partitions must fit the stream count validated by the physical planner",
        );
    }

    for (i, part) in partitions.iter().enumerate() {
        results[i % max_streams].push_back(part.clone());
    }
    results
}

fn dispatch_presorted_partitions(
    partitions: Vec<PartInfoPtr>,
    max_streams: usize,
) -> Option<Vec<VecDeque<PartInfoPtr>>> {
    let mut results = vec![VecDeque::new(); max_streams];
    let mut stream_maxes: Vec<Vec<Scalar>> = Vec::new();

    for part in partitions {
        let fuse_part = crate::FuseBlockPartInfo::from_part(&part).ok()?;
        let stats = fuse_part.cluster_stats.as_ref()?;
        let min = stats.min();
        let max = stats.max();
        let reusable_stream = stream_maxes
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| compare_cluster_values(left, right))
            .and_then(|(index, stream_max)| {
                (compare_cluster_values(stream_max, min) != Ordering::Greater).then_some(index)
            });

        let stream_index = match reusable_stream {
            Some(index) => index,
            None if stream_maxes.len() < max_streams => {
                stream_maxes.push(max.clone());
                stream_maxes.len() - 1
            }
            None => return None,
        };
        stream_maxes[stream_index] = max.clone();
        results[stream_index].push_back(part);
    }

    Some(results)
}

fn compare_cluster_values(left: &[Scalar], right: &[Scalar]) -> Ordering {
    left.iter()
        .map(Scalar::as_ref)
        .cmp(right.iter().map(Scalar::as_ref))
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
