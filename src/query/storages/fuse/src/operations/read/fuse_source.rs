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
use databend_common_catalog::plan::StealablePartitions;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SourcePipeBuilder;
use log::info;

use super::block_format::FuseNativeBlockFormat;
use super::block_format::FuseParquetBlockFormat;
use super::read_block_context::ReadBlockContext;
use super::read_data_transform::ReadDataTransform;
use crate::FuseStorageFormat;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::DeserializeDataTransform;
use crate::operations::read::NativeDeserializeDataTransform;
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
    topk: Option<TopK>,
    mut max_io_requests: usize,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    receiver: Option<Receiver<Result<PartInfoPtr>>>,
) -> Result<()> {
    (max_threads, max_io_requests) = adjust_threads_and_request(
        matches!(storage_format, FuseStorageFormat::Native),
        max_threads,
        max_io_requests,
        plan,
    );

    if matches!(storage_format, FuseStorageFormat::Native) && topk.is_some() {
        max_threads = max_threads.min(16);
        max_io_requests = max_io_requests.min(16);
    }

    let waker = pipeline.get_waker();
    let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
    let stream: Arc<dyn PartitionStream> = match receiver {
        Some(rx) => Arc::new(ReceiverPartitionStream::new(rx)),
        None => {
            let partitions = dispatch_partitions(ctx.clone(), plan, max_io_requests);
            let mut partitions = StealablePartitions::new(partitions, ctx.clone());

            if matches!(storage_format, FuseStorageFormat::Native) && topk.is_some() {
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
        FuseStorageFormat::Native => FuseNativeBlockFormat::create(),
        FuseStorageFormat::Parquet => FuseParquetBlockFormat::create(),
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
            input,
            output,
        )
    })?;

    info!(
        "[FUSE-SOURCE] Block data reader adjusted max_io_requests to {}",
        max_io_requests
    );

    pipeline.try_resize(std::cmp::min(max_threads, max_io_requests))?;

    info!(
        "[FUSE-SOURCE] Block read pipeline resized from {} to {} threads",
        max_io_requests,
        pipeline.output_len()
    );

    match storage_format {
        FuseStorageFormat::Native => {
            pipeline.add_transform(|transform_input, transform_output| {
                NativeDeserializeDataTransform::create(
                    ctx.clone(),
                    block_reader.clone(),
                    plan,
                    topk.clone(),
                    transform_input,
                    transform_output,
                    index_reader.clone(),
                )
            })?;
        }
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

    for (i, part) in partitions.iter().enumerate() {
        results[i % max_streams].push_back(part.clone());
    }
    results
}

pub fn adjust_threads_and_request(
    is_native: bool,
    mut max_threads: usize,
    mut max_io_requests: usize,
    plan: &DataSourcePlan,
) -> (usize, usize) {
    if plan.parts.partitions_type() == PartInfoType::BlockLevel {
        let mut block_nums = plan.parts.partitions.len();

        // If the read bytes of a partition is small enough, less than 16k rows
        // we will not use an extra heavy thread to process it.
        // now only works for native reader
        static MIN_ROWS_READ_PER_THREAD: u64 = 16 * 1024;
        if is_native {
            plan.parts.partitions.iter().for_each(|part| {
                if let Some(part) = part.as_any().downcast_ref::<FuseBlockPartInfo>() {
                    let to_read_rows = part
                        .columns_meta
                        .values()
                        .map(|meta| meta.read_rows(part.range()))
                        .find(|rows| *rows > 0)
                        .unwrap_or(part.nums_rows as u64);

                    if to_read_rows < MIN_ROWS_READ_PER_THREAD {
                        block_nums -= 1;
                    }
                }
            });
        }

        // At least max(1/8 of the original parts, 1), in case of too many small partitions but io threads is just one.
        block_nums = std::cmp::max(block_nums, plan.parts.partitions.len() / 8);
        block_nums = std::cmp::max(block_nums, 1);

        max_threads = std::cmp::min(max_threads, block_nums);
        max_io_requests = std::cmp::min(max_io_requests, block_nums);
    }
    (max_threads, max_io_requests)
}
