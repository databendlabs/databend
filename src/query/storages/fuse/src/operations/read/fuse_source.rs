//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::StealablePartitions;
use common_catalog::plan::TopK;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use tracing::info;

use crate::io::BlockReader;
use crate::operations::read::native_data_source_deserializer::NativeDeserializeDataTransform;
use crate::operations::read::native_data_source_reader::ReadNativeDataSource;
use crate::operations::read::parquet_data_source_deserializer::DeserializeDataTransform;
use crate::operations::read::parquet_data_source_reader::ReadParquetDataSource;

pub fn build_fuse_native_source_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    mut max_threads: usize,
    plan: &DataSourcePlan,
    topk: Option<TopK>,
    mut max_io_requests: usize,
) -> Result<()> {
    if topk.is_some() {
        const MAX_SOURCE_PARALLE_FOR_TOPK: usize = 16;
        max_io_requests = std::cmp::min(max_io_requests, MAX_SOURCE_PARALLE_FOR_TOPK);
        max_threads = std::cmp::min(max_threads, MAX_SOURCE_PARALLE_FOR_TOPK);
    }

    (max_threads, max_io_requests) = ajust_threads_and_request(max_threads, max_io_requests, plan);

    let mut source_builder = SourcePipeBuilder::create();

    match block_reader.support_blocking_api() {
        true => {
            let partitions = dispatch_partitions(ctx.clone(), max_threads);
            let partitions = StealablePartitions::new(partitions, ctx.clone());

            for i in 0..max_threads {
                let output = OutputPort::create();
                source_builder.add_source(
                    output.clone(),
                    ReadNativeDataSource::<true>::create(
                        i,
                        ctx.clone(),
                        output,
                        block_reader.clone(),
                        partitions.clone(),
                    )?,
                );
            }
            pipeline.add_pipe(source_builder.finalize());
        }
        false => {
            let partitions = dispatch_partitions(ctx.clone(), max_io_requests);
            let partitions = StealablePartitions::new(partitions, ctx.clone());

            for i in 0..max_io_requests {
                let output = OutputPort::create();
                source_builder.add_source(
                    output.clone(),
                    ReadNativeDataSource::<false>::create(
                        i,
                        ctx.clone(),
                        output,
                        block_reader.clone(),
                        partitions.clone(),
                    )?,
                );
            }
            pipeline.add_pipe(source_builder.finalize());
            pipeline.resize(max_threads)?;
        }
    };

    pipeline.add_transform(|transform_input, transform_output| {
        NativeDeserializeDataTransform::create(
            ctx.clone(),
            block_reader.clone(),
            plan,
            topk.clone(),
            transform_input,
            transform_output,
        )
    })?;

    pipeline.resize(max_threads)
}

pub fn build_fuse_parquet_source_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    plan: &DataSourcePlan,
    mut max_threads: usize,
    mut max_io_requests: usize,
) -> Result<()> {
    (max_threads, max_io_requests) = ajust_threads_and_request(max_threads, max_io_requests, plan);

    match block_reader.support_blocking_api() {
        true => {
            pipeline.add_source(
                |output| {
                    ReadParquetDataSource::<true>::create(ctx.clone(), output, block_reader.clone())
                },
                max_threads,
            )?;
        }
        false => {
            info!("read block data adjust max io requests:{}", max_io_requests);
            pipeline.add_source(
                |output| {
                    ReadParquetDataSource::<false>::create(
                        ctx.clone(),
                        output,
                        block_reader.clone(),
                    )
                },
                max_io_requests,
            )?;

            pipeline.resize(std::cmp::min(max_threads, max_io_requests))?;

            info!(
                "read block pipeline resize from:{} to:{}",
                max_io_requests,
                pipeline.output_len()
            );
        }
    };

    pipeline.add_transform(|transform_input, transform_output| {
        DeserializeDataTransform::create(
            ctx.clone(),
            block_reader.clone(),
            transform_input,
            transform_output,
        )
    })
}

pub fn dispatch_partitions(
    ctx: Arc<dyn TableContext>,
    max_streams: usize,
) -> Vec<VecDeque<PartInfoPtr>> {
    let mut results = vec![VecDeque::new(); max_streams];
    let partitions = ctx.get_partitions(usize::MAX);

    // that means the partition is lazy
    if partitions.is_empty() {
        return results;
    }

    for (i, part) in partitions.iter().enumerate() {
        results[i % max_streams].push_back(part.clone());
    }
    results
}

pub fn ajust_threads_and_request(
    mut max_threads: usize,
    mut max_io_requests: usize,
    plan: &DataSourcePlan,
) -> (usize, usize) {
    if !plan.parts.is_lazy {
        let block_nums = plan.parts.partitions.len();

        max_threads = std::cmp::min(max_threads, block_nums);
        max_io_requests = std::cmp::min(max_io_requests, block_nums);
    }
    (max_threads, max_io_requests)
}
