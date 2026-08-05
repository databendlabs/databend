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
use databend_common_catalog::runtime_filter_info::RuntimeScanFilters;
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
use crate::FuseStorageFormat;
use crate::fuse_part::FuseBlockPartInfo;
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

    let waker = pipeline.get_waker();
    let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
    let stream: Arc<dyn PartitionStream> = match receiver {
        Some(rx) => Arc::new(ReceiverPartitionStream::new(rx)),
        None => {
            let partitions = dispatch_partitions(ctx.clone(), plan, max_io_requests);
            let partitions = StealablePartitions::new(partitions, ctx.clone());

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
        FuseStorageFormat::Parquet => {
            pipeline.add_transform(|transform_input, transform_output| {
                DeserializeDataTransform::create(
                    ctx.clone(),
                    block_reader.clone(),
                    read_block_context.clone(),
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

    // Under runtime TopN (`enable_top_n`), read the most promising blocks
    // first so the shared boundary converges early and prunes the rest.
    let runtime_scan_filters = ctx.get_runtime_scan_filters(plan.scan_id);
    front_load_parts_for_runtime_top_n(
        &mut partitions,
        &runtime_scan_filters,
        max_streams.saturating_mul(16).max(1024),
    );

    for (i, part) in partitions.iter().enumerate() {
        results[i % max_streams].push_back(part.clone());
    }
    results
}

/// Move the `head` most promising parts to the front (sorted) so they are
/// read first. The tail is left unordered on purpose: once the head blocks
/// tighten the shared boundary, tail blocks are pruned at read time anyway,
/// so an O(n log n) sort over a potentially huge part list is avoided.
fn front_load_parts_for_runtime_top_n(
    parts: &mut [PartInfoPtr],
    filters: &RuntimeScanFilters,
    head: usize,
) {
    let Some((_, order)) = filters.preferred_filter() else {
        return;
    };
    let head = head.max(1);
    let compare = |left: &PartInfoPtr, right: &PartInfoPtr| {
        let left_stats = FuseBlockPartInfo::from_part(left)
            .ok()
            .and_then(|info| info.columns_stat.as_ref());
        let right_stats = FuseBlockPartInfo::from_part(right)
            .ok()
            .and_then(|info| info.columns_stat.as_ref());
        order.compare_ranks(&order.rank(left_stats), &order.rank(right_stats))
    };

    if parts.len() > head {
        parts.select_nth_unstable_by(head - 1, compare);
        parts[..head].sort_unstable_by(compare);
    } else {
        parts.sort_unstable_by(compare);
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::Compression;

    use super::*;

    fn int64(value: i64) -> Scalar {
        Scalar::Number(NumberScalar::Int64(value))
    }

    fn stats(min: i64, max: i64, null_count: u64) -> ColumnStatistics {
        ColumnStatistics::new(int64(min), int64(max), null_count, 0, None)
    }

    fn part_with_stats(location: &str, min_max: Option<(i64, i64)>) -> PartInfoPtr {
        part_with_nullable_stats(location, min_max.map(|(min, max)| (min, max, 0)))
    }

    fn part_with_nullable_stats(
        location: &str,
        min_max_nulls: Option<(i64, i64, u64)>,
    ) -> PartInfoPtr {
        FuseBlockPartInfo::create(
            location.to_string(),
            None,
            0,
            None,
            1,
            HashMap::new(),
            min_max_nulls.map(|(min, max, nulls)| HashMap::from([(3, stats(min, max, nulls))])),
            Compression::Lz4Raw,
            None,
            None,
            None,
        )
    }

    fn part_locations(parts: &[PartInfoPtr]) -> Vec<&str> {
        parts
            .iter()
            .map(|part| {
                FuseBlockPartInfo::from_part(part)
                    .unwrap()
                    .location
                    .as_str()
            })
            .collect()
    }

    #[test]
    fn test_front_load_parts_schedules_promising_blocks_first() {
        let mut parts = vec![
            part_with_stats("mid", Some((4, 40))),
            part_with_stats("no_stats", None),
            part_with_stats("high", Some((7, 70))),
            part_with_stats("low", Some((1, 10))),
        ];

        let no_filters = RuntimeScanFilters::default();
        front_load_parts_for_runtime_top_n(&mut parts, &no_filters, 1024);
        assert_eq!(part_locations(&parts), vec![
            "mid", "no_stats", "high", "low"
        ]);

        let mut asc = RuntimeScanFilters::default();
        asc.push(Arc::new(RuntimeTopNFilter::new(3, true, false)));
        front_load_parts_for_runtime_top_n(&mut parts, &asc, 1024);
        assert_eq!(part_locations(&parts), vec![
            "low", "mid", "high", "no_stats"
        ]);

        let mut desc = RuntimeScanFilters::default();
        desc.push(Arc::new(RuntimeTopNFilter::new(3, false, false)));
        front_load_parts_for_runtime_top_n(&mut parts, &desc, 1024);
        assert_eq!(part_locations(&parts), vec![
            "high", "mid", "low", "no_stats"
        ]);

        front_load_parts_for_runtime_top_n(&mut parts, &asc, 2);
        assert_eq!(part_locations(&parts)[..2], ["low", "mid"]);
        let mut tail = part_locations(&parts)[2..].to_vec();
        tail.sort_unstable();
        assert_eq!(tail, vec!["high", "no_stats"]);
    }

    #[test]
    fn test_front_load_ranks_null_bearing_blocks_best_under_nulls_first() {
        let mut nulls_first = RuntimeScanFilters::default();
        nulls_first.push(Arc::new(RuntimeTopNFilter::new(3, true, true)));
        let mut parts = vec![
            part_with_stats("low", Some((1, 10))),
            part_with_nullable_stats("with_nulls", Some((50, 60, 2))),
            part_with_stats("no_stats", None),
        ];

        front_load_parts_for_runtime_top_n(&mut parts, &nulls_first, 1024);
        assert_eq!(part_locations(&parts), vec![
            "with_nulls",
            "low",
            "no_stats"
        ]);

        let mut nulls_last = RuntimeScanFilters::default();
        nulls_last.push(Arc::new(RuntimeTopNFilter::new(3, true, false)));
        front_load_parts_for_runtime_top_n(&mut parts, &nulls_last, 1024);
        assert_eq!(part_locations(&parts), vec![
            "low",
            "with_nulls",
            "no_stats"
        ]);
    }
}
