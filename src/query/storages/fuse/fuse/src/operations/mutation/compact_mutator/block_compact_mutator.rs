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

use std::sync::Arc;
use std::vec;

use common_catalog::plan::Partitions;
use common_datablocks::BlockCompactThresholds;
use common_exception::Result;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use opendal::Operator;

use super::compact_part::CompactPartInfo;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_set_segments_memory_usage;
use crate::operations::CompactOptions;
use crate::statistics::reducers::merge_statistics_mut;
use crate::TableContext;

pub struct BlockCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    thresholds: BlockCompactThresholds,
    location_generator: TableMetaLocationGenerator,
    compact_tasks: Partitions,
    // summarised statistics of all the unchanged segments
    unchanged_segment_statistics: Statistics,
    // locations all the unchanged segments, with index and location.
    unchanged_segments_locations: Vec<(usize, Location)>,
}

impl BlockCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let snapshot = self.compact_params.base_snapshot.clone();
        let segment_locations = &snapshot.segments;

        // Read all segments information in parallel.
        let segments_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let segments = segments_io
            .read_segments(segment_locations)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // todo: add real metrics
        metrics_set_segments_memory_usage(0.0);

        let number_segments = segments.len();
        let limit = self.compact_params.limit.unwrap_or(number_segments);
        let blocks_per_seg = self.compact_params.block_per_seg as u64;

        let mut order = 0;
        let mut end = 0;
        let mut compacted_segment_cnt = 0;

        let mut builder = CompactPartBuilder::new(blocks_per_seg);

        for (idx, segment) in segments.iter().enumerate() {
            let tasks = builder.add(segment.clone());
            for t in tasks {
                if CompactPartBuilder::check_for_compact(&t) {
                    compacted_segment_cnt += t.len();
                    self.compact_tasks.push(CompactPartInfo::create(t, order));
                } else {
                    self.unchanged_segments_locations
                        .push((order, segment_locations[idx].clone()));
                    merge_statistics_mut(
                        &mut self.unchanged_segment_statistics,
                        &segments[idx].summary,
                    )?;
                }
                order += 1;
            }
            if compacted_segment_cnt >= limit {
                end = idx + 1;
                break;
            }
        }

        if !builder.is_empty() {
            let t = std::mem::take(&mut builder.segments);
            if CompactPartBuilder::check_for_compact(&t) {
                self.compact_tasks.push(CompactPartInfo::create(t, order));
            } else {
                self.unchanged_segments_locations
                    .push((order, segment_locations[end - 1].clone()));
                merge_statistics_mut(
                    &mut self.unchanged_segment_statistics,
                    &segments[end - 1].summary,
                )?;
            }
            order += 1;
        }

        if self.compact_tasks.is_empty() {
            return Ok(false);
        }

        if end < number_segments {
            for i in end..number_segments {
                self.unchanged_segments_locations
                    .push((order, segment_locations[i].clone()));
                merge_statistics_mut(&mut self.unchanged_segment_statistics, &segments[i].summary)?;
                order += 1;
            }
        }

        Ok(true)
    }
}

#[derive(Default)]
struct CompactPartBuilder {
    segments: Vec<Arc<SegmentInfo>>,
    block_count: u64,
    threshold: u64,
}

impl CompactPartBuilder {
    fn new(threshold: u64) -> Self {
        Self {
            threshold,
            ..Default::default()
        }
    }

    fn check_for_compact(segments: &Vec<Arc<SegmentInfo>>) -> bool {
        segments.len() != 1
            || segments[0].summary.perfect_block_count != segments[0].summary.block_count
    }

    fn add(&mut self, segment: Arc<SegmentInfo>) -> Vec<Vec<Arc<SegmentInfo>>> {
        self.block_count += segment.summary.block_count;
        if self.block_count < self.threshold {
            self.segments.push(segment);
            return vec![];
        }

        if self.block_count > 2 * self.threshold {
            self.block_count = 0;
            let trival = vec![segment];
            if self.segments.is_empty() {
                return vec![trival];
            } else {
                return vec![std::mem::take(&mut self.segments), trival];
            }
        }

        self.block_count = 0;
        self.segments.push(segment);
        vec![std::mem::take(&mut self.segments)]
    }

    fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}
