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

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_catalog::runtime_filter_info::RuntimeScanFilter;
use databend_common_catalog::runtime_filter_info::RuntimeScanOrder;
use databend_common_catalog::runtime_filter_info::RuntimeTopNRank;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;

use crate::pruning_pipeline::pruned_segment_meta::PrunedSegmentMeta;

/// One buffered segment, ordered so the most promising segment is the
/// greatest entry (`BinaryHeap` pops the greatest first).
struct Entry {
    rank: RuntimeTopNRank<Scalar>,
    order: RuntimeScanOrder,
    block: DataBlock,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // `compare_ranks` puts better-ranked entries first (`Less`); reverse
        // it so they become the heap maximum.
        self.order.compare_ranks(&self.rank, &other.rank).reverse()
    }
}

/// Reorder pruned segments under runtime TopN so the most promising ones
/// (ranked by the sort column's segment-level statistics) are block-pruned
/// and read first, letting the shared boundary converge early.
///
/// Segments the boundary already excludes are dropped on entry, and every
/// emission re-checks the boundary. A bounded sliding window keeps memory at
/// O(window) segment metas and avoids a full pipeline barrier: once the
/// window is full, every incoming segment releases the best buffered one.
pub struct RuntimeTopNSegmentReorder<M: PrunedSegmentMeta + BlockMetaInfo> {
    filter: Arc<dyn RuntimeScanFilter>,
    order: RuntimeScanOrder,
    window: usize,
    buffered: BinaryHeap<Entry>,
    _phantom: PhantomData<M>,
}

impl<M: PrunedSegmentMeta + BlockMetaInfo> RuntimeTopNSegmentReorder<M> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        filter: Arc<dyn RuntimeScanFilter>,
        order: RuntimeScanOrder,
        window: usize,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AccumulatingTransformer::create(
            input,
            output,
            Self::new(filter, order, window),
        )))
    }

    fn new(filter: Arc<dyn RuntimeScanFilter>, order: RuntimeScanOrder, window: usize) -> Self {
        Self {
            filter,
            order,
            window: window.max(1),
            buffered: BinaryHeap::new(),
            _phantom: PhantomData,
        }
    }

    fn column_stats(block: &DataBlock) -> Option<&HashMap<ColumnId, ColumnStatistics>> {
        let meta = block.get_meta()?;
        let meta = M::downcast_ref_from(meta)?;
        Some(&meta.segment().summary().col_stats)
    }

    fn should_prune(&self, block: &DataBlock) -> bool {
        let stats = Self::column_stats(block);
        self.filter.should_prune(stats)
    }
}

impl<M: PrunedSegmentMeta + BlockMetaInfo> AccumulatingTransform for RuntimeTopNSegmentReorder<M> {
    const NAME: &'static str = "RuntimeTopNSegmentReorder";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.should_prune(&block) {
            return Ok(vec![]);
        }

        let rank = self.order.rank(Self::column_stats(&block)).cloned();
        self.buffered.push(Entry {
            rank,
            order: self.order,
            block,
        });

        if self.buffered.len() > self.window {
            while let Some(entry) = self.buffered.pop() {
                if !self.should_prune(&entry.block) {
                    return Ok(vec![entry.block]);
                }
            }
        }
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            return Ok(vec![]);
        }
        let mut blocks = Vec::with_capacity(self.buffered.len());
        while let Some(entry) = self.buffered.pop() {
            if !self.should_prune(&entry.block) {
                blocks.push(entry.block);
            }
        }
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::CompactSegmentInfo;
    use databend_storages_common_table_meta::meta::SegmentInfo;
    use databend_storages_common_table_meta::meta::Statistics;

    use super::*;
    use crate::SegmentLocation;
    use crate::pruning_pipeline::PrunedCompactSegmentMeta;

    fn segment_block(location: &str, min: i64, max: i64) -> DataBlock {
        segment_block_with_nulls(location, min, max, 0)
    }

    fn segment_block_with_nulls(location: &str, min: i64, max: i64, null_count: u64) -> DataBlock {
        let col_stats = HashMap::from([(
            3,
            ColumnStatistics::new(
                Scalar::Number(NumberScalar::Int64(min)),
                Scalar::Number(NumberScalar::Int64(max)),
                null_count,
                0,
                None,
            ),
        )]);
        let summary = Statistics {
            col_stats,
            ..Statistics::default()
        };
        let segment = CompactSegmentInfo::try_from(SegmentInfo::new(vec![], summary)).unwrap();
        let segment_location = SegmentLocation {
            segment_idx: 0,
            location: (location.to_string(), 0),
            snapshot_loc: None,
        };
        DataBlock::empty_with_meta(PrunedCompactSegmentMeta::create((
            segment_location,
            Arc::new(segment),
        )))
    }

    fn locations(blocks: &[DataBlock]) -> Vec<String> {
        blocks
            .iter()
            .map(|block| {
                let meta = block.get_meta().unwrap();
                let meta = PrunedCompactSegmentMeta::downcast_ref_from(meta).unwrap();
                meta.segments.0.location.0.clone()
            })
            .collect()
    }

    #[test]
    fn test_window_releases_most_promising_segment_first() -> Result<()> {
        let filter = Arc::new(RuntimeTopNFilter::new(3, true, false));
        let order = filter.preferred_order().unwrap();
        let mut reorder =
            RuntimeTopNSegmentReorder::<PrunedCompactSegmentMeta>::new(filter, order, 2);

        // The window buffers the first two segments.
        assert!(reorder.transform(segment_block("mid", 20, 29))?.is_empty());
        assert!(reorder.transform(segment_block("high", 30, 39))?.is_empty());

        // A full window releases the best (smallest min for ASC) segment.
        let released = reorder.transform(segment_block("low", 10, 19))?;
        assert_eq!(locations(&released), vec!["low"]);

        // Finish drains the rest in rank order.
        let drained = reorder.on_finish(true)?;
        assert_eq!(locations(&drained), vec!["mid", "high"]);
        Ok(())
    }

    #[test]
    fn test_nulls_first_ranks_null_bearing_segments_best() -> Result<()> {
        let filter = Arc::new(RuntimeTopNFilter::new(3, true, true));
        let order = filter.preferred_order().unwrap();
        let mut reorder =
            RuntimeTopNSegmentReorder::<PrunedCompactSegmentMeta>::new(filter, order, 1);

        assert!(reorder.transform(segment_block("low", 10, 19))?.is_empty());
        // Under NULLS FIRST a null-bearing segment outranks any value, even a
        // smaller min.
        let released = reorder.transform(segment_block_with_nulls("with_nulls", 50, 59, 7))?;
        assert_eq!(locations(&released), vec!["with_nulls"]);

        let drained = reorder.on_finish(true)?;
        assert_eq!(locations(&drained), vec!["low"]);
        Ok(())
    }

    #[test]
    fn test_boundary_prunes_segments_at_entry_and_emission() -> Result<()> {
        let filter = Arc::new(RuntimeTopNFilter::new(3, true, false));
        let order = filter.preferred_order().unwrap();
        let mut reorder =
            RuntimeTopNSegmentReorder::<PrunedCompactSegmentMeta>::new(filter.clone(), order, 8);

        assert!(reorder.transform(segment_block("keep", 1, 9))?.is_empty());
        assert!(
            reorder
                .transform(segment_block("late_pruned", 20, 29))?
                .is_empty()
        );

        filter.update(&Scalar::Number(NumberScalar::Int64(10)));

        // Entry-time filter drops segments the boundary already excludes.
        assert!(
            reorder
                .transform(segment_block("entry_pruned", 30, 39))?
                .is_empty()
        );

        // Draining re-checks buffered segments with the tightened boundary.
        let drained = reorder.on_finish(true)?;
        assert_eq!(locations(&drained), vec!["keep"]);
        Ok(())
    }
}
