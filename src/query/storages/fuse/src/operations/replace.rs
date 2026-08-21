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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_pipeline::core::PipeItem;
use databend_common_sql::executor::physical_plans::OnConflictField;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::Location;
use rand::prelude::SliceRandom;

use crate::FuseTable;
use crate::operations::mutation::SegmentIndex;
use crate::operations::replace_into::ReplaceIntoMutatorParams;
use crate::operations::replace_into::ReplaceIntoOperationAggregator;

impl FuseTable {
    // The big picture of the replace into pipeline:
    //
    // - If table is not empty:
    //
    //                      ┌──────────────────────┐            ┌──────────────────┐               ┌────────────────┐
    //                      │                      ├──┬────────►│ SerializeBlock   ├──────────────►│SerializeSegment├───────────────────────┐
    // ┌─────────────┐      │                      ├──┘         └──────────────────┘               └────────────────┘                       │
    // │ UpsertSource├─────►│ ReplaceIntoProcessor │                                                                                        │
    // └─────────────┘      │                      ├──┐         ┌───────────────────┐              ┌──────────────────────┐                 │
    //                      │                      ├──┴────────►│                   ├──┬──────────►│MergeIntoOperationAggr├─────────────────┤
    //                      └──────────────────────┘            │                   ├──┘           └──────────────────────┘                 │
    //                                                          │ BroadcastProcessor│                                                       ├───────┐
    //                                                          │                   ├──┐           ┌──────────────────────┐                 │       │
    //                                                          │                   ├──┴──────────►│MergeIntoOperationAggr├─────────────────┤       │
    //                                                          │                   │              └──────────────────────┘                 │       │
    //                                                          │                   ├──┐                                                    │       │
    //                                                          │                   ├──┴──────────►┌──────────────────────┐                 │       │
    //                                                          └───────────────────┘              │MergeIntoOperationAggr├─────────────────┘       │
    //                                                                                             └──────────────────────┘                         │
    //                                                                                                                                              │
    //                                                                                                                                              │
    //                                                                                                                                              │
    //                                                                                                                                              │
    //                                                                                                                                              │
    //                                                                                                                                              │
    //                 ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    //                 │
    //                 │
    //                 │      ┌───────────────────┐       ┌───────────────────────┐         ┌───────────────────┐
    //                 └─────►│ResizeProcessor(1) ├──────►│TableMutationAggregator├────────►│     CommitSink    │
    //                        └───────────────────┘       └───────────────────────┘         └───────────────────┘
    //
    //
    //  - If table is empty:
    //
    //
    //                      ┌──────────────────────┐            ┌─────────────────┐         ┌─────────────────┐
    //                      │                      ├──┬────────►│ SerializeBlock  ├────────►│SerializeSegment ├─────────┐
    // ┌─────────────┐      │                      ├──┘         └─────────────────┘         └─────────────────┘         │
    // │ UpsertSource├─────►│ ReplaceIntoProcessor │                                                                    ├─────┐
    // └─────────────┘      │                      ├──┐         ┌─────────────────┐         ┌─────────────────┐         │     │
    //                      │                      ├──┴────────►│  DummyTransform ├────────►│  DummyTransform ├─────────┘     │
    //                      └──────────────────────┘            └─────────────────┘         └─────────────────┘               │
    //                                                                                                                        │
    //                                                                                                                        │
    //                                                                                                                        │
    //                      ┌─────────────────────────────────────────────────────────────────────────────────────────────────┘
    //                      │
    //                      │
    //                      │      ┌───────────────────┐       ┌───────────────────────┐         ┌───────────────────┐
    //                      └─────►│ResizeProcessor(1) ├──────►│TableMutationAggregator├────────►│     CommitSink    │
    //                             └───────────────────┘       └───────────────────────┘         └───────────────────┘

    pub fn merge_into_mutators(
        &self,
        num_partition: usize,
        segments: &[(usize, Location)],
        params: ReplaceIntoMutatorParams,
    ) -> Result<Vec<PipeItem>> {
        let chunks = Self::partition_segments(segments, num_partition);
        let mut items = Vec::with_capacity(num_partition);
        for segment_locations in chunks {
            let item = ReplaceIntoOperationAggregator::try_create(
                self,
                params.clone(),
                segment_locations,
            )?;
            items.push(item.into_pipe_item());
        }
        Ok(items)
    }

    pub fn partition_segments(
        segments: &[(usize, Location)],
        num_partition: usize,
    ) -> Vec<Vec<(SegmentIndex, Location)>> {
        let chunk_size = segments.len() / num_partition;
        assert!(chunk_size >= 1);
        let mut segments = segments.to_vec();

        segments.shuffle(&mut rand::thread_rng());

        let mut chunks = Vec::with_capacity(num_partition);
        for chunk in segments.chunks(chunk_size) {
            let mut segment_chunk = chunk
                .iter()
                .map(|(segment_idx, location)| (*segment_idx, (*location).clone()))
                .collect::<Vec<_>>();
            if chunks.len() < num_partition {
                chunks.push(segment_chunk);
            } else {
                chunks.last_mut().unwrap().append(&mut segment_chunk);
            }
        }
        chunks
    }

    // choose the bloom filter columns (from on-conflict fields).
    // columns with larger number of number-of-distinct-values, will be kept, is their types
    // are supported by bloom index.
    pub async fn choose_bloom_filter_columns(
        &self,
        ctx: Arc<dyn TableContext>,
        on_conflicts: &[OnConflictField],
        max_num_columns: u64,
    ) -> Result<Vec<FieldIndex>> {
        let col_stats_provider = self.column_statistics_provider(ctx).await?;
        let mut cols = on_conflicts
            .iter()
            .enumerate()
            .filter_map(|(idx, key)| {
                if !BloomIndex::supported_type(&key.table_field.data_type) {
                    None
                } else {
                    let maybe_col_stats =
                        col_stats_provider.column_statistics(key.table_field.column_id);
                    // Safe to unwrap: ndv in FuseTable's ColumnStatistics is not None.
                    maybe_col_stats.map(|col_stats| (idx, col_stats.ndv.unwrap()))
                }
            })
            .collect::<Vec<_>>();

        cols.sort_by(|l, r| l.1.cmp(&r.1).reverse());
        Ok(cols
            .into_iter()
            .map(|v| v.0)
            .take(max_num_columns as usize)
            .collect())
    }
}
