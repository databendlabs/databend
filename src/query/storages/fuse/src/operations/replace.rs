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

use common_base::base::tokio::sync::Semaphore;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::FieldIndex;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use common_sql::executor::MutationKind;
use common_sql::executor::OnConflictField;
use rand::prelude::SliceRandom;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::TableSnapshot;

use crate::io::BlockBuilder;
use crate::io::ReadSettings;
use crate::operations::common::CommitSink;
use crate::operations::common::MutationGenerator;
use crate::operations::common::TableMutationAggregator;
use crate::operations::mutation::SegmentIndex;
use crate::operations::replace_into::MergeIntoOperationAggregator;
use crate::pipelines::Pipeline;
use crate::FuseTable;

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

    #[allow(clippy::too_many_arguments)]
    pub fn merge_into_mutators(
        &self,
        ctx: Arc<dyn TableContext>,
        num_partition: usize,
        block_builder: BlockBuilder,
        on_conflicts: Vec<OnConflictField>,
        bloom_filter_column_indexes: Vec<FieldIndex>,
        segments: &[(usize, Location)],
        io_request_semaphore: Arc<Semaphore>,
    ) -> Result<Vec<PipeItem>> {
        let chunks = Self::partition_segments(segments, num_partition);
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let mut items = Vec::with_capacity(num_partition);
        for chunk_of_segment_locations in chunks {
            let item = MergeIntoOperationAggregator::try_create(
                ctx.clone(),
                on_conflicts.clone(),
                bloom_filter_column_indexes.clone(),
                chunk_of_segment_locations,
                self.operator.clone(),
                self.table_info.schema(),
                self.get_write_settings(),
                read_settings.clone(),
                block_builder.clone(),
                io_request_semaphore.clone(),
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

    pub fn chain_mutation_pipes(
        &self,
        ctx: &Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        base_snapshot: Arc<TableSnapshot>,
        mutation_kind: MutationKind,
    ) -> Result<()> {
        // resize
        pipeline.try_resize(1)?;

        // a) append TableMutationAggregator
        pipeline.add_transform(|input, output| {
            let base_segments = base_snapshot.segments.clone();
            let base_summary = base_snapshot.summary.clone();
            let mutation_aggregator = TableMutationAggregator::create(
                self,
                ctx.clone(),
                base_segments,
                base_summary,
                mutation_kind,
            );
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input,
                output,
                mutation_aggregator,
            )))
        })?;

        // b) append  CommitSink
        let snapshot_gen = MutationGenerator::new(base_snapshot);
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                None,
                snapshot_gen.clone(),
                input,
                None,
                false,
                None,
            )
        })?;
        Ok(())
    }

    // choose the bloom filter columns (from on-conflict fields).
    // columns with larger number of number-of-distinct-values, will be kept, is their types
    // are supported by bloom index.
    pub async fn choose_bloom_filter_columns(
        &self,
        on_conflicts: &[OnConflictField],
        max_num_columns: u64,
    ) -> Result<Vec<FieldIndex>> {
        let col_stats_provider = self.column_statistics_provider().await?;
        let mut cols = on_conflicts
            .iter()
            .enumerate()
            .filter_map(|(idx, key)| {
                if !BloomIndex::supported_type(&key.table_field.data_type) {
                    None
                } else {
                    let maybe_col_stats =
                        col_stats_provider.column_statistics(key.table_field.column_id);
                    maybe_col_stats.map(|col_stats| (idx, col_stats.number_of_distinct_values))
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
