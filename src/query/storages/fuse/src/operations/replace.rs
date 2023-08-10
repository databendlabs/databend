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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::TableField;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::create_dummy_item;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use log::info;
use rand::prelude::SliceRandom;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::TableSnapshot;

use super::common::MutationKind;
use crate::io::BlockBuilder;
use crate::io::ReadSettings;
use crate::operations::common::CommitSink;
use crate::operations::common::MutationGenerator;
use crate::operations::common::TableMutationAggregator;
use crate::operations::common::TransformSerializeBlock;
use crate::operations::common::TransformSerializeSegment;
use crate::operations::mutation::SegmentIndex;
use crate::operations::replace_into::BroadcastProcessor;
use crate::operations::replace_into::MergeIntoOperationAggregator;
use crate::operations::replace_into::OnConflictField;
use crate::operations::replace_into::ReplaceIntoProcessor;
use crate::pipelines::Pipeline;
use crate::FuseTable;

impl FuseTable {
    // The pipeline going to be constructed
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

    #[async_backtrace::framed]
    pub async fn build_replace_pipeline<'a>(
        &'a self,
        ctx: Arc<dyn TableContext>,
        on_conflict_field_identifiers: Vec<TableField>,
        pipeline: &'a mut Pipeline,
    ) -> Result<()> {
        let schema = self.table_info.schema();

        let mut on_conflicts = Vec::with_capacity(on_conflict_field_identifiers.len());
        for f in on_conflict_field_identifiers {
            let field_name = f.name();
            let (field_index, _) =
                schema
                    .column_with_name(field_name)
                    .ok_or(ErrorCode::Internal(
                        "not expected, on conflict field not found (after binding)",
                    ))?;
            on_conflicts.push(OnConflictField {
                table_field: f.clone(),
                field_index,
            })
        }

        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, self.get_block_thresholds())?;

        // 1. resize input to 1, since the UpsertTransform need to de-duplicate inputs "globally"
        pipeline.try_resize(1)?;

        // 2. connect with ReplaceIntoProcessor

        //                      ┌──────────────────────┐
        //                      │                      ├──┐
        // ┌─────────────┐      │                      ├──┘
        // │ UpsertSource├─────►│ ReplaceIntoProcessor │
        // └─────────────┘      │                      ├──┐
        //                      │                      ├──┘
        //                      └──────────────────────┘
        // NOTE: here the pipe items of last pipe are arranged in the following order
        // (0) -> output_port_append_data
        // (1) -> output_port_merge_into_action
        //    the "downstream" is supposed to be connected with a processor which can process MergeIntoOperations
        //    in our case, it is the broadcast processor

        let base_snapshot = self.read_table_snapshot().await?.unwrap_or_else(|| {
            Arc::new(TableSnapshot::new_empty_snapshot(schema.as_ref().clone()))
        });

        let table_is_empty = base_snapshot.segments.is_empty();
        let table_level_range_index = base_snapshot.summary.col_stats.clone();
        let cluster_keys = self.cluster_keys(ctx.clone());

        // currently, we only try apply bloom filter pruning when the table is clustered
        let bloom_filter_column_indexes: Vec<FieldIndex> = if !cluster_keys.is_empty()
            && ctx.get_settings().get_enable_replace_into_bloom_pruning()?
        {
            let max_num_pruning_columns = ctx
                .get_settings()
                .get_replace_into_bloom_pruning_max_column_number()?;
            self.choose_bloom_filter_columns(&on_conflicts, max_num_pruning_columns)
                .await?
        } else {
            info!("replace-into, bloom filter pruning not enabled.");
            vec![]
        };

        info!(
            "replace-into, bloom filter field chosen, {:?}",
            bloom_filter_column_indexes
                .iter()
                .map(|idx| (idx, on_conflicts[*idx].table_field.name.clone()))
                .collect::<Vec<_>>(),
        );

        let replace_into_processor = ReplaceIntoProcessor::create(
            ctx.as_ref(),
            on_conflicts.clone(),
            cluster_keys,
            bloom_filter_column_indexes.clone(),
            schema.as_ref(),
            table_is_empty,
            table_level_range_index,
        )?;

        pipeline.add_pipe(replace_into_processor.into_pipe());

        // 3. connect to broadcast processor and append transform

        let max_threads = ctx.get_settings().get_max_threads()?;
        let segment_partition_num =
            std::cmp::min(base_snapshot.segments.len(), max_threads as usize);

        let serialize_block_transform = TransformSerializeBlock::try_create(
            ctx.clone(),
            InputPort::create(),
            OutputPort::create(),
            self,
            cluster_stats_gen,
        )?;
        let block_builder = serialize_block_transform.get_block_builder();

        let serialize_segment_transform = TransformSerializeSegment::new(
            InputPort::create(),
            OutputPort::create(),
            self,
            self.get_block_thresholds(),
        );

        if segment_partition_num == 0 {
            let dummy_item = create_dummy_item();
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│  SerializeBlock  │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│  DummyTransform  │
            //                      └──────────────────────┘            └──────────────────┘
            // wrap them into pipeline, order matters!
            pipeline.add_pipe(Pipe::create(2, 2, vec![
                serialize_block_transform.into_pipe_item(),
                dummy_item,
            ]));
        } else {
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│ SerializeBlock   │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│BroadcastProcessor│
            //                      └──────────────────────┘            └──────────────────┘
            let broadcast_processor = BroadcastProcessor::new(segment_partition_num);
            // wrap them into pipeline, order matters!
            pipeline.add_pipe(Pipe::create(2, segment_partition_num + 1, vec![
                serialize_block_transform.into_pipe_item(),
                broadcast_processor.into_pipe_item(),
            ]));
        };

        // 4. connect with MergeIntoOperationAggregators
        if segment_partition_num == 0 {
            let dummy_item = create_dummy_item();
            pipeline.add_pipe(Pipe::create(2, 2, vec![
                serialize_segment_transform.into_pipe_item(),
                dummy_item,
            ]));
        } else {
            //      ┌──────────────────┐               ┌────────────────┐
            // ────►│  SerializeBlock  ├──────────────►│SerializeSegment│
            //      └──────────────────┘               └────────────────┘
            //
            //      ┌───────────────────┐              ┌──────────────────────┐
            // ────►│                   ├──┬──────────►│MergeIntoOperationAggr│
            //      │                   ├──┘           └──────────────────────┘
            //      │ BroadcastProcessor│
            //      │                   ├──┐           ┌──────────────────────┐
            //      │                   ├──┴──────────►│MergeIntoOperationAggr│
            //      │                   │              └──────────────────────┘
            //      │                   ├──┐
            //      │                   ├──┴──────────►┌──────────────────────┐
            //      └───────────────────┘              │MergeIntoOperationAggr│
            //                                         └──────────────────────┘

            let item_size = segment_partition_num + 1;
            let mut pipe_items = Vec::with_capacity(item_size);
            // setup the dummy transform
            pipe_items.push(serialize_segment_transform.into_pipe_item());

            let max_io_request = ctx.get_settings().get_max_storage_io_requests()?;
            let io_request_semaphore = Arc::new(Semaphore::new(max_io_request as usize));

            // setup the merge into operation aggregators
            let mut merge_into_operation_aggregators = self
                .merge_into_mutators(
                    ctx.clone(),
                    segment_partition_num,
                    block_builder,
                    on_conflicts.clone(),
                    bloom_filter_column_indexes,
                    &base_snapshot,
                    io_request_semaphore,
                )
                .await?;
            assert_eq!(
                segment_partition_num,
                merge_into_operation_aggregators.len()
            );
            pipe_items.append(&mut merge_into_operation_aggregators);

            // extend the pipeline
            assert_eq!(pipeline.output_len(), item_size);
            assert_eq!(pipe_items.len(), item_size);
            pipeline.add_pipe(Pipe::create(item_size, item_size, pipe_items));
        }

        // 5. connect with mutation pipes, the TableMutationAggregator, then CommitSink
        //
        //    ┌───────────────────┐       ┌───────────────────────┐         ┌───────────────────┐
        //    │ResizeProcessor(1) ├──────►│TableMutationAggregator├────────►│     CommitSink    │
        //    └───────────────────┘       └───────────────────────┘         └───────────────────┘
        self.chain_mutation_pipes(&ctx, pipeline, base_snapshot, MutationKind::Replace)?;

        Ok(())
    }

    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    async fn merge_into_mutators(
        &self,
        ctx: Arc<dyn TableContext>,
        num_partition: usize,
        block_builder: BlockBuilder,
        on_conflicts: Vec<OnConflictField>,
        bloom_filter_column_indexes: Vec<FieldIndex>,
        table_snapshot: &TableSnapshot,
        io_request_semaphore: Arc<Semaphore>,
    ) -> Result<Vec<PipeItem>> {
        let chunks = Self::partition_segments(&table_snapshot.segments, num_partition);
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
        segments: &[Location],
        num_partition: usize,
    ) -> Vec<Vec<(SegmentIndex, Location)>> {
        let chunk_size = segments.len() / num_partition;
        assert!(chunk_size >= 1);

        let mut indexed_segment = segments.iter().enumerate().collect::<Vec<_>>();
        indexed_segment.shuffle(&mut rand::thread_rng());

        let mut chunks = Vec::with_capacity(num_partition);
        for chunk in indexed_segment.chunks(chunk_size) {
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
    async fn choose_bloom_filter_columns(
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
