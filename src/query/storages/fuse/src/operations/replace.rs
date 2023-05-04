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

use std::default::Default;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableField;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::create_dummy_item;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

use crate::io::BlockBuilder;
use crate::io::ReadSettings;
use crate::operations::merge_into::AppendTransform;
use crate::operations::merge_into::BroadcastProcessor;
use crate::operations::merge_into::CommitSink;
use crate::operations::merge_into::MergeIntoOperationAggregator;
use crate::operations::merge_into::OnConflictField;
use crate::operations::merge_into::TableMutationAggregator;
use crate::operations::mutation::base_mutator::SegmentIndex;
use crate::operations::replace_into::processor_replace_into::ReplaceIntoProcessor;
use crate::pipelines::Pipeline;
use crate::FuseTable;

impl FuseTable {
    // The pipeline going to be constructed
    //
    // - If table is not empty:
    //
    //                      ┌──────────────────────┐            ┌──────────────────┐               ┌──────────────┐
    //                      │                      ├──┬────────►│ AppendTransform  ├──────────────►│DummyTransform├─────────────────────────┐
    // ┌─────────────┐      │                      ├──┘         └──────────────────┘               └──────────────┘                         │
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
    //                      ┌──────────────────────┐            ┌──────────────────┐
    //                      │                      ├──┬────────►│ AppendTransform  ├────────────────────────────────────┐
    // ┌─────────────┐      │                      ├──┘         └──────────────────┘                                    │
    // │ UpsertSource├─────►│ ReplaceIntoProcessor │                                                                    ├─────┐
    // └─────────────┘      │                      ├──┐         ┌──────────────────┐                                    │     │
    //                      │                      ├──┴────────►│  DummyTransform  ├────────────────────────────────────┘     │
    //                      └──────────────────────┘            └──────────────────┘                                          │
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
            let (field_index, _) = match schema.column_with_name(field_name) {
                Some(idx) => idx,
                None => {
                    return Err(ErrorCode::Internal(
                        "not expected, on conflict field not found (after binding)",
                    ));
                }
            };
            on_conflicts.push(OnConflictField {
                table_field: f.clone(),
                field_index,
            })
        }

        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, self.get_block_thresholds())?;

        // 1. resize input to 1, since the UpsertTransform need to de-duplicate inputs "globally"
        pipeline.resize(1)?;

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

        let base_snapshot = self
            .read_table_snapshot()
            .await?
            .unwrap_or_else(|| Arc::new(self.new_empty_snapshot()));

        let empty_table = base_snapshot.segments.is_empty();
        let replace_into_processor =
            ReplaceIntoProcessor::create(on_conflicts.clone(), empty_table);
        pipeline.add_pipe(replace_into_processor.into_pipe());

        // 3. connect to broadcast processor and append transform

        let base_snapshot = self
            .read_table_snapshot()
            .await?
            .unwrap_or_else(|| Arc::new(self.new_empty_snapshot()));

        let max_threads = ctx.get_settings().get_max_threads()?;
        let segment_partition_num =
            std::cmp::min(base_snapshot.segments.len(), max_threads as usize);

        let append_transform = AppendTransform::try_create(
            ctx.clone(),
            self.get_write_settings(),
            self.operator.clone(),
            self.meta_location_generator.clone(),
            self.table_info.schema(),
            self.get_block_thresholds(),
            cluster_stats_gen,
        );
        let block_builder = append_transform.get_block_builder();

        if segment_partition_num == 0 {
            let dummy_item = create_dummy_item();
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│ AppendTransform  │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│  DummyTransform  │
            //                      └──────────────────────┘            └──────────────────┘
            // wrap them into pipeline, order matters!
            pipeline.add_pipe(Pipe::create(2, 2, vec![
                append_transform.into_pipe_item(),
                dummy_item,
            ]));
        } else {
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│ AppendTransform  │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│BroadcastProcessor│
            //                      └──────────────────────┘            └──────────────────┘
            let broadcast_processor = BroadcastProcessor::new(segment_partition_num);
            // wrap them into pipeline, order matters!
            pipeline.add_pipe(Pipe::create(2, segment_partition_num + 1, vec![
                append_transform.into_pipe_item(),
                broadcast_processor.into_pipe_item(),
            ]));
        };

        // 4. connect with MergeIntoOperationAggregators
        if segment_partition_num == 0 {
            // do nothing
        } else {
            //      ┌──────────────────┐               ┌──────────────┐
            // ────►│ AppendTransform  ├──────────────►│DummyTransform│
            //      └──────────────────┘               └──────────────┘
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
            let dummy_item = create_dummy_item();
            pipe_items.push(dummy_item);

            // setup the merge into operation aggregators
            let mut merge_into_operation_aggregators = self
                .merge_into_mutators(
                    ctx.clone(),
                    segment_partition_num,
                    block_builder,
                    on_conflicts.clone(),
                    &base_snapshot,
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
        self.chain_mutation_pipes(&ctx, pipeline, base_snapshot)
            .await?;

        Ok(())
    }

    #[async_backtrace::framed]
    async fn merge_into_mutators(
        &self,
        ctx: Arc<dyn TableContext>,
        num_partition: usize,
        block_builder: BlockBuilder,
        on_conflicts: Vec<OnConflictField>,
        table_snapshot: &TableSnapshot,
    ) -> Result<Vec<PipeItem>> {
        let chunks = Self::partition_segments(&table_snapshot.segments, num_partition);
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let mut items = vec![];
        for chunk_of_segment_locations in chunks {
            let item = MergeIntoOperationAggregator::try_create(
                ctx.clone(),
                on_conflicts.clone(),
                chunk_of_segment_locations,
                self.operator.clone(),
                self.table_info.schema(),
                self.get_write_settings(),
                read_settings.clone(),
                block_builder.clone(),
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
        // caller site guarantees this
        assert!(chunk_size >= 1);

        let mut chunks = vec![];
        for (chunk_idx, chunk) in segments.chunks(chunk_size).enumerate() {
            let mut segment_chunk = (chunk_idx * chunk_size..)
                .zip(chunk.to_vec())
                .collect::<Vec<_>>();
            if chunks.len() < num_partition {
                chunks.push(segment_chunk);
            } else {
                chunks.last_mut().unwrap().append(&mut segment_chunk);
            }
        }
        chunks
    }

    #[async_backtrace::framed]
    async fn chain_mutation_pipes(
        &self,
        ctx: &Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        base_snapshot: Arc<TableSnapshot>,
    ) -> Result<()> {
        // resize
        pipeline.resize(1)?;

        // a) append TableMutationAggregator
        pipeline.add_transform(|input, output| {
            let base_segments = base_snapshot.segments.clone();
            let thresholds = self.get_block_thresholds();
            let location_gen = self.meta_location_generator.clone();
            let schema = self.table_info.schema();
            let dal = self.operator.clone();
            let mutation_aggregator = TableMutationAggregator::create(
                ctx.clone(),
                base_segments,
                thresholds,
                location_gen,
                schema,
                dal,
            );
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input,
                output,
                mutation_aggregator,
            )))
        })?;

        // b) append  CommitSink

        pipeline.add_sink(|input| {
            CommitSink::try_create(self, ctx.clone(), base_snapshot.clone(), input)
        })?;
        Ok(())
    }

    fn new_empty_snapshot(&self) -> TableSnapshot {
        TableSnapshot::new(
            Uuid::new_v4(),
            &None,
            None,
            self.table_info.meta.schema.as_ref().clone(),
            Statistics::default(),
            vec![],
            None,
            None,
        )
    }
}
