// Copyright 2023 Datafuse Labs.
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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableField;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

use crate::io::BlockBuilder;
use crate::operations::merge_into::AppendTransform;
use crate::operations::merge_into::CommitSink;
use crate::operations::merge_into::MergeIntoOperationAggregator;
use crate::operations::merge_into::MutationAccumulator;
use crate::operations::merge_into::ReplaceIntoProcessor;
use crate::operations::merge_into::TableMutationAggregator;
use crate::pipelines::Pipeline;
use crate::FuseTable;

impl FuseTable {
    pub async fn build_replace_pipeline<'a>(
        &'a self,
        ctx: Arc<dyn TableContext>,
        on_filed: TableField,
        pipeline: &'a mut Pipeline,
    ) -> Result<()> {
        // just a testing of processor integration, will migrate to real pipeline later

        // 1. resize input to 1, since the UpsertTransform need to de-duplicate inputs "globally"
        pipeline.resize(1)?;

        // 2. connect with append transform
        pipeline.add_transform(|input, output| {
            let block_builder = BlockBuilder {
                ctx: ctx.clone(),
                meta_locations: self.meta_location_generator.clone(),
                source_schema: self.table_info.schema(),
                write_settings: self.get_write_settings(),
            };

            // TODO simplify the constructor of AppendTransform
            let append = AppendTransform::try_create(
                self.get_write_settings(),
                self.operator.clone(),
                self.meta_location_generator.clone(),
                self.get_block_compact_thresholds(),
                block_builder,
            )?;
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, append,
            )))
        })?;

        // 2. connect with mutation aggregator
        let base_snapshot = match self.read_table_snapshot().await? {
            Some(snapshot) => snapshot,
            None => Arc::new(TableSnapshot::new(
                Uuid::new_v4(),
                &None,
                None,
                self.table_info.meta.schema.as_ref().clone(),
                Statistics::default(),
                vec![],
                None, // TODO find it
                None,
            )),
        };
        pipeline.add_transform(|input, output| {
            let base_segments = base_snapshot.segments.clone();
            let thresholds = self.get_block_compact_thresholds();
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

        // 3. connect with commit sink

        pipeline.add_sink(|input| {
            CommitSink::try_create(self, ctx.clone(), base_snapshot.clone(), input)
        })?;

        Ok(())
    }

    pub async fn build_replace_pipeline_should_be<'a>(
        &'a self,
        _ctx: Arc<dyn TableContext>,
        pipeline: &'a mut Pipeline,
    ) -> Result<()> {
        //                                             ┌────────────────┐
        //                                          ┌─►│MergeIntoMutator├────────────────────────────────────────┐
        //                                          │  └────────────────┘                                        │
        // ┌─────────────┐      ┌───────────────┐   │                                                            │
        // │ UpsertSource├─────►│UpsertTransform├───┤                                                            │
        // └─────────────┘      └───────┬───────┘   │                                                            ▼
        //                              │           │  ┌────────────────┐                               ┌──────────────────┐      ┌──────────┐
        //                              │           └─►│MergeIntoMutator├──────────────────────────────►│MutationAggregator├─────►│CommitSink│
        //                              │              └────────────────┘                               └──────────────────┘      └──────────┘
        //                              │                                                                        ▲
        //                              │                                                                        │
        //                              │              ┌────────────────┐       ┌─────────────┐                  │
        //                              └─────────────►│CompactTransform│ ────► │AppendMutator├──────────────────┘
        //                                             └────────────────┘       └─────────────┘

        // ** assuming the pipeline is wired to input source

        // 1. resize input to 1, since the UpsertTransform need to de-duplicate inputs "globally"
        pipeline.resize(1)?;

        // 2.connect input stream to ReplaceIntoProcessor (TODO adjust the diagram, UpsertTransform should be ReplaceIntoProcessor)
        let replace_in_processor = ReplaceIntoProcessor::try_create()?;

        let output_port_append = replace_in_processor.get_append_data_output_port().clone();
        let output_port_merge_into_action = replace_in_processor
            .get_merge_into_action_output_port()
            .clone();

        let outputs = vec![
            output_port_append.clone(),
            output_port_merge_into_action.clone(),
        ];
        let input_port = replace_in_processor.get_input_port().clone();

        pipeline.add_pipe(Pipe::create(1, outputs.len(), vec![PipeItem::create(
            ProcessorPtr::create(Box::new(replace_in_processor)),
            vec![input_port],
            outputs,
        )]));

        // 3.1 connect the output_port_merge_into_action to MergeIntoMutator

        // let pipeline
        // pipeline.add_pipe(Pipe::create(1, outputs.len(), vec![PipeItem::create(
        //     ProcessorPtr::create(Box::new(replace_in_processor)),
        //     vec![input_port],
        //     outputs,
        // )]));

        // self.connect_merge_into_mutator(pipeline).await?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn connect_merge_into_mutator(&self, pipeline: &mut Pipeline) -> Result<()> {
        // we do not have a broadcast processor yet

        // if let Some(table_snapshot) = self.read_table_snapshot().await? {
        //     let max_threads = ctx.get_settings().get_max_threads()?;
        //     let chunk_size = table_snapshot.segments.len() / max_threads as usize;
        //     for partition in table_snapshot.segments.as_slice().chunks(chunk_size) {
        //         let _merge_into_mutator = MergeIntoMutator::try_create(partition)?;
        //     }
        // } // else plain insert with du-duplication

        // let connect to a single MergeIntoMutator first

        if let Some(table_snapshot) = self.read_table_snapshot().await? {
            let partition = table_snapshot.segments.as_slice();

            pipeline.add_transform(|input, output| {
                let merge_into_mutator = MergeIntoOperationAggregator::try_create(partition)?;
                let t = AsyncAccumulatingTransformer::create(input, output, merge_into_mutator);
                Ok(ProcessorPtr::create(Box::new(t)))
            })?;
        } // else plain insert with du-duplication

        Ok(())
    }
}

// TODO already there?
// struct BroadcastProcessor;
//
// trait DockWith {}
