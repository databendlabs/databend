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

use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_pipeline_core::Pipe;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::executor::physical_plans::MutationManipulate;
use databend_common_storages_fuse::operations::MatchedSplitProcessor;
use databend_common_storages_fuse::operations::MergeIntoNotMatchedProcessor;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // Handle matched and unmatched data separately.
    // This is a complete pipeline with matched and not matched clauses, for matched only or unmatched only
    // we will delicate useless pipeline and processor
    //                                                                                 +-----------------------------+-+
    //                                    +-----------------------+     Matched        |                             +-+
    //                                    |                       +---+--------------->|    MatchedSplitProcessor    |
    //                                    |                       |   |                |                             +-+
    // +----------------------+           |                       +---+                +-----------------------------+-+
    // |      MergeInto       +---------->|MutationSplitProcessor |
    // +----------------------+           |                       +---+                +-----------------------------+
    //                                    |                       |   | NotMatched     |                             +-+
    //                                    |                       +---+--------------->| MergeIntoNotMatchedProcessor| |
    //                                    +-----------------------+                    |                             +-+
    //                                                                                 +-----------------------------+
    // Note: here the output_port of MatchedSplitProcessor are arranged in the following order
    // (0) -> output_port_row_id
    // (1) -> output_port_updated

    // Outputs from MatchedSplitProcessor's output_port_updated and MergeIntoNotMatchedProcessor's output_port are merged and processed uniformly by the subsequent ResizeProcessor
    // receive matched data and not matched data parallelly.
    pub(crate) fn build_mutation_manipulate(
        &mut self,
        merge_into_manipulate: &MutationManipulate,
    ) -> Result<()> {
        self.build_pipeline(&merge_into_manipulate.input)?;

        let (step, need_match, need_unmatch) = match merge_into_manipulate.strategy {
            MutationStrategy::MatchedOnly => (1, true, false),
            MutationStrategy::NotMatchedOnly => (1, false, true),
            MutationStrategy::MixedMatched => (2, true, true),
            MutationStrategy::Direct => unreachable!(),
        };

        let tbl = self
            .ctx
            .build_table_by_table_info(&merge_into_manipulate.table_info, None)?;

        let input_schema = merge_into_manipulate.input.output_schema()?;
        let mut pipe_items = Vec::with_capacity(self.main_pipeline.output_len());
        for _ in (0..self.main_pipeline.output_len()).step_by(step) {
            if need_match {
                let matched_split_processor = MatchedSplitProcessor::create(
                    self.ctx.clone(),
                    merge_into_manipulate.row_id_idx,
                    merge_into_manipulate.matched.clone(),
                    merge_into_manipulate.field_index_of_input_schema.clone(),
                    input_schema.clone(),
                    Arc::new(DataSchema::from(tbl.schema_with_stream())),
                    false,
                    merge_into_manipulate.can_try_update_column_only,
                )?;
                pipe_items.push(matched_split_processor.into_pipe_item());
            }

            if need_unmatch {
                let merge_into_not_matched_processor = MergeIntoNotMatchedProcessor::create(
                    merge_into_manipulate.unmatched.clone(),
                    merge_into_manipulate.unmatched_schema.clone(),
                    self.func_ctx.clone(),
                    self.ctx.clone(),
                )?;
                pipe_items.push(merge_into_not_matched_processor.into_pipe_item());
            }
        }

        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            output_len,
            pipe_items.clone(),
        ));

        Ok(())
    }
}
