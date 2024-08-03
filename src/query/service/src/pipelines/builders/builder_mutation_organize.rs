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

use databend_common_exception::Result;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::executor::physical_plans::MutationOrganize;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // Organize outputs and resize row_id
    pub(crate) fn build_mutation_organize(
        &mut self,
        merge_into_organize: &MutationOrganize,
    ) -> Result<()> {
        self.build_pipeline(&merge_into_organize.input)?;

        // The complete pipeline:
        // -----------------------------------------------------------------------------------------
        // row_id port0_1               row_id port0_1              row_id port0_1
        // matched data port0_2              .....                  row_id port1_1         row_id port
        // unmatched port0_3            data port0_2                    ......
        // row_id port1_1       ====>   row_id port1_1    ====>     data port0_2    ====>  data port0
        // matched data port1_2              .....                  data port1_2           data port1
        // unmatched port1_3            data port1_2                    ......
        // ......                            .....
        // -----------------------------------------------------------------------------------------
        // 1. matched only or complete pipeline are same with above
        // 2. for unmatched only, there are no row_id port

        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        let mut rules = Vec::with_capacity(self.main_pipeline.output_len());
        match merge_into_organize.strategy {
            MutationStrategy::MixedMatched => {
                assert_eq!(self.main_pipeline.output_len() % 3, 0);
                // merge matched update ports and not matched ports ===> data ports
                for idx in (0..self.main_pipeline.output_len()).step_by(3) {
                    ranges.push(vec![idx]);
                    ranges.push(vec![idx + 1, idx + 2]);
                }
                self.main_pipeline.resize_partial_one(ranges.clone())?;
                assert_eq!(self.main_pipeline.output_len() % 2, 0);
                let row_id_len = self.main_pipeline.output_len() / 2;
                for idx in 0..row_id_len {
                    rules.push(idx);
                    rules.push(idx + row_id_len);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2)?;
            }
            MutationStrategy::MatchedOnly => {
                assert_eq!(self.main_pipeline.output_len() % 2, 0);
                let row_id_len = self.main_pipeline.output_len() / 2;
                for idx in 0..row_id_len {
                    rules.push(idx);
                    rules.push(idx + row_id_len);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2)?;
            }
            MutationStrategy::NotMatchedOnly => {}
            MutationStrategy::Direct => unreachable!(),
        }
        Ok(())
    }

    fn resize_row_id(&mut self, step: usize) -> Result<()> {
        // resize row_id
        let row_id_len = self.main_pipeline.output_len() / step;
        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        let mut vec = Vec::with_capacity(row_id_len);
        for idx in 0..row_id_len {
            vec.push(idx);
        }
        ranges.push(vec.clone());

        // data ports
        for idx in 0..row_id_len {
            ranges.push(vec![idx + row_id_len]);
        }

        self.main_pipeline.resize_partial_one(ranges.clone())
    }
}
