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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_transforms::processors::build_full_sort_pipeline;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::physical_plans::Sort;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_sort(&mut self, sort: &Sort) -> Result<()> {
        self.build_pipeline(&sort.input)?;

        let input_schema = sort.input.output_schema()?;

        if let Some(proj) = &sort.pre_projection {
            // Do projection to reduce useless data copying during sorting.
            let projection = proj
                .iter()
                .filter_map(|i| input_schema.index_of(&i.to_string()).ok())
                .collect::<Vec<_>>();

            if projection.len() < input_schema.fields().len() {
                // Only if the projection is not a full projection, we need to add a projection transform.
                self.main_pipeline.add_transform(|input, output| {
                    Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                        input,
                        output,
                        input_schema.num_fields(),
                        self.func_ctx.clone(),
                        vec![BlockOperator::Project {
                            projection: projection.clone(),
                        }],
                    )))
                })?;
            }
        }

        let input_schema = sort.output_schema()?;

        let sort_desc = sort
            .order_by
            .iter()
            .map(|desc| {
                let offset = input_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                    is_nullable: input_schema.field(offset).is_nullable(),  // This information is not needed here.
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.build_sort_pipeline(
            input_schema,
            sort_desc,
            sort.plan_id,
            sort.limit,
            sort.after_exchange,
        )
    }

    pub(crate) fn build_sort_pipeline(
        &mut self,
        input_schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        plan_id: u32,
        limit: Option<usize>,
        after_exchange: bool,
    ) -> Result<()> {
        let block_size = self.settings.get_max_block_size()? as usize;
        let max_threads = self.settings.get_max_threads()? as usize;

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() == 1 || max_threads == 1 {
            self.main_pipeline.try_resize(max_threads)?;
        }
        let prof_info = if self.enable_profiling {
            Some((plan_id, self.proc_profs.clone()))
        } else {
            None
        };

        build_full_sort_pipeline(
            &mut self.main_pipeline,
            input_schema,
            sort_desc,
            limit,
            block_size,
            block_size,
            prof_info,
            after_exchange,
        )
    }
}
