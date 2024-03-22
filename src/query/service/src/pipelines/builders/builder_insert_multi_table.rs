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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::EmptySource;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::PhysicalInsertMultiTable;
use databend_common_sql::gen_mutation_stream_operator;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::TransformAddStreamColumns;
use crate::pipelines::PipelineBuilder;
impl PipelineBuilder {
    pub(crate) fn build_insert_multi_table(
        &mut self,
        plan: &PhysicalInsertMultiTable,
    ) -> Result<()> {
        let PhysicalInsertMultiTable {
            plan_id: _,
            input,
            select_column_bindings,
            filters,
            keep_remain: _,
        } = plan;
        let filters_ = filters
            .iter()
            .map(|v| v.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();

        self.build_pipeline(&input)?;
        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            input.output_schema()?,
            select_column_bindings,
            &mut self.main_pipeline,
            false,
        )?;
        Ok(())
    }
}
